package bench;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.HdrHistogram.Histogram;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CLOB matching engine on the original LMAX Disruptor, benchmarked.
 *
 * <p>Single-producer ring buffer (com.lmax.disruptor, busy-spin) feeding a
 * single-writer matching core that owns all book state. The exact same
 * workload, book algorithm, and measurement protocol are implemented in the
 * Rust version ({@code ../rust}) — the point of this repository is the
 * comparison. Both sides print a deterministic book checksum; equal checksums
 * prove both engines processed a bit-identical stream identically.
 *
 * <p>Modes: {@code throughput <ops>}, {@code latency <ops> <rate>}, {@code all}.
 */
public final class Main {

    // ------------------------------------------------------------ workload
    // splitmix64, bit-identical to the Rust generator.

    static final class SplitMix64 {
        long state;

        SplitMix64(long seed) { this.state = seed; }

        long next() {
            state += 0x9E3779B97F4A7C15L;
            long z = state;
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }
    }

    static final byte KIND_LIMIT = 0;
    static final byte KIND_CANCEL = 1;
    static final byte KIND_REPORT = 2;
    static final byte SIDE_BID = 0;

    static final class Gen {
        final SplitMix64 rng;
        long mid = 1000;
        long lastId = 0;

        // out-params of nextOp (avoids allocating an Op per call)
        byte kind, side;
        long price, qty, id;

        Gen(long seed) { rng = new SplitMix64(seed); }

        void nextOp() {
            long r = rng.next();
            if (Long.remainderUnsigned(r, 7) == 0) {
                mid += Long.remainderUnsigned(r >>> 40, 3) - 1;
                mid = Math.clamp(mid, 500, 1500);
            }
            long c = Long.remainderUnsigned(r, 100);
            if (c < 85) {
                side = (byte) ((r >>> 8) & 1);
                boolean aggressive = c >= 55;
                long offset = aggressive
                        ? Long.remainderUnsigned(r >>> 16, 10)
                        : 1 + Long.remainderUnsigned(r >>> 16, 20);
                price = ((side == SIDE_BID) != aggressive) ? mid - offset : mid + offset;
                qty = 1 + Long.remainderUnsigned(r >>> 32, 100);
                id = ++lastId;
                kind = KIND_LIMIT;
            } else {
                long back = Long.remainderUnsigned(r >>> 16, 5000);
                id = lastId > back ? lastId - back : 0; // saturating
                kind = KIND_CANCEL;
                side = 0;
                price = 0;
                qty = 0;
            }
        }
    }

    // ------------------------------------------------------------ the book
    // Price-time priority; lazy cancellation (cancel removes from the id map,
    // stale ids are skipped at the head of a level queue).

    static final class Resting {
        final long price;
        long qty;

        Resting(long price, long qty) { this.price = price; this.qty = qty; }
    }

    static final class Book {
        final TreeMap<Long, ArrayDeque<Long>> bids = new TreeMap<>();
        final TreeMap<Long, ArrayDeque<Long>> asks = new TreeMap<>();
        final HashMap<Long, Resting> orders = new HashMap<>();
        long fills = 0;
        long volume = 0;

        void limit(byte side, long price, long qty, long id) {
            while (qty > 0) {
                Map.Entry<Long, ArrayDeque<Long>> best =
                        side == SIDE_BID ? asks.firstEntry() : bids.lastEntry();
                if (best == null) break;
                long bestPrice = best.getKey();
                boolean crosses = side == SIDE_BID ? bestPrice <= price : bestPrice >= price;
                if (!crosses) break;
                ArrayDeque<Long> level = best.getValue();
                while (qty > 0) {
                    Long head = level.peekFirst();
                    if (head == null) break;
                    Resting rec = orders.get(head);
                    if (rec == null) {           // lazily-cancelled order
                        level.pollFirst();
                        continue;
                    }
                    long m = Math.min(qty, rec.qty);
                    rec.qty -= m;
                    qty -= m;
                    fills++;
                    volume += m;
                    if (rec.qty == 0) {
                        orders.remove(head);
                        level.pollFirst();
                    }
                }
                if (level.isEmpty()) {
                    (side == SIDE_BID ? asks : bids).remove(bestPrice);
                }
            }
            if (qty > 0) {
                (side == SIDE_BID ? bids : asks)
                        .computeIfAbsent(price, p -> new ArrayDeque<>())
                        .addLast(id);
                orders.put(id, new Resting(price, qty));
            }
        }

        void cancel(long id) { orders.remove(id); }

        /** Deterministic checksum over live orders in (price, FIFO) order. */
        long checksum() {
            long acc = 0;
            for (ArrayDeque<Long> level : bids.values()) acc = foldLevel(acc, level);
            for (ArrayDeque<Long> level : asks.values()) acc = foldLevel(acc, level);
            return acc;
        }

        private long foldLevel(long acc, ArrayDeque<Long> level) {
            for (long id : level) {
                Resting r = orders.get(id);
                if (r != null) {
                    acc = new SplitMix64(acc ^ id ^ (r.price << 20) ^ r.qty).next();
                }
            }
            return acc;
        }
    }

    // ------------------------------------------------------------ harness

    static final class Ev {
        byte kind, side;
        long price, qty, id;
        long ts; // scheduled-publish nanos (System.nanoTime domain); 0 = don't measure
    }

    record Report(long fills, long volume, int resting, long checksum, Histogram hist) {}

    static final class CoreHandler implements EventHandler<Ev> {
        final Book book = new Book();
        final Histogram hist = new Histogram(60_000_000_000L, 3);
        final AtomicReference<Report> report;

        CoreHandler(AtomicReference<Report> report) { this.report = report; }

        @Override
        public void onEvent(Ev e, long seq, boolean endOfBatch) {
            switch (e.kind) {
                case KIND_LIMIT -> book.limit(e.side, e.price, e.qty, e.id);
                case KIND_CANCEL -> book.cancel(e.id);
                default -> {
                    report.set(new Report(
                            book.fills, book.volume, book.orders.size(),
                            book.checksum(), hist.copy()));
                    return;
                }
            }
            if (e.ts != 0) {
                hist.recordValue(Math.max(1, System.nanoTime() - e.ts));
            }
        }
    }

    static final int RING_SIZE = 65_536;

    record RunResult(Report report, double wallSeconds, long gcCount, long gcMillis) {}

    static RunResult run(long totalOps, long warmupOps, long rate) {
        AtomicReference<Report> reportRef = new AtomicReference<>();
        Disruptor<Ev> disruptor = new Disruptor<>(
                Ev::new, RING_SIZE, DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new BusySpinWaitStrategy());
        disruptor.handleEventsWith(new CoreHandler(reportRef));
        disruptor.start();
        RingBuffer<Ev> rb = disruptor.getRingBuffer();

        Gen wl = new Gen(42);
        long interval = rate > 0 ? 1_000_000_000L / rate : 0;
        long nextDue = System.nanoTime();

        long gcCount0 = 0, gcMillis0 = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount0 += gc.getCollectionCount();
            gcMillis0 += gc.getCollectionTime();
        }

        long t0 = System.nanoTime();
        for (long i = 0; i < totalOps; i++) {
            wl.nextOp();
            long ts = 0;
            if (interval > 0) {
                nextDue += interval;
                while (System.nanoTime() < nextDue) Thread.onSpinWait();
                if (i >= warmupOps) ts = nextDue;
            }
            long seq = rb.next();
            Ev e = rb.get(seq);
            e.kind = wl.kind;
            e.side = wl.side;
            e.price = wl.price;
            e.qty = wl.qty;
            e.id = wl.id;
            e.ts = ts;
            rb.publish(seq);
        }
        long seq = rb.next();
        Ev e = rb.get(seq);
        e.kind = KIND_REPORT;
        e.ts = 0;
        rb.publish(seq);
        disruptor.shutdown(); // waits for the backlog to drain
        double wall = (System.nanoTime() - t0) / 1e9;

        long gcCount = 0, gcMillis = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += gc.getCollectionCount();
            gcMillis += gc.getCollectionTime();
        }

        Report report = reportRef.get();
        if (report == null) throw new IllegalStateException("consumer did not report");
        return new RunResult(report, wall, gcCount - gcCount0, gcMillis - gcMillis0);
    }

    static void printResult(RunResult r, long totalOps, boolean latency) {
        Report rep = r.report();
        System.out.printf("  gc: %d collections, %d ms total pause%n", r.gcCount(), r.gcMillis());
        System.out.printf("  ops=%d wall=%.3fs throughput=%.0f ops/s%n",
                totalOps, r.wallSeconds(), totalOps / r.wallSeconds());
        System.out.printf("  fills=%d volume=%d resting=%d checksum=%016x%n",
                rep.fills(), rep.volume(), rep.resting(), rep.checksum());
        if (latency) {
            Histogram h = rep.hist();
            System.out.printf(
                    "  latency ns: p50=%d p90=%d p99=%d p99.9=%d p99.99=%d max=%d (n=%d)%n",
                    h.getValueAtPercentile(50), h.getValueAtPercentile(90),
                    h.getValueAtPercentile(99), h.getValueAtPercentile(99.9),
                    h.getValueAtPercentile(99.99), h.getMaxValue(), h.getTotalCount());
        }
    }

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : "all";

        if (mode.equals("throughput") || mode.equals("all")) {
            long ops = args.length > 1 && mode.equals("throughput")
                    ? Long.parseLong(args[1]) : 10_000_000L;
            System.out.println("[java] throughput mode");
            printResult(run(ops, ops, 0), ops, false);
        }
        if (mode.equals("latency") || mode.equals("all")) {
            long ops = 2_000_000L;
            long rate = 250_000L;
            if (mode.equals("latency")) {
                if (args.length > 1) ops = Long.parseLong(args[1]);
                if (args.length > 2) rate = Long.parseLong(args[2]);
            }
            System.out.printf("[java] latency mode (%d ops/s paced)%n", rate);
            printResult(run(ops, ops / 5, rate), ops, true);
        }
    }
}
