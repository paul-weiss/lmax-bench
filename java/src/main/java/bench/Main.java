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

    interface Engine {
        void limit(byte side, long price, long qty, long id);
        void cancel(long id);
        long fills();
        long volume();
        int resting();
        long checksum();
    }

    static final class Book implements Engine {
        final TreeMap<Long, ArrayDeque<Long>> bids = new TreeMap<>();
        final TreeMap<Long, ArrayDeque<Long>> asks = new TreeMap<>();
        final HashMap<Long, Resting> orders = new HashMap<>();
        long fills = 0;
        long volume = 0;

        public void limit(byte side, long price, long qty, long id) {
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

        public void cancel(long id) { orders.remove(id); }

        public long fills() { return fills; }
        public long volume() { return volume; }
        public int resting() { return orders.size(); }

        /** Deterministic checksum over live orders in (price, FIFO) order. */
        public long checksum() {
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

    // ------------------------------------------------------------ tuned book
    // Same semantics as Book (identical fills/volume/checksum on the same
    // stream), engineered for the hot path in the Agrona spirit but with
    // plain primitive arrays: dense array-backed price ladder with best
    // pointers instead of a TreeMap, reusable long[]-FIFO levels, and — as
    // order ids are dense sequential integers — the id map replaced by two
    // preallocated long[] indexed by id (qty==0 marks dead). No boxing, no
    // hashing, zero allocation in steady state: the GC never has to run.

    static final int LADDER_LEN = 4096; // prices stay well inside [480, 1521]

    static final class FlatLevel {
        long[] ids = new long[16];
        int head = 0, size = 0;

        boolean isEmpty() { return head == size; }
        void reset() { head = 0; size = 0; }
        void push(long id) {
            if (size == ids.length) ids = java.util.Arrays.copyOf(ids, size * 2);
            ids[size++] = id;
        }
    }

    static final class TunedBook implements Engine {
        final FlatLevel[] bids = new FlatLevel[LADDER_LEN];
        final FlatLevel[] asks = new FlatLevel[LADDER_LEN];
        long bestBid = -1;
        long bestAsk = LADDER_LEN; // sentinel: no asks
        final long[] price;
        final long[] qty; // 0 = dead
        int live = 0;
        long fills = 0;
        long volume = 0;

        TunedBook(long maxId) {
            for (int i = 0; i < LADDER_LEN; i++) {
                bids[i] = new FlatLevel();
                asks[i] = new FlatLevel();
            }
            price = new long[(int) maxId + 2];
            qty = new long[(int) maxId + 2];
        }

        public void limit(byte side, long px, long q, long id) {
            while (q > 0) {
                long best = (side == SIDE_BID) ? bestAsk : bestBid;
                boolean crosses = (side == SIDE_BID) ? best <= px : best >= px;
                if (best < 0 || best >= LADDER_LEN || !crosses) break;
                FlatLevel lvl = (side == SIDE_BID) ? asks[(int) best] : bids[(int) best];
                while (q > 0) {
                    if (lvl.isEmpty()) break;
                    long head = lvl.ids[lvl.head];
                    if (qty[(int) head] == 0) { // lazily-cancelled order
                        lvl.head++;
                        continue;
                    }
                    long m = Math.min(q, qty[(int) head]);
                    qty[(int) head] -= m;
                    q -= m;
                    fills++;
                    volume += m;
                    if (qty[(int) head] == 0) {
                        live--;
                        lvl.head++;
                    }
                }
                if (lvl.isEmpty()) {
                    lvl.reset();
                    if (side == SIDE_BID) {
                        long p = best + 1;
                        while (p < LADDER_LEN && asks[(int) p].isEmpty()) p++;
                        bestAsk = p;
                    } else {
                        long p = best - 1;
                        while (p >= 0 && bids[(int) p].isEmpty()) p--;
                        bestBid = p;
                    }
                }
            }
            if (q > 0) {
                if (side == SIDE_BID) {
                    bids[(int) px].push(id);
                    bestBid = Math.max(bestBid, px);
                } else {
                    asks[(int) px].push(id);
                    bestAsk = Math.min(bestAsk, px);
                }
                price[(int) id] = px;
                qty[(int) id] = q;
                live++;
            }
        }

        public void cancel(long id) {
            if (qty[(int) id] != 0) {
                qty[(int) id] = 0;
                live--;
            }
        }

        public long fills() { return fills; }
        public long volume() { return volume; }
        public int resting() { return live; }

        public long checksum() {
            long acc = 0;
            for (FlatLevel[] side : new FlatLevel[][] {bids, asks}) {
                for (FlatLevel lvl : side) {
                    for (int i = lvl.head; i < lvl.size; i++) {
                        long id = lvl.ids[i];
                        if (qty[(int) id] != 0) {
                            acc = new SplitMix64(acc ^ id ^ (price[(int) id] << 20)
                                    ^ qty[(int) id]).next();
                        }
                    }
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
        final Engine book;
        final Histogram hist = new Histogram(60_000_000_000L, 3);
        final AtomicReference<Report> report;

        CoreHandler(AtomicReference<Report> report, Engine book) {
            this.report = report;
            this.book = book;
        }

        @Override
        public void onEvent(Ev e, long seq, boolean endOfBatch) {
            switch (e.kind) {
                case KIND_LIMIT -> book.limit(e.side, e.price, e.qty, e.id);
                case KIND_CANCEL -> book.cancel(e.id);
                default -> {
                    report.set(new Report(
                            book.fills(), book.volume(), book.resting(),
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
        boolean tuned = "tuned".equals(System.getenv("ENGINE"));
        System.out.printf("  engine=%s%n", tuned ? "tuned" : "idiomatic");
        Engine engine = tuned ? new TunedBook(totalOps) : new Book();
        Disruptor<Ev> disruptor = new Disruptor<>(
                Ev::new, RING_SIZE, DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new BusySpinWaitStrategy());
        disruptor.handleEventsWith(new CoreHandler(reportRef, engine));
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
                    "  latency µs: p50=%.3f p90=%.3f p99=%.3f p99.9=%.1f p99.99=%.1f max=%.1f (n=%d)%n",
                    h.getValueAtPercentile(50) / 1000.0, h.getValueAtPercentile(90) / 1000.0,
                    h.getValueAtPercentile(99) / 1000.0, h.getValueAtPercentile(99.9) / 1000.0,
                    h.getValueAtPercentile(99.99) / 1000.0, h.getMaxValue() / 1000.0,
                    h.getTotalCount());
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
