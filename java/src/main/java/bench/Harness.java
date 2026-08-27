package bench;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.HdrHistogram.Histogram;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicReference;

import static bench.Ev.KIND_CANCEL;
import static bench.Ev.KIND_LIMIT;
import static bench.Ev.KIND_REPORT;
import static bench.Ev.SIDE_BID;

/** Builds the Disruptor, runs a mode, and reports. */
final class Harness {
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

        Workload wl = new Workload(42);
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
}
