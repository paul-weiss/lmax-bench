package bench;

import com.lmax.disruptor.EventHandler;
import org.HdrHistogram.Histogram;
import java.util.concurrent.atomic.AtomicReference;

import static bench.Ev.KIND_CANCEL;
import static bench.Ev.KIND_LIMIT;
import static bench.Ev.KIND_REPORT;
import static bench.Ev.SIDE_BID;

/** The single-writer matching core: all book state lives behind this handler. */
final class CoreHandler implements EventHandler<Ev> {
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
