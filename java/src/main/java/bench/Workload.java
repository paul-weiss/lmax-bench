package bench;

import static bench.Ev.KIND_CANCEL;
import static bench.Ev.KIND_LIMIT;
import static bench.Ev.KIND_REPORT;
import static bench.Ev.SIDE_BID;

/** Deterministic workload generator (out-params avoid allocating per op). */
final class Workload {
    final SplitMix64 rng;
    long mid = 1000;
    long lastId = 0;

    // out-params of nextOp (avoids allocating an Op per call)
    byte kind, side;
    long price, qty, id;

    Workload(long seed) { rng = new SplitMix64(seed); }

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
