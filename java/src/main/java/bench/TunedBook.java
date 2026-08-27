package bench;

import static bench.Ev.KIND_CANCEL;
import static bench.Ev.KIND_LIMIT;
import static bench.Ev.KIND_REPORT;
import static bench.Ev.SIDE_BID;

/** Tuned book: array ladder + best pointers + id-indexed primitive arrays — zero steady-state allocation. */
final class TunedBook implements Engine {
    static final int LADDER = 4096; // prices stay well inside [480, 1521]

    final FlatLevel[] bids = new FlatLevel[LADDER];
    final FlatLevel[] asks = new FlatLevel[LADDER];
    long bestBid = -1;
    long bestAsk = LADDER; // sentinel: no asks
    final long[] price;
    final long[] qty; // 0 = dead
    int live = 0;
    long fills = 0;
    long volume = 0;

    TunedBook(long maxId) {
        for (int i = 0; i < LADDER; i++) {
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
            if (best < 0 || best >= LADDER || !crosses) break;
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
                    while (p < LADDER && asks[(int) p].isEmpty()) p++;
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
