package bench;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static bench.Ev.KIND_CANCEL;
import static bench.Ev.KIND_LIMIT;
import static bench.Ev.KIND_REPORT;
import static bench.Ev.SIDE_BID;

/** Idiomatic price-time priority book; lazy cancellation. */
final class Book implements Engine {
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
