package bench;

/** Reusable long[]-backed FIFO level for the tuned book. */
final class FlatLevel {
    long[] ids = new long[16];
    int head = 0, size = 0;

    boolean isEmpty() { return head == size; }
    void reset() { head = 0; size = 0; }
    void push(long id) {
        if (size == ids.length) ids = java.util.Arrays.copyOf(ids, size * 2);
        ids[size++] = id;
    }
}
