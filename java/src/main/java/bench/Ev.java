package bench;

/** The ring-buffer event, plus the wire constants all components share. */
final class Ev {
    static final byte KIND_LIMIT = 0;
    static final byte KIND_CANCEL = 1;
    static final byte KIND_REPORT = 2;
    static final byte SIDE_BID = 0;

    byte kind, side;
    long price, qty, id;
    long ts; // scheduled-publish nanos (System.nanoTime domain); 0 = don't measure
}
