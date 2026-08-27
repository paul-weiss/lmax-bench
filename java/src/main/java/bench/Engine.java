package bench;

/** The matching-engine contract shared by the idiomatic and tuned books. */
interface Engine {
    void limit(byte side, long price, long qty, long id);
    void cancel(long id);
    long fills();
    long volume();
    int resting();
    long checksum();
}
