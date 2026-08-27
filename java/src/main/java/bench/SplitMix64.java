package bench;

/** splitmix64 — bit-identical to the Rust, Go, and C++ generators. */
final class SplitMix64 {
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
