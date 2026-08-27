package bench;

/** A resting order: price fixed, quantity mutable across partial fills. */
final class Resting {
    final long price;
    long qty;

    Resting(long price, long qty) { this.price = price; this.qty = qty; }
}
