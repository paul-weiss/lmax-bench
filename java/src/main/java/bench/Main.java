package bench;



/**
 * CLOB matching engine on the original LMAX Disruptor, benchmarked.
 *
 * <p>Single-producer ring buffer (com.lmax.disruptor, busy-spin) feeding a
 * single-writer matching core that owns all book state. The exact same
 * workload, book algorithm, and measurement protocol are implemented in the
 * Rust version ({@code ../rust}) — the point of this repository is the
 * comparison. Both sides print a deterministic book checksum; equal checksums
 * prove both engines processed a bit-identical stream identically.
 *
 * <p>Modes: {@code throughput <ops>}, {@code latency <ops> <rate>}, {@code all}.
 */
public final class Main {

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : "all";

        if (mode.equals("throughput") || mode.equals("all")) {
            long ops = args.length > 1 && mode.equals("throughput")
                    ? Long.parseLong(args[1]) : 10_000_000L;
            System.out.println("[java] throughput mode");
            Harness.printResult(Harness.run(ops, ops, 0), ops, false);
        }
        if (mode.equals("latency") || mode.equals("all")) {
            long ops = 2_000_000L;
            long rate = 250_000L;
            if (mode.equals("latency")) {
                if (args.length > 1) ops = Long.parseLong(args[1]);
                if (args.length > 2) rate = Long.parseLong(args[2]);
            }
            System.out.printf("[java] latency mode (%d ops/s paced)%n", rate);
            Harness.printResult(Harness.run(ops, ops / 5, rate), ops, true);
        }
    }
}
