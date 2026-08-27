//! CLOB matching engine on the LMAX pattern in Rust, benchmarked.
//!
//! Architecture: single-producer ring buffer (the `disruptor` crate, busy-spin)
//! feeding a single-writer matching core that owns all book state. The exact
//! same workload, book algorithm, and measurement protocol are implemented in
//! the Java version (`../java`) on the original com.lmax.disruptor — the point
//! of this repository is the comparison.
//!
//! Modes:
//!   throughput <ops>       — publish as fast as the ring accepts; report ops/s.
//!   latency <ops> <rate>   — paced producer at `rate` ops/s; report publish->
//!                            processed latency percentiles (HdrHistogram),
//!                            stamped with the *scheduled* time to avoid
//!                            coordinated omission.
//!   all                    — both, with defaults.

mod alloc;
mod book;
mod harness;
mod tuned;
mod workload;

use harness::{print_report, run};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("all");
    let arg = |i: usize, d: u64| args.get(i).and_then(|s| s.parse().ok()).unwrap_or(d);

    if mode == "throughput" || mode == "all" {
        let ops = arg(2, 10_000_000);
        println!("[rust] throughput mode");
        let (rep, wall) = run(ops, ops, None);
        print_report(&rep, wall, ops, false);
    }
    if mode == "latency" || mode == "all" {
        let (ops, rate) = if mode == "latency" {
            (arg(2, 2_000_000), arg(3, 250_000))
        } else {
            (2_000_000, 250_000)
        };
        println!("[rust] latency mode ({rate} ops/s paced)");
        let (rep, wall) = run(ops, ops / 5, Some(rate));
        print_report(&rep, wall, ops, true);
    }
}
