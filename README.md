# lmax-bench

The same limit-order-book matching engine, built four times on the LMAX
architecture —
**Java on the original [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor)**,
**Rust on the [`disruptor`](https://crates.io/crates/disruptor) crate**, and
**Go and C++ on hand-rolled SPSC busy-spin ring buffers** (neither has a
canonical Disruptor library; the pattern is ~60 lines) — benchmarked
head-to-head on a bit-identical workload.

The interesting result is not "Rust is fastest" (it is, at the median and in
throughput). It's that **each runtime puts its latency somewhere different**:
Java allocates fast and pays in the tail (GC); Go pays at the median and in
throughput but keeps a remarkably tight tail (concurrent sub-ms GC); C++ pays
malloc inline on every node — deterministic, but enough to lose to *Java* on
throughput — and shares Rust's OS-bound tail; Rust pays the least everywhere,
until the OS scheduler becomes its tail. In trading systems the tail is the
story.

## Results (Apple M1 Max, macOS, out of the box — see Methodology)

**Throughput** (10M ops, producer publishing as fast as the ring accepts;
ranges across repeated runs):

| | Java (disruptor 4.0) | Rust (disruptor 4.4) | Go (SPSC ring) | C++ (SPSC ring) |
|---|---:|---:|---:|---:|
| ops/s | 8.0–8.8M | 13.1–14.5M | 5.7–6.9M | 7.4–8.3M |
| GC | 8–9 collections, 41–48 ms pause | — | 13 cycles, **1.2 ms** total STW | — |
| heap allocations | (boxed keys, order objects) | 310,882 | 567,939 | **5,537,674** |

**Latency** (2M ops paced at 250k ops/s, publish→processed, HdrHistogram,
scheduled-time stamping to avoid coordinated omission). All values in
**microseconds**; each cell is the range observed across four runs, because on
an un-isolated desktop the run-to-run spread *is* part of the result:

| percentile | Java (µs) | Rust (µs) | Go (µs) | C++ (µs) |
|---|---:|---:|---:|---:|
| p50 | 0.25 – 0.42 | 0.17 – 0.29 | 0.33 – 0.50 | 0.21 – 0.25 |
| p90 | 0.67 – 0.79 | 0.54 – 0.58 | 0.71 – 0.83 | 0.63 – 0.71 |
| p99 | 6.6 – 440 | 3.5 – 13.3 | 3.6 – 14.8 | 4.0 – 4.4 |
| p99.9 | **1,674 – 8,208** | 37 – 3,070 | **29 – 48** | 24 – 56 |
| p99.99 | 4,018 – 13,509 | 2,204 – 8,294 | **79 – 179** | 1,752 – 1,907 |
| max | 4,481 – 14,017 | 2,806 – 8,897 | 165 – 744 | 2,377 – 2,530 |

Every run of every implementation prints a deterministic checksum of the final
book plus fill/volume counts — and **all four languages produce the same
checksum on the same workload**, proving they processed a bit-identical
operation stream through identical matching logic. This is what makes the
comparison meaningful.

## What this shows

1. **The architecture, not the language, sets the median.** A single-writer
   core behind a ring buffer processes an order in a few hundred nanoseconds
   in all three languages. If p50 is what you care about, any of them is
   already there.
2. **Java's tail is GC-bound and reproduces every run.** p99.9 in the
   milliseconds, every time, tracking the collection counters one-for-one.
   The idiomatic-collections book (`TreeMap<Long, ArrayDeque<Long>>`, boxed
   `Long`s, an object per resting order) produces steady garbage, and G1's
   pauses land in the distribution exactly where trading systems can least
   afford them.
3. **Go is the surprise: the tightest, most reproducible tail of the three.**
   Its collector is *designed* for this — fully concurrent marking with
   sub-millisecond stop-the-world phases (1.2 ms total across 13 cycles here,
   vs Java's 41–48 ms across 9). The GC runs, constantly, and barely shows at
   p99.99. The price appears elsewhere: the lowest throughput (write barriers,
   allocation cost, no free lunch) and a slightly higher median. **GC design
   matters more than GC presence.**
4. **Rust has the best median and throughput, and its tail belongs to the
   OS.** With no collector, what's left at p99.9+ is scheduler preemption and
   core migration — which on an unpinned macOS desktop swings its tail from
   37 µs to 3 ms run-to-run. Removing the GC doesn't grant a flat histogram;
   it promotes the platform to your biggest tail source, and pinning,
   isolation, and allocation-free steady state are still on you.
5. **C++ shows that manual memory is not automatically fast.** Idiomatic
   `std::map`/`std::deque` C++ allocates a node per price level and chunk —
   5.5M mallocs over 10M ops, each paid inline — and *loses to Java on
   throughput*, because the JVM's bump-pointer TLAB allocation is far cheaper
   per-object than malloc; Java just presents the bill later, as GC, in the
   tail. C++'s tail profile matches Rust's: clean through p99.9 (~25–56 µs),
   then OS-bound at p99.99 (~1.8 ms, reproducibly). The cross-language
   ranking flips depending on which percentile — or which cost — you look at:
   **allocation strategy matters more than language.**
6. **None of this is any language's ceiling.** LMAX ran Java in production by
   refusing to allocate on the hot path (object pools,
   [Agrona](https://github.com/aeron-io/agrona) primitive collections,
   flyweights); low-garbage Go (`sync.Pool`, preallocation, value types) is
   standard practice in Go trading shops for the same reason. See Future work.

## Architecture

All three are the same shape — the LMAX pattern:

```
producer thread                    consumer thread (single writer)
  workload gen (splitmix64)   →      matching core owns ALL state
  publish into ring buffer    →      price-time priority book
  (busy-spin wait strategy)          fills/cancels, HdrHistogram
```

- One producer, one consumer, busy-spin on both sides, ring size 65,536.
- Transports: Java uses the original Disruptor (`BusySpinWaitStrategy`,
  `ProducerType.SINGLE`); Rust the `disruptor` crate (`BusySpin`); Go and C++
  hand-rolled SPSC rings — two cache-line-padded atomic cursors over a
  pre-allocated event array (`go/main.go`, `cpp/main.cpp`), acquire/release
  ordering, busy-spin on both sides.
- All book state is owned by the consumer thread: no locks, no sharing, no
  concurrent data structures. Determinism is a design property, which is what
  makes the cross-language checksum possible at all.

### The matching engine ("a known algorithm")

Price-time priority continuous limit-order-book matching:

- Two sides of price levels in each language's idiomatic ordered map (Java
  `TreeMap`, Rust `BTreeMap`, Go `google/btree` — the stdlib has none — and
  C++ `std::map`), each level a FIFO queue of order ids; an id→order hash map
  for O(1) cancel.
- An incoming limit order sweeps crossing opposite levels best-first, FIFO
  within a level; any residual rests.
- Cancels are **lazy**: removal from the id map only; dead ids are skipped when
  they surface at the head of a queue (a standard real-world technique —
  cancel-heavy flow makes eager level surgery expensive).

### The workload

Deterministic, generated by splitmix64 implemented identically in all four
languages (seed 42): 55% passive limits (1–20 ticks behind the touch), 30%
aggressive limits (0–9 ticks into/through the spread), 15% cancels of recent
ids; the mid random-walks in [500, 1500]. ~68% of limit orders end up trading.

## Methodology notes

- **Coordinated omission**: in latency mode the producer is paced (250k ops/s)
  and each event is stamped with its *scheduled* publish time, not the actual
  one — a delayed publish therefore charges the delay to itself, per
  HdrHistogram practice.
- **Warmup**: first 20% of ops are unmeasured (JIT warmup, allocator warmup,
  map growth).
- **Percentiles**: Java/Rust/Go record into HdrHistogram (3 significant
  digits); C++ stores every raw sample in a preallocated vector and sorts at
  the end — exact percentiles, and a cross-check that histogram resolution
  isn't shaping the other results.
- Throughput mode measures the drain-inclusive wall clock: publish loop plus a
  final sentinel event, ending when the consumer has processed everything.
- Caveats: macOS, no core pinning or isolation, shared desktop machine, one
  JVM/one binary run per mode, default JVM flags (JDK 22, default G1). Treat
  absolute numbers as indicative, deltas as the signal. Run-to-run tail
  variance is large on a shared machine — Rust's p99.9 has been observed
  anywhere from ~40 µs to ~3 ms across runs (scheduler noise), while Java's
  multi-millisecond p99.9+ and Go's tens-of-microseconds p99.9 both reproduce
  every run; that reproducibility is the actual finding. Linux + pinned cores
  + JMH-style forking would tighten all of it (see below).

## Run it

```
# Rust
cd rust && cargo build --release
./target/release/clob-bench all            # or: throughput 10000000 | latency 2000000 250000

# Java
cd java && mvn -q package
java -jar target/clob-bench-0.1.0.jar all  # same subcommands

# Go
cd go && go build -o clob-bench .
./clob-bench all                           # same subcommands

# C++
cd cpp && make
./clob-bench all                           # same subcommands
```

Or all four, back to back, via `./run.sh`.

## Future work

- **Zero-allocation variants in every language** (object pools, Agrona
  primitive maps / preallocated slices, custom arenas in C++, array-backed
  levels) — the fair fight, and the most instructive diff: how much of the
  tail and throughput come back, and what the code has to give up to get
  them.
- Array-backed price ladder (dense ticks) replacing the tree maps everywhere.
- Linux run with isolated, pinned cores; `perf`/dtrace flame graphs.
- Journal + replay to demonstrate deterministic recovery (the other half of
  the LMAX story).
- Multi-producer mode (N gateway threads, one core).

## License

MIT
