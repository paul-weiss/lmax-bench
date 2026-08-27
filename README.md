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

## Round 2 — Linux, pinned cores, and tuned engines

Round 1 blamed the Rust/C++ tail on "the OS scheduler." Round 2 tested that
claim on Linux (i9-13980HX, 8 P-cores + 16 E-cores, Ubuntu 24.04, lightly
loaded home server) with two new knobs, both env-gated so the idiomatic
baselines are untouched:

- **`ENGINE=tuned`** — a second engine in Rust and C++ with identical
  semantics (same fills, volume, and checksum on the same stream), built for
  the hot path: a dense array-backed price ladder with best-bid/ask pointers
  instead of an ordered tree, pooled slice-FIFO levels (head index, cleared
  not freed), and a pre-reserved id map (FxHash in Rust, identity hash in
  C++). Steady state approaches zero allocation.
- **`PIN_PROD=<core> PIN_CONS=<core>`** — thread pinning (Rust via the
  disruptor crate's `pin_at_core` + `core_affinity`; C++ via
  `pthread_setaffinity_np`). Linux only.

**Latency on Linux** (2M ops paced at 250k ops/s, µs, pinned = distinct
physical P-cores):

| config | p50 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|
| Rust idiomatic | 0.19 | 0.43 | 4.0 | 1,757 | 2,376 |
| Rust idiomatic, pinned | 0.20 | 0.45 | 4.4 | 1,813 | 2,433 |
| Rust tuned | 0.18 | 0.97 | 4.2 | 20.7 | 155 |
| **Rust tuned, pinned** | **0.18** | **0.55** | **3.3** | **17.0** | **54.6** |
| C++ idiomatic | 0.19 | 1.26 | 211 | 3,610 | 4,230 |
| C++ idiomatic, pinned | 0.20 | 0.62 | 210 | 3,691 | 4,310 |
| C++ tuned | 0.16 | 1.21 | 4.9 | 24.4 | 211 |
| C++ tuned, pinned | 0.18 | 0.73 | 2.7 | 89.9 | 203 |

**Throughput on Linux** (10M ops, ranges across runs): Rust idiomatic
16.8–19.5M ops/s, Rust tuned 25.6–29.2M; C++ idiomatic 12.1–14.7M, C++ tuned
15.9–16.4M.

**Go and Java on the same Linux box** (idiomatic engines — no tuned variants
exist for them yet; user-local Go 1.26 / Temurin JDK 22; "restricted" =
`taskset` onto the same two P-cores the Rust/C++ pinned runs used):

| config | p50 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|
| Go | 0.22 | 2.5 | 130 | 236 | 357 |
| Go, restricted to 2 P-cores | 0.21 | 1.1 | 12.4 | 317 | 562 |
| Java | 0.21 | 1.1 | 921 | 2,611 | 3,050 |
| Java, restricted to 2 P-cores | 0.21 | **799** | **7,631** | 9,421 | 9,986 |

Throughput on Linux: Go 11.3M ops/s (double its macOS number), Java 10.2M.

### What Round 2 shows

1. **Round 1's diagnosis was wrong — the "OS tail" was mostly the
   allocator.** Pinning did *nothing* for the idiomatic engines (p99.99
   unchanged at ~1.8 ms Rust / ~3.7 ms C++). Switching to the
   zero-steady-state-allocation engine collapsed p99.99 by ~75–150x *without
   pinning*. The idiomatic tail was malloc slow paths and page faults from
   constant node churn, not scheduler preemption. Measure, then re-measure:
   the first attribution you believe is the one that bites you.
2. **Pinning pays only after allocation discipline.** On the tuned engine it
   cut Rust's max from 155 µs to 54.6 µs — worthless on the idiomatic
   engines, a further ~3x once the allocator was out of the way. Tuning
   layers compose in a specific order.
3. **The tuned engines are also 35–70% faster in throughput** — the WK Selph
   article's actual thesis (arrays + pools beat trees at the touch), verified
   by identical checksums.
4. **"The optimization that wasn't":** the C++ tuned map first shipped with a
   splitmix-mixed hash — theoretically better distribution — and *lost 25%
   throughput* to the identity hash, because sequential order ids under
   identity hashing land in adjacent buckets and stay prefetch-friendly,
   while a well-mixed hash scatters every lookup across a 32 MB table.
   Cache locality beat hash quality. (`cpp/main.cpp`, `IdHash`.)
5. **Linux out of the box beats macOS out of the box, everywhere.** Rust
   idiomatic p99.9 is ~4 µs on Linux vs 37 µs–3 ms on macOS; medians are
   lower; variance is smaller. The deployment platform is a bigger variable
   than most language debates.
6. **The study's floor so far: p99.99 = 17 µs, max = 54.6 µs across 1.6M
   measured ops** (Rust, tuned, pinned) — four orders of magnitude below
   where idiomatic Java on macOS started, with the algorithm and workload
   bit-identical throughout.
7. **Pinning a GC'd runtime without budgeting cores for the collector is a
   disaster.** Restricting the Java process to the same two P-cores the
   Rust/C++ runs were pinned to — so the two busy-spin threads starve G1's
   collector and the JIT — inflated p99 to ~800 µs and p99.9 to 7.6 ms,
   several times *worse* than unrestricted. Go took the same restriction far
   more gracefully (its p99.9 actually improved to 12 µs, with a modestly
   worse extreme tail). Affinity plans must count the runtime's helper
   threads, not just your own.
8. **Idiomatic Rust vs Go on Linux is a crossover, not a ranking.** Rust
   idiomatic wins p99.9 (4 µs vs 130 µs); Go wins p99.99 (236 µs vs 1.8 ms,
   its concurrent GC never producing the allocator's rare multi-ms stall).
   Which one is "better" depends on which percentile your SLA is written
   against — until you remove allocation from the hot path, at which point
   tuned Rust/C++ beat everything at every percentile.

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

# Rust and C++ knobs (Round 2)
ENGINE=tuned ./clob-bench all              # array-ladder engine, same checksums
PIN_PROD=8 PIN_CONS=10 ./clob-bench all    # pin threads (Linux)
```

Or all four, back to back, via `./run.sh`.

## Future work

- **Zero-garbage Java and Go variants** (object pools, Agrona primitive
  maps / preallocated slices) — done for Rust and C++ in Round 2; the GC
  languages are the remaining, and most instructive, half of that experiment.
- Linux with `isolcpus`/`nohz_full` (true isolation, not just affinity);
  `perf` flame graphs of the remaining tuned-engine tail.
- Journal + replay to demonstrate deterministic recovery (the other half of
  the LMAX story).
- Multi-producer mode (N gateway threads, one core).

## License

MIT
