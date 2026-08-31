# lmax-bench

The same limit-order-book matching engine, built four times on the LMAX
architecture —
**Java on the original [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor)**,
**Rust on the [`disruptor`](https://crates.io/crates/disruptor) crate**, and
**Go and C++ on hand-rolled SPSC busy-spin ring buffers** (neither has a
canonical Disruptor library; the pattern is ~60 lines) — benchmarked
head-to-head on a bit-identical workload.

The interesting result is not which language is fastest — that answer changed
hands three times as the study progressed, which is itself the finding. **Each
runtime puts its latency somewhere different**: Java's idiomatic collections
allocate fast and pay in the tail (GC); Go pays at the median and in
throughput but keeps a remarkably tight tail (concurrent sub-ms GC); C++ pays
malloc inline on every node — deterministic, but enough to lose to *Java* on
throughput. And the biggest lever turns out to be allocation discipline, not
language: every engine that truly stopped allocating on the hot path jumped
the ladder. Zero-garbage Java out-threw tuned Rust (Round 2); then the C++
engine that finally *earned* its "zero-allocation" label — Round 3 below
caught its `std::unordered_map` quietly malloc'ing a node per resting order —
went from last place to first. A ~450x idiomatic cross-language spread at
p99.99 compresses to ~5x once the allocator is out of the way. In trading
systems the tail is the story.

Every run of every implementation prints a deterministic checksum of the final
book plus fill/volume counts — and **all four languages produce the same
checksum on the same workload**, proving they processed a bit-identical
operation stream through identical matching logic. This is what makes the
comparison meaningful.

**Prefer the story to the reference?** [BLOG.md](BLOG.md) is the narrative
write-up — the rounds in the order they actually happened, including the wrong
"scheduler jitter" diagnosis, how it was caught, and the study then catching
itself making the same class of mistake a second time.

## Round 3 — the "zero-allocation" engine that wasn't (Linux, C++)

Round 2 (below) described the tuned C++ engine as near-zero-allocation. The
repo's own allocation counter said otherwise: **1,029,698 heap allocations
over a 2M-op run** — one per resting order, barely fewer than idiomatic's
1,165,249. `std::unordered_map` is node-based *by the standard* (stable
element addresses), so `reserve()` sizes only the bucket array: every
`emplace` still mallocs a node, every `erase` frees one. The identity-hash
fix (Round 2, finding 4) treated the lookup cost and left the allocator on
the hot path — and Round 2 then mis-filed the resulting 89.9 µs p99.99 as
C++'s runtime character. The exact failure mode this study exists to expose,
hiding in the standard library, in the study's own code.

Two changes (PR #1), semantics identical, all conformance vectors unchanged:

- the id map replaced by a preallocated `std::vector<Resting>` indexed
  directly by order id (ids are dense sequence numbers; `qty == 0` marks a
  dead slot) — the same trick the tuned Java and Go engines already used.
  Allocations per 2M-op run: 1,029,698 → **9,895**.
- the SPSC ring gained the Disruptor's *cached gating sequence* (the producer
  keeps a private lower bound of the consumer cursor and touches the shared
  cache line only when the ring looks full) and a batch-draining consumer —
  one acquire/release pair per batch instead of per event.

**Latency** (same box, protocol, and P-cores as Round 2; ranges across runs;
Rust rebuilt and re-run in the same session for a fair same-day comparison):

| config | p50 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|
| C++ idiomatic, pinned (same-day baseline) | 0.19 | 1.33 | 163 | 3,305 | 3,926 |
| C++ tuned | 0.15 | 0.57 | 2.5 | 29.4 | 179 |
| **C++ tuned, pinned** | **0.15–0.17** | **0.45–0.63** | **2.6–3.7** | **13.7–15.3** | **74.6–116** |
| Rust tuned, pinned (same day) | 0.17 | 0.60–0.82 | 4.9–9.7 | 38.8–42.8 | 138–158 |

**Throughput** (10M ops): C++ tuned **48.7–49.2M ops/s** — up 3x from Round
2's 15.9–16.4M, and 1.8x same-day tuned Rust (26.7M). On the Round 1 macOS
box the same change reads 46.4M ops/s and p99.99 = 39.3 µs, unpinned.

(The latency rows above were measured in a quiet window; Round 4 below puts
the run-to-run spread of the extreme tail on the record — read its p99.99
numbers as ranges, not points.)

### What Round 3 shows

1. **C++ went from last place to first** — p99.99 from 90 µs to 13.7–15.3 µs,
   throughput from 16M to 49M ops/s — by deleting allocations the study had
   already declared deleted. The gap Round 2 attributed to runtime character
   was a million hidden mallocs.
2. **Zero-allocation is a measured property, not a declared one.** The
   harness's allocation counter had the truth the whole time; nobody looked
   at it for the engine that was "obviously" allocation-free. Believe the
   counter over the comment.
3. **Same-day comparisons only.** Rust tuned+pinned measured 38.8–42.8 µs
   p99.99 today against 17.0 µs in Round 2 — the un-isolated box's
   day-to-day range. Cross-engine conclusions here come from runs in the
   same session; numbers from different days are weather.
4. **The ladder is provisional in the other direction too.** Rust's tuned
   engine still pays FxHash-and-probe per id lookup where C++ now indexes an
   array, and its ring is the crate's. Symmetric treatment is future work —
   the podium order is a function of who most recently earned zero
   allocation, which is precisely the point.

## Round 4 — measurement hygiene: warmup, a no-GC Go, and which percentile you can trust

Three changes to *how* the study measures, none to what it measures —
semantics and conformance vectors untouched throughout.

**Warmup, all four versions.** Every main now runs 1M ops through a full
discarded pipeline — its own ring, engine, and generator — before anything is
measured, so no measured run executes cold: C2 has compiled the ring,
handler, and matching paths in Java; branch predictors, caches, and the
allocator are warm everywhere. The GC languages then force one collection so
warmup garbage cannot bill a measured run (JMH does the same between
iterations). The effect is largest where JIT and cold caches were: tuned C++
2M-op throughput on the macOS box went **~30M → ~48M ops/s** purely from
being measured warm, and tuned Java latency runs now report **`gc: 0
collections` by construction**, not by luck.

**Go without a collector at all.** `GODEBUG=gctrace=1` showed the tuned Go
engine's two GC cycles per run both fire in the first ~11 ms — the pacer
reacting to the book's construction as the id arrays step the heap 18→35 MB.
Steady state (~10k tiny allocations per run) triggers nothing. That makes
`GOGC=off` — native to the runtime, zero code — a safe configuration: **the
collector never runs, period** (`gc: 0 cycles` on every run), and Linux
tuned throughput rose **13.3M → 18.6M ops/s** (no-GC plus warm). The harness
prints `gogc=<value>` beside the engine so every result line is
self-documenting. Symmetric footnote: tuned *Java's* 10M-op throughput runs
still show 2 collections — constructing its 160 MB of id arrays steps the
heap exactly the way Go's pacer cycles did; its latency runs show zero.

**The affinity trap, sequel.** The Rust warmup initially pinned the producer
on the main thread — which narrows the affinity mask every later thread
inherits, so the *measured* run's consumer could no longer be pinned ("No
core with ID=10 is available", straight from the disruptor crate). The
warmup now runs in a throwaway thread whose pin dies with it, while still
heating the exact cores the measured run uses. Round 2, finding 8 said
affinity plans must count threads you didn't create; this round adds:
affinity is *inherited*, so a pin's lifetime matters as much as its target.

**What repeated clean runs actually show.** With stray processes ruled out
and the box back to ambient load, repeated same-config reps (tuned, pinned/
restricted) put the run-to-run spread on the record (p99.99 / max, µs):

| rep | C++ | Rust | Java | Go (GOGC=off) |
|---|---:|---:|---:|---:|
| quiet window (Round 3) | 13.7–15.3 / 75–116 | 38.8–42.8 / 138–158 | — | — |
| busier window | 48.6–63.3 / 132–192 | 28.9–54.9 / 131–195 | 60.1 / 141 | 61.9 / 144 |
| clean rep 1 | 38.5 / 134 | 54.9 / 136 | 88.3 / 325 | 290.6 / 885 |
| clean rep 2 | 40.0 / 130 | 64.3 / 178 | 66.8 / 222 | 52.2 / 139 |

Meanwhile **p99.9 and below barely move**: C++ 2.5–2.7, Java 2.5–2.6, Go
2.6–4.0, Rust 4.6–9.7 µs, rep after rep. Only ~160 of 1.6M samples sit above
p99.99, so a handful of ambient interruptions — kernel workers, the desktop
session — own that percentile on an un-isolated box, for every language
equally. The finding, stated once: **on shared hardware, p99.9 measures the
engine; p99.99 measures the machine's mood.** Cross-language claims in this
study now lean on p99.9 and report p99.99 as ranges; making p99.99
trustworthy is exactly the `isolcpus`/`nohz_full` item in Future work.

## Results — Linux, pinned cores, and tuned engines (Round 2)

Round 1 (below, on macOS) blamed the Rust/C++ tail on "the OS scheduler."
Round 2 tested that claim on Linux (i9-13980HX, 8 P-cores + 16 E-cores, Ubuntu 24.04, lightly
loaded home server) with two new knobs, both env-gated so the idiomatic
baselines are untouched:

- **`ENGINE=tuned`** — a second engine in Rust and C++ with identical
  semantics (same fills, volume, and checksum on the same stream), built for
  the hot path: a dense array-backed price ladder with best-bid/ask pointers
  instead of an ordered tree, pooled slice-FIFO levels (head index, cleared
  not freed), and a fast id store (a pre-reserved FxHash map in Rust; in C++
  originally an identity-hashed `std::unordered_map`, since Round 3 a
  preallocated array indexed by id). Round 2 believed both approached zero
  allocation in steady state; Round 3 shows that was only true for Rust.
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

**Go and Java on the same Linux box** (user-local Go 1.26 / Temurin JDK 22;
"restricted" = `taskset` onto the same two P-cores the Rust/C++ pinned runs
used). Both languages also got tuned engines: the same array ladder and
pooled level FIFOs, plus — because order ids are dense sequential integers —
the id map replaced by two preallocated primitive arrays indexed by id
(Agrona-spirit zero-garbage Java without the dependency; the same trick in
Go). Checksums still match every other implementation:

| config | p50 | p99 | p99.9 | p99.99 | max |
|---|---:|---:|---:|---:|---:|
| Go idiomatic | 0.22 | 2.5 | 130 | 236 | 357 |
| Go idiomatic, restricted | 0.21 | 1.1 | 12.4 | 317 | 562 |
| Go tuned | 0.18 | 2.0 | 110 | 172 | 247 |
| **Go tuned, restricted** | 0.19 | 0.55 | **3.3** | **67.6** | **160** |
| Java idiomatic | 0.21 | 1.1 | 921 | 2,611 | 3,050 |
| Java idiomatic, restricted | 0.21 | **799** | **7,631** | 9,421 | 9,986 |
| **Java tuned** | 0.20 | 0.34 | **2.6** | **63.0** | **149** |
| Java tuned, restricted | 0.19 | 1.0 | 1,433 | 3,152 | 3,482 |

Throughput on Linux: Go idiomatic 11.3M ops/s (double its macOS number), Go
tuned 13.3M; Java idiomatic 10.2M, **Java tuned 24.9–33.8M — overlapping
with, and sometimes beating, tuned Rust's 28.4–29.6M**. The Java tuned
latency runs report **zero GC collections**: with nothing allocated, the
collector never runs, and the GC tail simply does not exist.

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
   Cache locality beat hash quality. (The full three-step history — mixed
   hash → identity hash → no hash at all — is preserved as a comment in
   `cpp/tuned.hpp`; Round 3 is the third step.)
5. **Linux out of the box beats macOS out of the box, everywhere.** Rust
   idiomatic p99.9 is ~4 µs on Linux vs 37 µs–3 ms on macOS; medians are
   lower; variance is smaller. The deployment platform is a bigger variable
   than most language debates.
6. **The study's floor after Round 2: p99.99 = 17 µs, max = 54.6 µs across
   1.6M measured ops** (Rust, tuned, pinned) — four orders of magnitude below
   where idiomatic Java on macOS started, with the algorithm and workload
   bit-identical throughout. (Round 3 lowered the p99.99 floor to 13.7 µs —
   C++, tuned, pinned.)
7. **The LMAX thesis, reproduced.** Zero-garbage Java (primitive arrays, no
   boxing, no steady-state allocation) went from p99.9 = 921 µs to
   **2.6 µs** and from 10.2M to **~30M ops/s — the fastest throughput of any
   configuration in the study until Round 3**, trading blows with tuned
   Rust. C2 compiles
   allocation-free primitive-array code superbly; Java was never slow — its
   *idioms* were. The cost is that the tuned code no longer looks like Java
   anyone would write by default, which is exactly what LMAX engineers have
   said for fifteen years.
8. **Pinning a GC'd runtime without budgeting cores for the collector is a
   disaster.** Restricting the Java process to the same two P-cores the
   Rust/C++ runs were pinned to — so the two busy-spin threads starve G1's
   collector and the JIT — inflated p99 to ~800 µs and p99.9 to 7.6 ms,
   several times *worse* than unrestricted. Go took the same restriction far
   more gracefully (its p99.9 actually improved to 12 µs, with a modestly
   worse extreme tail). Affinity plans must count the runtime's helper
   threads, not just your own.
9. **Idiomatic Rust vs Go on Linux is a crossover, not a ranking.** Rust
   idiomatic wins p99.9 (4 µs vs 130 µs); Go wins p99.99 (236 µs vs 1.8 ms,
   its concurrent GC never producing the allocator's rare multi-ms stall).
   Which one is "better" depends on which percentile your SLA is written
   against — until you remove allocation from the hot path, at which point
   tuned Rust/C++ beat everything at every percentile.

## Round 1 — macOS, out of the box, where the study started

The first round ran on an everyday desktop with everything at defaults. Its
headline tables are below and its lessons hold — but its diagnosis of the
Rust/C++ tail ("the OS scheduler") turned out to be mostly wrong, which is
exactly what Round 2 above demonstrates. It is kept as written because the
wrong attribution is instructive.

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

### What Round 1 showed

1. **The architecture, not the language, sets the median.** A single-writer
   core behind a ring buffer processes an order in a few hundred nanoseconds
   in all four languages. If p50 is what you care about, any of them is
   already there.
2. **Java's tail is GC-bound and reproduces every run.** p99.9 in the
   milliseconds, every time, tracking the collection counters one-for-one.
   The idiomatic-collections book (`TreeMap<Long, ArrayDeque<Long>>`, boxed
   `Long`s, an object per resting order) produces steady garbage, and G1's
   pauses land in the distribution exactly where trading systems can least
   afford them.
3. **Go is the surprise: the tightest, most reproducible tail of the four.**
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

All four are the same shape — the LMAX pattern:

![The LMAX Disruptor — single-producer/single-consumer ring buffer between the workload producer and the single-writer matching core](disruptor.svg)

The whole program, as pseudocode — the ring buffer is the delivery mechanism,
the matching algorithm is the payload of one arm of the consumer's switch:

```python
# ============ PRODUCER THREAD ============
while True:
    op = next_operation()                   # splitmix64 workload gen
    slot = ring.claim()                     # spins only if the ring is full
    slot.write(op)                          # in place — the slot is pre-allocated; no allocation, ever
    ring.publish()                          # advance the producer cursor (release store)

# ============ CONSUMER THREAD — single writer, owns ALL book state ============
seq = 0
while True:
    while producer_cursor <= seq: spin()    # busy-wait (acquire load)
    seq += 1
    e = ring[seq % N]                       # N is a power of two: computed as seq & (N-1)
    match e.kind:
        case LIMIT:  limit(e)               # the matching algorithm, below
        case CANCEL: cancel(e.id)           # remove from the id map only (lazy)
    consumer_cursor = seq                   # frees the slot for reuse

# ============ limit(e) — price-time priority matching ============
while e.qty > 0:
    level = best_opposite_level()           # lowest ask for a buy, highest bid for a sell
    if level is None or not crosses(level.price, e.price): break
    while e.qty > 0 and not level.empty():
        head = level.front()                # oldest order at this price: FIFO = time priority
        if head not in id_map:              # lazily-cancelled tombstone
            level.pop_front(); continue
        fill = min(e.qty, head.qty)
        head.qty -= fill; e.qty -= fill     # a partial fill stays AT THE HEAD (keeps its priority)
        if head.qty == 0: id_map.remove(head); level.pop_front()
    if level.empty(): remove_level(level)   # tuned engines: advance the best pointer instead
if e.qty > 0: rest(e)                       # append to the FIFO at e.price; insert into the id map
```

Note what is absent from `limit()`: locks, atomics, defensive copies. The ring's
single-consumer contract means the matching core runs as if single-threaded —
the two cursors are the only shared state in the program, so all concurrency is
paid at the boundary. Draining in strict sequence order is also what makes the
book a deterministic fold over the event stream, which is what the
cross-language checksum (and, in a real venue, replay recovery) relies on.

### When the producer laps the ring: backpressure

Sequences `s` and `s − N` map to the same slot, so the producer may never run
more than N ahead of the slowest consumer — `claim()` enforces it:

```
claim(s):  wait until  s − consumer_cursor ≤ N     # else the slot still holds an unread event
```

That wait is visible verbatim in the hand-rolled rings (`go/ring.go`'s
`claim()` spins while `next − cons > N`; `cpp/ring.hpp` likewise, behind a
cached lower bound of the consumer cursor — the Disruptor's *cached gating
sequence* — so the shared cache line is only read when the ring looks full)
and happens
inside `RingBuffer.next()` / `publish()` in the Java and Rust libraries. With
multiple consumers the producer gates on the *minimum* of their cursors — at
LMAX the slowest of journaler, replicator, and unmarshaller set the wrap limit.

A full bounded buffer only has three possible policies, and the choice is a
design statement:

1. **Drop / conflate** — right for market data (only the latest price
   matters), catastrophic for orders: you cannot conflate a fill.
2. **Buffer unboundedly** — hides overload as growing memory and latency until
   it fails at the worst moment.
3. **Backpressure — stall the producer** ← the Disruptor. Loss is impossible
   by construction, and overload surfaces immediately at the boundary, where
   the layer above can act on it (reject orders, throttle a client, shed).

Two consequences: under saturation, publish latency honestly includes the wait
(and the scheduled-time stamping keeps the histogram honest about it); and
**ring size is the backpressure budget** — LMAX's 20-million-slot production
input ring (vs 65,536 here, matching their published throughput tests) buys
~3 seconds of burst absorption at 6M events/s before a slow downstream
consumer stalls order intake. Sizing the ring is choosing how long a consumer
may misbehave before the whole system feels it.

In this benchmark, latency mode never approaches the wrap (the consumer trails
by a slot or two); throughput mode lives at the ring's edge continuously —
which is exactly why it measures the matching core's drain rate rather than
the generator.

- One producer, one consumer, busy-spin on both sides, ring size 65,536.
- Transports: Java uses the original Disruptor (`BusySpinWaitStrategy`,
  `ProducerType.SINGLE`); Rust the `disruptor` crate (`BusySpin`); Go and C++
  hand-rolled SPSC rings — two cache-line-padded atomic cursors over a
  pre-allocated event array (`go/ring.go`, `cpp/ring.hpp`), acquire/release
  ordering, busy-spin on both sides.
- All book state is owned by the consumer thread: no locks, no sharing, no
  concurrent data structures. Determinism is a design property, which is what
  makes the cross-language checksum possible at all.

### The matching engine — a CLOB

The "known algorithm" here is a **central limit order book**: the core
mechanism of essentially every modern exchange. All resting limit orders for
an instrument are organized as two sides (bids and asks) of **price levels**,
orders queued **first-in-first-out within each level**. An incoming order
matches against the best opposite price first (*price priority*), oldest
order first at that price (*time priority*); whatever doesn't match rests in
the book and becomes the liquidity the next arrival trades against.
Price-time priority is what "the matching algorithm" means at most equity,
futures, and crypto venues.

How each CLOB concept maps onto this code:

| CLOB concept | Where it is here |
|---|---|
| The book (two sides of price levels) | Ordered maps in the idiomatic engines (Java `TreeMap`, Rust `BTreeMap`, Go `google/btree` — the stdlib has none — C++ `std::map`); the dense array **price ladder** in the tuned engines |
| Time priority within a level | The FIFO queue per level (deque → slice-with-head-index) |
| Price priority / best bid-offer | Tree min/max → the `best_bid`/`best_ask` pointers |
| An aggressive order sweeping the book | The nested matching loop: cross the best level, FIFO through it, advance to the next level |
| Partial fills and resting residuals | Quantity decrement; residual inserted at its price level |
| Cancels (the bulk of real exchange flow) | **Lazy cancellation**: the id is removed from the id map only; dead ids are skipped when they surface at a queue head — a real production technique, because cancel-heavy flow makes eager level surgery expensive |
| The exchange's input pipeline | The ring buffer + single-writer thread — the LMAX architecture, which is how production matching engines are actually hosted |
| Deterministic matching (a regulatory and failover requirement at real venues) | The design property this repo exploits for the cross-language checksum |

So a benchmark "op" is an order-book operation — an insert, match, or cancel
against a live book holding hundreds of thousands of resting orders — and
the latency percentiles are literally "how long the matching engine takes to
process an order."

**What a production CLOB adds that this one deliberately omits**, so the
scope is honest: more order types (market, IOC/FOK, stop, iceberg/hidden),
cancel-*replace* with its queue-priority rules, self-trade prevention,
pre-trade risk checks (price bands, fat-finger and credit limits),
**market-data publication** (every book change fanned out as an L2/L3 feed —
often half the engineering of a real venue), opening/closing auctions,
per-symbol partitioning across engine instances, throttles and kill
switches, and above all **journaling with replicated replay for failover**
(the Aeron Cluster pattern — see Future work).

### Code layout

Each implementation is split along the LMAX architecture's seams — the same
component names in every language:

| component | role (LMAX analog) | Java | Rust | Go | C++ |
|---|---|---|---|---|---|
| workload | deterministic input gen (the gateway feed) | `SplitMix64` + `Workload` | `workload.rs` | `workload.go` | `workload.hpp` |
| book | idiomatic business-logic state | `Book`, `Resting`, `Engine` | `book.rs` | `book.go` | `book.hpp` |
| tuned book | the low-allocation engine | `TunedBook`, `FlatLevel` | `tuned.rs` | `tuned.go` | `tuned.hpp` |
| ring | the transport | com.lmax.disruptor (library) | `disruptor` crate (library) | `ring.go` | `ring.hpp` |
| event + handler | what flows, and the single-writer core | `Ev`, `CoreHandler` | `harness.rs` | `harness.go` | `main.cpp` |
| harness | modes, pacing, histograms, reporting | `Harness`, `Report`, `Main` | `harness.rs`, `main.rs` | `harness.go`, `main.go` | `main.cpp` |

Java and Rust get their ring from the respective Disruptor libraries; Go and
C++ carry theirs in-tree (`ring.go`, `ring.hpp`) since neither has a canonical
one — which also makes those two files the place to read the pattern's ~40
essential lines.

### The same algorithm, four spellings — per-language implementation notes

The matching loop is line-for-line the same algorithm everywhere (the checksums
prove it). What differs is what each language makes you *say* to express it —
and each difference is a small lesson about that runtime.

**Java** (`java/src/main/java/bench/Main.java`)

```java
TreeMap<Long, ArrayDeque<Long>> bids, asks;   // price -> FIFO of order ids
HashMap<Long, Resting> orders;                // id -> (price, qty)
```

- Best level is one call: `asks.firstEntry()` / `bids.lastEntry()` — the
  red-black `TreeMap` hands you either end, with price and queue in one entry.
- A partial fill mutates the shared `Resting` object through its reference —
  no write-back, the map holds the same object.
- Resting an order is `computeIfAbsent(price, p -> new ArrayDeque<>()).addLast(id)`
  — idiomatic, and the allocation story in miniature: an object per resting
  order, a boxed `Long` for every id that crosses a collection boundary, a tree
  node per price level. The JIT makes the *logic* as fast as anyone's (identical
  p50); this allocation profile is what idiomatic style costs, and it is exactly
  what the tuned `TunedBook` (primitive `long[]` everywhere) deletes.

**Rust** (`rust/src/main.rs`)

```rust
bids: BTreeMap<i64, VecDeque<u64>>, asks: BTreeMap<i64, VecDeque<u64>>,
orders: HashMap<u64, Resting>,     // Resting stored BY VALUE, 16 bytes inline
```

- Best level: `iter_mut().next()` / `next_back()`, with the cross test and the
  `(price, level)` binding done in one `match` guard.
- The shape the borrow checker forces is the interesting part: the inner loop
  holds `level: &mut VecDeque` borrowed out of one field while calling
  `self.orders.get_mut(..)` on another. That compiles only because they are
  *disjoint fields* of `Book` — and that is not an inconvenience, it is the
  compiler enforcing exactly the one-mutator-per-structure discipline a matching
  engine wants anyway.
- `Resting` lives by value inside the map — no per-order heap box at all, one
  reason idiomatic Rust allocates ~18x less than idiomatic C++ here (BTreeMap's
  many-entries-per-node layout is the other).

**Go** (`go/main.go`)

```go
bidPrices, askPrices *btree.BTreeG[int64]   // ordered prices only
bidLevels, askLevels map[int64]*level       // price -> FIFO
orders map[uint64]resting                   // VALUE type
```

- Two structures where the others have one: the stdlib has no ordered map, and
  `google/btree` orders *keys*, so prices live in the btree and level bodies in
  a hash map — two lookups per best-level, and the two must be kept in sync by
  hand on level create and level removal.
- The value-semantics gotcha: `rec := b.orders[head]` returns a **copy**. After
  `rec.qty -= m` you must write it back (`b.orders[head] = rec`) or the fill
  silently doesn't happen — Go forbids taking the address of a map element. The
  upside is no per-order heap object; the trap is that forgetting the write-back
  still compiles. The checksum tests are what make this safe to touch.

**C++** (`cpp/main.cpp`)

```cpp
std::map<int64_t, std::deque<uint64_t>> bids, asks;
std::unordered_map<uint64_t, Resting> orders;
```

- Best ask is `asks.begin()`; best bid is `std::prev(bids.end())` — guarded by
  an `empty()` check first, because `prev(end())` on an empty map is undefined
  behavior. `std::map` has no `lastEntry()`.
- `auto& lvl = it->second` stays valid through the whole inner loop because
  node-based `std::map` never invalidates references on other keys' operations —
  and erasing the emptied level by iterator is the one moment after which `it`
  and `lvl` must never be touched again. In the other three languages those
  lifetime rules are enforced (GC or borrow checker); here they are a
  code-review obligation.
- The throughput surprise lives here: a heap node per price level and a chunk
  per deque block — 5.5M inline mallocs per 10M ops, each paid retail, which is
  how idiomatic C++ loses to Java's bump-pointer allocation on throughput while
  beating it on tail.

**Summary**

| concern | Java | Rust | Go | C++ |
|---|---|---|---|---|
| best level | `firstEntry`/`lastEntry` | `iter_mut().next{_back}()` | `btree.Min/Max` + map lookup | `begin()` / `prev(end())` |
| partial fill | mutate shared object | `get_mut`, in place | copy, mutate, **write back** | mutate via iterator |
| per-order cost | boxed key + object | 16 B by value, inline | value in map, no box | entry in node/bucket |
| what the language enforces | nothing (GC forgives) | aliasing, via disjoint-field borrows | nothing (write-back is on you) | nothing (iterator rules are on you) |

The tuned engines erase most of these differences on purpose — dense array
ladder, pooled level FIFOs, and id-indexed arrays in Java, Go, and (since
Round 3) C++, with Rust still on a reserved FxHash map — which is how a ~450x
idiomatic spread at p99.99 compresses to ~5x: once allocation is out of the
hot path, what remains is runtime character, not language identity.

### The workload

Deterministic, generated by splitmix64 implemented identically in all four
languages (seed 42): 55% passive limits (1–20 ticks behind the touch), 30%
aggressive limits (0–9 ticks into/through the spread), 15% cancels of recent
ids; the mid random-walks in [500, 1500]. ~68% of limit orders end up trading.

## Test environments

| | Linux (Round 2) | macOS (Round 1) |
|---|---|---|
| CPU | Intel i9-13980HX, 24 cores / 32 threads (8P+16E, P-cores to 5.6 GHz) | Apple M1 Max, 10 cores (8P+2E) |
| RAM | 64 GB | 32 GB |
| OS | Ubuntu 24.04.4 LTS, kernel 6.14 | macOS 26.3.1 |
| Rust | rustc 1.97 | rustc 1.92 |
| Java | Temurin JDK 22.0.2, default G1 | Oracle JDK 22.0.1, default G1 |
| Go | go 1.26.5 | go 1.26.5 |
| C++ | g++ 13.3, `-O3 -std=c++20` | Homebrew clang 21, `-O3 -std=c++20` |
| Conditions | lightly loaded home server, no `isolcpus`; pinned runs on distinct physical P-cores (8, 10) | shared desktop, no isolation |

Both are everyday machines, deliberately: the study measures what languages and
disciplines deliver under realistic conditions, not on a lab-isolated box —
and run-to-run ranges are reported for exactly that reason.

## Methodology notes

- **Coordinated omission**: in latency mode the producer is paced (250k ops/s)
  and each event is stamped with its *scheduled* publish time, not the actual
  one — a delayed publish therefore charges the delay to itself, per
  HdrHistogram practice.
- **Warmup**: two layers. Since Round 4, every process first runs 1M ops
  through a full discarded pipeline (own ring, engine, generator) so no
  measured run executes cold — C2-compiled code in Java, warm predictors,
  caches, and allocator everywhere — with a forced collection afterwards in
  the GC languages so warmup garbage cannot bill a measured run. Within each
  latency run, the first 20% of ops are additionally unmeasured.
- **Percentiles**: Java/Rust/Go record into HdrHistogram (3 significant
  digits); C++ stores every raw sample in a preallocated vector and sorts at
  the end — exact percentiles, and a cross-check that histogram resolution
  isn't shaping the other results.
- Throughput mode measures the drain-inclusive wall clock: publish loop plus a
  final sentinel event, ending when the consumer has processed everything.
- Round 1 caveats: macOS, no core pinning or isolation, shared desktop
  machine, one JVM/one binary run per mode, default JVM flags (JDK 22,
  default G1). Treat absolute numbers as indicative, deltas as the signal.
  Run-to-run tail variance is large on a shared machine — Rust's p99.9 has
  been observed anywhere from ~40 µs to ~3 ms across runs, while Java's
  multi-millisecond p99.9+ and Go's tens-of-microseconds p99.9 both reproduce
  every run; that reproducibility is the actual finding. Round 2 (Linux,
  pinned cores) tightened exactly what this predicts; JMH-style forking
  remains future work.

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

# Go knob (Round 4): disable the collector outright — safe with ENGINE=tuned
# (steady state allocates ~nothing); the gogc= line in the output confirms it
GOGC=off ENGINE=tuned ./clob-bench all
```

Or all four, back to back, via `./run.sh`.

## Reusing this code

This is a benchmark, so reuse means reading, copying, and conforming — not
importing. Four recipes, in descending order of uniqueness:

1. **Verify your own matching engine** — the deterministic workload plus the
   book checksum form a free correctness oracle: implement the semantics, feed
   the seed-42 stream, compare against the published vectors. Everything you
   need, including the exact spec and the reference numbers, is in
   [CONFORMANCE.md](CONFORMANCE.md). This works for a fifth language, a
   rewrite, or a refactor of any of the four here (it is how this repo's own
   restructuring was proven behavior-preserving).
2. **Lift a ring** — `go/ring.go` and `cpp/ring.hpp` are dependency-free
   ~40-line SPSC busy-spin rings under MIT: copy the file, rename the event
   type, done. (Java and Rust users: use the real
   [Disruptor](https://github.com/LMAX-Exchange/disruptor) /
   [`disruptor`](https://crates.io/crates/disruptor) libraries, as this repo
   does.) The Go module is importable as
   `github.com/paul-weiss/lmax-bench/go` if you prefer `go get` to copying.
3. **Start a book from ours** — `book.*` / `tuned.*` are single-responsibility
   files meant to be copied as the skeleton of a real book and then owned:
   your order types, your tick handling, your id scheme. An order book is
   domain logic, not a dependency — which is why these are deliberately not
   published as packages.
4. **Steal the harness methodology** — the coordinated-omission-safe pacing
   loop, warmup discipline, HdrHistogram wiring, and the allocation/GC
   counters (`harness.*`) are a template for benchmarking any event-processing
   core: swap the book for your component, keep the measurement scaffolding.

## Future work

- **Symmetric Round 3 treatment for Rust** (and Go/Java rings): a direct
  id-indexed array in `tuned.rs` where FxHash still hashes and probes, and
  cached-cursor rings where the hand-rolled ones still load the shared
  cursor per event. Until then the Round 3 ladder is provisional — see
  Round 3, finding 4.
- Agrona-based Java variant (the production-grade version of the primitive-
  array approach). (The `GOGC=off` Go run landed in Round 4.)
- Linux with `isolcpus`/`nohz_full` (true isolation, not just affinity);
  `perf` flame graphs of the remaining tuned-engine tail. After Round 4 this
  is the main lever left: it is what would make p99.99 measure the engine
  rather than the machine.
- Journal + replay to demonstrate deterministic recovery (the other half of
  the LMAX story).
- Multi-producer mode (N gateway threads, one core).

## License

MIT
