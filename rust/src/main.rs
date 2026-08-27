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

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use disruptor::{build_single_producer, BusySpin, Producer, ProcessorSettings};
use hdrhistogram::Histogram;
use rustc_hash::FxHashMap;

// ---------------------------------------------------------------- allocation counter

struct CountingAlloc;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// ---------------------------------------------------------------- workload generator
// splitmix64 — implemented identically in the Java harness so both sides
// consume a bit-identical operation stream.

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

const KIND_LIMIT: u8 = 0;
const KIND_CANCEL: u8 = 1;
const KIND_REPORT: u8 = 2;
const SIDE_BID: u8 = 0;

struct Op {
    kind: u8,
    side: u8,
    price: i64,
    qty: i64,
    id: u64,
}

struct Gen {
    rng: SplitMix64,
    mid: i64,
    last_id: u64,
}

impl Gen {
    fn new(seed: u64) -> Self {
        Gen { rng: SplitMix64(seed), mid: 1000, last_id: 0 }
    }

    fn next_op(&mut self) -> Op {
        let r = self.rng.next();
        if r % 7 == 0 {
            self.mid += ((r >> 40) % 3) as i64 - 1;
            self.mid = self.mid.clamp(500, 1500);
        }
        let c = r % 100;
        if c < 85 {
            // limit order: c < 55 passive (behind the touch), else aggressive
            // (into / across the spread so it will often trade)
            let side = ((r >> 8) & 1) as u8;
            let aggressive = c >= 55;
            let offset = if aggressive {
                ((r >> 16) % 10) as i64
            } else {
                1 + ((r >> 16) % 20) as i64
            };
            let price = if (side == SIDE_BID) != aggressive {
                self.mid - offset
            } else {
                self.mid + offset
            };
            let qty = 1 + ((r >> 32) % 100) as i64;
            self.last_id += 1;
            Op { kind: KIND_LIMIT, side, price, qty, id: self.last_id }
        } else {
            // cancel a recent id; already-gone ids are harmless no-ops
            let back = (r >> 16) % 5000;
            let id = self.last_id.saturating_sub(back);
            Op { kind: KIND_CANCEL, side: 0, price: 0, qty: 0, id }
        }
    }
}

// ---------------------------------------------------------------- the book
// Price-time priority. Lazy cancellation: cancel removes from the id map;
// stale ids are skipped when they reach the head of a level queue.

#[derive(Clone, Copy)]
struct Resting {
    price: i64,
    qty: i64,
}

#[derive(Default)]
struct Book {
    bids: BTreeMap<i64, VecDeque<u64>>,
    asks: BTreeMap<i64, VecDeque<u64>>,
    orders: HashMap<u64, Resting>,
    fills: u64,
    volume: u64,
}

impl Book {
    fn limit(&mut self, side: u8, price: i64, mut qty: i64, id: u64) {
        while qty > 0 {
            // best opposite level that crosses, else rest the residual
            let (best_price, level) = if side == SIDE_BID {
                match self.asks.iter_mut().next() {
                    Some((p, l)) if *p <= price => (*p, l),
                    _ => break,
                }
            } else {
                match self.bids.iter_mut().next_back() {
                    Some((p, l)) if *p >= price => (*p, l),
                    _ => break,
                }
            };
            while qty > 0 {
                let Some(&head) = level.front() else { break };
                let Some(rec) = self.orders.get_mut(&head) else {
                    level.pop_front(); // lazily-cancelled order
                    continue;
                };
                let m = qty.min(rec.qty);
                rec.qty -= m;
                qty -= m;
                self.fills += 1;
                self.volume += m as u64;
                if rec.qty == 0 {
                    self.orders.remove(&head);
                    level.pop_front();
                }
            }
            if level.is_empty() {
                if side == SIDE_BID {
                    self.asks.remove(&best_price);
                } else {
                    self.bids.remove(&best_price);
                }
            }
        }
        if qty > 0 {
            let book_side = if side == SIDE_BID { &mut self.bids } else { &mut self.asks };
            book_side.entry(price).or_default().push_back(id);
            self.orders.insert(id, Resting { price, qty });
        }
    }

    fn cancel(&mut self, id: u64) {
        self.orders.remove(&id);
    }

    /// Deterministic checksum over live orders in (price, FIFO) order — must be
    /// identical between the Rust and Java runs on the same workload.
    fn checksum(&self) -> u64 {
        let mut acc = 0u64;
        let fold = |acc: &mut u64, id: u64, r: &Resting| {
            let mut h = SplitMix64(*acc ^ id ^ ((r.price as u64) << 20) ^ (r.qty as u64));
            *acc = h.next();
        };
        for level in self.bids.values() {
            for &id in level {
                if let Some(r) = self.orders.get(&id) {
                    fold(&mut acc, id, r);
                }
            }
        }
        for level in self.asks.values() {
            for &id in level {
                if let Some(r) = self.orders.get(&id) {
                    fold(&mut acc, id, r);
                }
            }
        }
        acc
    }
}

// ---------------------------------------------------------------- tuned book
// Same semantics as `Book` (identical fills/volume/checksum on the same
// stream), engineered for the hot path: a dense array-backed price ladder
// with best-bid/best-ask pointers instead of an ordered tree, pooled
// slice-FIFO levels (head index, cleared not freed), FxHash with pre-reserved
// capacity for the id map. Steady state approaches zero allocation.

const LADDER_BASE: i64 = 0;
const LADDER_LEN: usize = 4096; // workload prices stay well inside [480, 1521]

#[derive(Default)]
struct FlatLevel {
    ids: Vec<u64>,
    head: usize,
}

impl FlatLevel {
    #[inline]
    fn is_empty(&self) -> bool {
        self.head == self.ids.len()
    }
    #[inline]
    fn reset(&mut self) {
        self.ids.clear();
        self.head = 0;
    }
}

struct TunedBook {
    bids: Vec<FlatLevel>,
    asks: Vec<FlatLevel>,
    best_bid: i64, // -1 when no bids
    best_ask: i64, // i64::MAX when no asks
    orders: FxHashMap<u64, Resting>,
    fills: u64,
    volume: u64,
}

impl TunedBook {
    fn new() -> Self {
        let mk = || (0..LADDER_LEN).map(|_| FlatLevel::default()).collect();
        let mut orders = FxHashMap::default();
        orders.reserve(1 << 21);
        TunedBook { bids: mk(), asks: mk(), best_bid: -1, best_ask: i64::MAX, orders, fills: 0, volume: 0 }
    }

    fn limit(&mut self, side: u8, price: i64, mut qty: i64, id: u64) {
        while qty > 0 {
            let best = if side == SIDE_BID { self.best_ask } else { self.best_bid };
            let crosses = if side == SIDE_BID { best <= price } else { best >= price };
            if best < LADDER_BASE || best >= LADDER_BASE + LADDER_LEN as i64 || !crosses {
                break;
            }
            let level = if side == SIDE_BID {
                &mut self.asks[(best - LADDER_BASE) as usize]
            } else {
                &mut self.bids[(best - LADDER_BASE) as usize]
            };
            while qty > 0 {
                let Some(&head) = level.ids.get(level.head) else { break };
                let Some(rec) = self.orders.get_mut(&head) else {
                    level.head += 1; // lazily-cancelled order
                    continue;
                };
                let m = qty.min(rec.qty);
                rec.qty -= m;
                qty -= m;
                self.fills += 1;
                self.volume += m as u64;
                if rec.qty == 0 {
                    self.orders.remove(&head);
                    level.head += 1;
                }
            }
            if level.is_empty() {
                level.reset();
                // advance the best pointer to the next occupied level
                if side == SIDE_BID {
                    self.best_ask = (best + 1..LADDER_BASE + LADDER_LEN as i64)
                        .find(|p| !self.asks[(p - LADDER_BASE) as usize].is_empty())
                        .unwrap_or(i64::MAX);
                } else {
                    self.best_bid = (LADDER_BASE..best)
                        .rev()
                        .find(|p| !self.bids[(p - LADDER_BASE) as usize].is_empty())
                        .unwrap_or(-1);
                }
            }
        }
        if qty > 0 {
            let idx = (price - LADDER_BASE) as usize;
            if side == SIDE_BID {
                self.bids[idx].ids.push(id);
                self.best_bid = self.best_bid.max(price);
            } else {
                self.asks[idx].ids.push(id);
                self.best_ask = self.best_ask.min(price);
            }
            self.orders.insert(id, Resting { price, qty });
        }
    }

    fn cancel(&mut self, id: u64) {
        self.orders.remove(&id);
    }

    fn checksum(&self) -> u64 {
        let mut acc = 0u64;
        let fold = |acc: &mut u64, id: u64, r: &Resting| {
            let mut h = SplitMix64(*acc ^ id ^ ((r.price as u64) << 20) ^ (r.qty as u64));
            *acc = h.next();
        };
        for side in [&self.bids, &self.asks] {
            for level in side {
                for &id in &level.ids[level.head..] {
                    if let Some(r) = self.orders.get(&id) {
                        fold(&mut acc, id, r);
                    }
                }
            }
        }
        acc
    }
}

// engine dispatch: idiomatic (default) or tuned (ENGINE=tuned)

enum AnyBook {
    Idio(Book),
    Tuned(Box<TunedBook>),
}

impl AnyBook {
    fn limit(&mut self, side: u8, price: i64, qty: i64, id: u64) {
        match self {
            AnyBook::Idio(b) => b.limit(side, price, qty, id),
            AnyBook::Tuned(b) => b.limit(side, price, qty, id),
        }
    }
    fn cancel(&mut self, id: u64) {
        match self {
            AnyBook::Idio(b) => b.cancel(id),
            AnyBook::Tuned(b) => b.cancel(id),
        }
    }
    fn stats(&self) -> (u64, u64, usize, u64) {
        match self {
            AnyBook::Idio(b) => (b.fills, b.volume, b.orders.len(), b.checksum()),
            AnyBook::Tuned(b) => (b.fills, b.volume, b.orders.len(), b.checksum()),
        }
    }
}

// ---------------------------------------------------------------- harness

struct Event {
    kind: u8,
    side: u8,
    price: i64,
    qty: i64,
    id: u64,
    ts: u64, // scheduled-publish nanos since run epoch; 0 = don't measure
}

struct Report {
    fills: u64,
    volume: u64,
    resting: usize,
    checksum: u64,
    hist: Histogram<u64>,
}

struct Core {
    book: AnyBook,
    hist: Histogram<u64>,
    epoch: Instant,
    report: Arc<Mutex<Option<Report>>>,
}

impl Core {
    fn on_event(&mut self, e: &Event) {
        match e.kind {
            KIND_LIMIT => self.book.limit(e.side, e.price, e.qty, e.id),
            KIND_CANCEL => self.book.cancel(e.id),
            _ => {
                let (fills, volume, resting, checksum) = self.book.stats();
                *self.report.lock().unwrap() =
                    Some(Report { fills, volume, resting, checksum, hist: self.hist.clone() });
                return;
            }
        }
        if e.ts != 0 {
            let now = self.epoch.elapsed().as_nanos() as u64;
            let _ = self.hist.record(now.saturating_sub(e.ts).max(1));
        }
    }
}

fn env_core(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn engine_from_env() -> (AnyBook, &'static str) {
    if std::env::var("ENGINE").as_deref() == Ok("tuned") {
        (AnyBook::Tuned(Box::new(TunedBook::new())), "tuned")
    } else {
        (AnyBook::Idio(Book::default()), "idiomatic")
    }
}

const RING_SIZE: usize = 65_536;

fn run(total_ops: u64, warmup_ops: u64, rate: Option<u64>) -> (Report, f64) {
    let epoch = Instant::now();
    let report: Arc<Mutex<Option<Report>>> = Arc::new(Mutex::new(None));
    let (book, engine) = engine_from_env();
    let pin_prod = env_core("PIN_PROD");
    let pin_cons = env_core("PIN_CONS");
    println!(
        "  engine={engine} pin_prod={} pin_cons={}",
        pin_prod.map_or("-".into(), |c| c.to_string()),
        pin_cons.map_or("-".into(), |c| c.to_string())
    );
    let mut core = Core {
        book,
        hist: Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap(),
        epoch,
        report: Arc::clone(&report),
    };

    let factory = || Event { kind: 0, side: 0, price: 0, qty: 0, id: 0, ts: 0 };
    let handler = move |e: &Event, _seq: i64, _eob: bool| core.on_event(e);
    let builder = build_single_producer(RING_SIZE, factory, BusySpin);
    let mut producer = match pin_cons {
        Some(c) => builder.pin_at_core(c).handle_events_with(handler).build(),
        None => builder.handle_events_with(handler).build(),
    };
    // Pin the producer only after the consumer thread exists: on Linux, child
    // threads inherit the parent's affinity mask, so pinning first would leave
    // the consumer no core to be pinned to.
    if let Some(c) = pin_prod {
        core_affinity::set_for_current(core_affinity::CoreId { id: c });
    }

    let mut wl = Gen::new(42);
    let interval = rate.map(|r| 1_000_000_000 / r);
    let mut next_due: u64 = epoch.elapsed().as_nanos() as u64;

    let allocs0 = ALLOCS.load(Ordering::Relaxed);
    let t0 = Instant::now();
    for i in 0..total_ops {
        let op = wl.next_op();
        let ts = match interval {
            Some(iv) => {
                next_due += iv;
                while (epoch.elapsed().as_nanos() as u64) < next_due {
                    std::hint::spin_loop();
                }
                if i >= warmup_ops { next_due } else { 0 }
            }
            None => 0,
        };
        producer.publish(|e| {
            e.kind = op.kind;
            e.side = op.side;
            e.price = op.price;
            e.qty = op.qty;
            e.id = op.id;
            e.ts = ts;
        });
    }
    producer.publish(|e| {
        e.kind = KIND_REPORT;
        e.ts = 0;
    });
    drop(producer); // joins the consumer thread
    let wall = t0.elapsed().as_secs_f64();
    let allocs = ALLOCS.load(Ordering::Relaxed) - allocs0;

    let rep = report.lock().unwrap().take().expect("consumer did not report");
    println!("  heap allocations during run: {allocs}");
    (rep, wall)
}

fn print_report(rep: &Report, wall: f64, total_ops: u64, latency: bool) {
    println!(
        "  ops={} wall={:.3}s throughput={:.0} ops/s",
        total_ops,
        wall,
        total_ops as f64 / wall
    );
    println!(
        "  fills={} volume={} resting={} checksum={:016x}",
        rep.fills, rep.volume, rep.resting, rep.checksum
    );
    if latency {
        let h = &rep.hist;
        let us = |q: f64| h.value_at_quantile(q) as f64 / 1_000.0;
        println!(
            "  latency µs: p50={:.3} p90={:.3} p99={:.3} p99.9={:.1} p99.99={:.1} max={:.1} (n={})",
            us(0.50),
            us(0.90),
            us(0.99),
            us(0.999),
            us(0.9999),
            h.max() as f64 / 1_000.0,
            h.len()
        );
    }
}

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
