//! Engine dispatch, the ring-buffer harness, and reporting.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use disruptor::{build_single_producer, BusySpin, Producer, ProcessorSettings};
use hdrhistogram::Histogram;

use crate::alloc::ALLOCS;
use crate::book::Book;
use crate::tuned::TunedBook;
use crate::workload::{Gen, KIND_CANCEL, KIND_LIMIT, KIND_REPORT};

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

pub struct Report {
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

// Power of two, as the disruptor crate requires: the ring indexes slots with
// `seq & (N-1)` — a single AND computing seq mod N, not a division.
const RING_SIZE: usize = 65_536;

pub fn run(total_ops: u64, warmup_ops: u64, rate: Option<u64>, quiet: bool) -> (Report, f64) {
    let epoch = Instant::now();
    let report: Arc<Mutex<Option<Report>>> = Arc::new(Mutex::new(None));
    let (book, engine) = engine_from_env();
    let pin_prod = env_core("PIN_PROD");
    let pin_cons = env_core("PIN_CONS");
    if !quiet {
        println!(
            "  engine={engine} pin_prod={} pin_cons={}",
            pin_prod.map_or("-".into(), |c| c.to_string()),
            pin_cons.map_or("-".into(), |c| c.to_string())
        );
    }
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
    if !quiet {
        println!("  heap allocations during run: {allocs}");
    }
    (rep, wall)
}

pub fn print_report(rep: &Report, wall: f64, total_ops: u64, latency: bool) {
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

