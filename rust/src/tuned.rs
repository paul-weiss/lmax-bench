//! The tuned book: array ladder + best pointers + FxHash, near-zero steady-state allocation.

use rustc_hash::FxHashMap;

use crate::book::Resting;
use crate::workload::{SplitMix64, SIDE_BID};

// ---------------------------------------------------------------- tuned book
// Same semantics as `Book` (identical fills/volume/checksum on the same
// stream), engineered for the hot path: a dense array-backed price ladder
// with best-bid/best-ask pointers instead of an ordered tree, pooled
// slice-FIFO levels (head index, cleared not freed), FxHash with pre-reserved
// capacity for the id map. Steady state approaches zero allocation.

pub const LADDER_BASE: i64 = 0;
pub const LADDER_LEN: usize = 4096; // workload prices stay well inside [480, 1521]

#[derive(Default)]
pub struct FlatLevel {
    pub ids: Vec<u64>,
    pub head: usize,
}

impl FlatLevel {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == self.ids.len()
    }
    #[inline]
    pub fn reset(&mut self) {
        self.ids.clear();
        self.head = 0;
    }
}

pub struct TunedBook {
    pub bids: Vec<FlatLevel>,
    pub asks: Vec<FlatLevel>,
    pub best_bid: i64, // -1 when no bids
    pub best_ask: i64, // i64::MAX when no asks
    pub orders: FxHashMap<u64, Resting>,
    pub fills: u64,
    pub volume: u64,
}

impl TunedBook {
    pub fn new() -> Self {
        let mk = || (0..LADDER_LEN).map(|_| FlatLevel::default()).collect();
        let mut orders = FxHashMap::default();
        orders.reserve(1 << 21);
        TunedBook { bids: mk(), asks: mk(), best_bid: -1, best_ask: i64::MAX, orders, fills: 0, volume: 0 }
    }

    pub fn limit(&mut self, side: u8, price: i64, mut qty: i64, id: u64) {
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

    pub fn cancel(&mut self, id: u64) {
        self.orders.remove(&id);
    }

    pub fn checksum(&self) -> u64 {
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

