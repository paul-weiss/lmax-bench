//! The idiomatic price-time priority book.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::workload::{SplitMix64, SIDE_BID};

// ---------------------------------------------------------------- the book
// Price-time priority. Lazy cancellation: cancel removes from the id map;
// stale ids are skipped when they reach the head of a level queue.

#[derive(Clone, Copy)]
pub struct Resting {
    pub price: i64,
    pub qty: i64,
}

#[derive(Default)]
pub struct Book {
    pub bids: BTreeMap<i64, VecDeque<u64>>,
    pub asks: BTreeMap<i64, VecDeque<u64>>,
    pub orders: HashMap<u64, Resting>,
    pub fills: u64,
    pub volume: u64,
}

impl Book {
    pub fn limit(&mut self, side: u8, price: i64, mut qty: i64, id: u64) {
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

    pub fn cancel(&mut self, id: u64) {
        self.orders.remove(&id);
    }

    /// Deterministic checksum over live orders in (price, FIFO) order — must be
    /// identical between the Rust and Java runs on the same workload.
    pub fn checksum(&self) -> u64 {
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

