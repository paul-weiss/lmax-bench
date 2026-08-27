//! splitmix64 workload generator — bit-identical across all four languages.

// ---------------------------------------------------------------- workload generator
// splitmix64 — implemented identically in the Java harness so both sides
// consume a bit-identical operation stream.

pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

pub const KIND_LIMIT: u8 = 0;
pub const KIND_CANCEL: u8 = 1;
pub const KIND_REPORT: u8 = 2;
pub const SIDE_BID: u8 = 0;

pub struct Op {
    pub kind: u8,
    pub side: u8,
    pub price: i64,
    pub qty: i64,
    pub id: u64,
}

pub struct Gen {
    pub rng: SplitMix64,
    pub mid: i64,
    pub last_id: u64,
}

impl Gen {
    pub fn new(seed: u64) -> Self {
        Gen { rng: SplitMix64(seed), mid: 1000, last_id: 0 }
    }

    pub fn next_op(&mut self) -> Op {
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

