# Conformance suite — verify your own matching engine against four references

The most reusable thing in this repository is not the code — it is the
**correctness oracle**. The workload is deterministic and the book is a pure
function of it, so any matching engine that implements the semantics below and
consumes the same operation stream must finish with byte-identical statistics.
Four independent implementations (Java, Rust, Go, C++) produce the vectors at
the bottom; if your fifth implementation matches them, it is behaviorally
identical to all four.

This gives you, for free, the hardest thing to test about a matching engine:
end-to-end behavioral correctness over millions of adversarial operations.

## 1. The PRNG — splitmix64

All arithmetic is unsigned 64-bit with wraparound; `>>` is a logical
(zero-filling) shift.

```
state: u64            # initialized to the seed; the reference seed is 42

next():
    state = state + 0x9E3779B97F4A7C15
    z = state
    z = (z XOR (z >> 30)) * 0xBF58476D1CE4E5B9
    z = (z XOR (z >> 27)) * 0x94D049BB133111EB
    return z XOR (z >> 31)
```

## 2. The operation stream

State: `mid = 1000`, `last_id = 0`. For each operation, draw `r = next()`
once, then:

```
if r mod 7 == 0:
    mid = mid + ((r >> 40) mod 3) - 1
    mid = clamp(mid, 500, 1500)

c = r mod 100
if c < 85:                                   # NEW LIMIT order
    side       = (r >> 8) AND 1              # 0 = bid (buy), 1 = ask (sell)
    aggressive = (c >= 55)
    offset     = aggressive ? (r >> 16) mod 10
                            : 1 + ((r >> 16) mod 20)
    price      = ((side == bid) != aggressive) ? mid - offset : mid + offset
    qty        = 1 + ((r >> 32) mod 100)
    last_id    = last_id + 1
    id         = last_id
else:                                        # CANCEL
    back = (r >> 16) mod 5000
    id   = (last_id > back) ? last_id - back : 0    # saturating; id 0 never exists
```

All `mod` operations are unsigned. Every quantity above (`side`, `offset`,
`qty`, `back`) is derived from the *same* draw `r`.

## 3. Required matching semantics

Price-time priority, continuous matching, with **lazy cancellation**:

- The book is, per side, price levels each holding a FIFO queue of order ids,
  plus a "live" map id → (price, remaining qty).
- **NEW LIMIT**: while remaining qty > 0, take the best opposite level (lowest
  ask for a buy, highest bid for a sell); stop if none exists or it does not
  cross the order's price. Within the level, repeatedly take the id at the
  FIFO head:
  - if the id is **not** in the live map (it was cancelled), pop it and
    continue — this is the lazy-cancel skip;
  - otherwise fill `m = min(remaining, head.qty)`; decrement both; count
    **one fill event** and add `m` to volume; if the head's qty reaches 0,
    remove it from the live map and pop it — a **partially filled head stays
    at the head** with its residual (it keeps time priority).
  - when the level's queue is empty, remove the level and continue with the
    next best.
  Any remaining qty **rests**: append the id to the FIFO at its price
  (creating the level if needed) and insert it into the live map.
- **CANCEL**: remove the id from the live map. Cancelling an id that is not
  live is a no-op. The references leave the id in its level queue as a
  tombstone (lazy cancellation, skipped at the head later); removing it from
  the queue eagerly is behaviorally equivalent — dead ids are invisible to
  both matching and the checksum — so either strategy conforms.

## 4. Required statistics

- `fills`  — count of fill events (each head-match increments by exactly 1;
  one incoming order sweeping three resting orders = 3 fills).
- `volume` — sum of all matched quantities.
- `resting` — number of live orders at the end.
- `checksum` — fold over the final book, **bids first, then asks; price
  ascending within each side; FIFO order within each level; live ids only**:

```
acc = 0
for each live id in that order, with its (price, qty):
    acc = splitmix64_next( acc XOR id XOR (price << 20) XOR qty )
    # i.e. seed a fresh splitmix64 with that value and take one next()
```

## 5. Reference vectors (seed 42)

Identical from all four reference implementations, in every mode (the mode
only changes timing, never the stream):

| ops | fills | volume | resting | checksum |
|---:|---:|---:|---:|---|
| 500,000 | 336,832 | 8,586,932 | 49,678 | `655b5179049aef04` |
| 1,000,000 | 679,141 | 17,312,555 | 93,116 | `7015904315b82df4` |
| 2,000,000 | 1,351,869 | 34,451,140 | 192,504 | `20baa15fa5841b0a` |
| 10,000,000 | 7,088,742 | 180,770,708 | 631,837 | `89e1e9e4df54122f` |

Reproduce with any reference implementation, e.g.
`./clob-bench throughput 1000000` — the second output line prints all four
numbers. Both engines (`ENGINE=tuned` or default) produce the same vectors.

If your implementation disagrees: the most common divergences are, in order —
signed shifts or signed `mod` where unsigned is required (§1–2); the partially
filled head being re-queued instead of left in place (§3); counting one fill
per incoming order instead of one per resting order matched (§4); and
checksum iteration order or a missing one-`next()` fold (§4).
