package main

// ---------------------------------------------------------------- tuned book
// Same semantics as book (identical fills/volume/checksum on the same
// stream), engineered for the hot path: dense array-backed price ladder with
// best-bid/ask pointers instead of a btree, slice-FIFO levels (cleared, not
// freed), and — because order ids are dense sequential integers — the id map
// replaced by two preallocated arrays indexed by id (qty==0 marks dead).
// Zero allocation in steady state; the GC has nothing to collect.

type tunedLevel struct {
	ids  []uint64
	head int
}

func (l *tunedLevel) empty() bool { return l.head == len(l.ids) }
func (l *tunedLevel) reset()      { l.ids = l.ids[:0]; l.head = 0 }

const ladderBase = 0
const ladderLen = 4096 // workload prices stay well inside [480, 1521]

type tunedBook struct {
	bids, asks []tunedLevel
	bestBid    int64   // -1 when no bids
	bestAsk    int64   // sentinel ladderLen when no asks
	price      []int64 // indexed by order id
	oqty       []int64 // indexed by order id; 0 = dead
	live       int
	fills      uint64
	volume     uint64
}

func newTunedBook(maxID uint64) *tunedBook {
	return &tunedBook{
		bids:    make([]tunedLevel, ladderLen),
		asks:    make([]tunedLevel, ladderLen),
		bestBid: -1,
		bestAsk: ladderLen,
		price:   make([]int64, maxID+2),
		oqty:    make([]int64, maxID+2),
	}
}

func (b *tunedBook) limit(side byte, price, qty int64, id uint64) {
	for qty > 0 {
		var best int64
		if side == sideBid {
			best = b.bestAsk
			if best >= ladderLen || best > price {
				break
			}
		} else {
			best = b.bestBid
			if best < 0 || best < price {
				break
			}
		}
		var lvl *tunedLevel
		if side == sideBid {
			lvl = &b.asks[best-ladderBase]
		} else {
			lvl = &b.bids[best-ladderBase]
		}
		for qty > 0 {
			if lvl.empty() {
				break
			}
			head := lvl.ids[lvl.head]
			if b.oqty[head] == 0 { // lazily-cancelled order
				lvl.head++
				continue
			}
			m := min(qty, b.oqty[head])
			b.oqty[head] -= m
			qty -= m
			b.fills++
			b.volume += uint64(m)
			if b.oqty[head] == 0 {
				b.live--
				lvl.head++
			}
		}
		if lvl.empty() {
			lvl.reset()
			if side == sideBid {
				p := best + 1
				for p < ladderLen && b.asks[p-ladderBase].empty() {
					p++
				}
				b.bestAsk = p
			} else {
				p := best - 1
				for p >= 0 && b.bids[p-ladderBase].empty() {
					p--
				}
				b.bestBid = p
			}
		}
	}
	if qty > 0 {
		if side == sideBid {
			b.bids[price-ladderBase].ids = append(b.bids[price-ladderBase].ids, id)
			b.bestBid = max(b.bestBid, price)
		} else {
			b.asks[price-ladderBase].ids = append(b.asks[price-ladderBase].ids, id)
			b.bestAsk = min(b.bestAsk, price)
		}
		b.price[id] = price
		b.oqty[id] = qty
		b.live++
	}
}

func (b *tunedBook) cancel(id uint64) {
	if b.oqty[id] != 0 {
		b.oqty[id] = 0
		b.live--
	}
}

func (b *tunedBook) stats() (uint64, uint64, int, uint64) {
	var acc uint64
	fold := func(side []tunedLevel) {
		for i := range side {
			lvl := &side[i]
			for _, id := range lvl.ids[lvl.head:] {
				if b.oqty[id] != 0 {
					s := splitMix64{acc ^ id ^ (uint64(b.price[id]) << 20) ^ uint64(b.oqty[id])}
					acc = s.next()
				}
			}
		}
	}
	fold(b.bids)
	fold(b.asks)
	return b.fills, b.volume, b.live, acc
}

// engine dispatch

type engine interface {
	limit(side byte, price, qty int64, id uint64)
	cancel(id uint64)
	stats() (fills, volume uint64, resting int, checksum uint64)
}
