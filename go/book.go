package main

import "github.com/google/btree"

// ---------------------------------------------------------------- the book
// Price-time priority; lazy cancellation (cancel removes from the id map,
// stale ids are skipped at the head of a level queue).

type resting struct {
	price int64
	qty   int64
}

// FIFO of order ids as a slice with a head index.
type level struct {
	ids  []uint64
	head int
}

func (l *level) empty() bool    { return l.head == len(l.ids) }
func (l *level) front() uint64  { return l.ids[l.head] }
func (l *level) popFront()      { l.head++ }
func (l *level) push(id uint64) { l.ids = append(l.ids, id) }

type book struct {
	bidPrices *btree.BTreeG[int64]
	askPrices *btree.BTreeG[int64]
	bidLevels map[int64]*level
	askLevels map[int64]*level
	orders    map[uint64]resting
	fills     uint64
	volume    uint64
}

func newBook() *book {
	less := func(a, b int64) bool { return a < b }
	return &book{
		bidPrices: btree.NewG(16, less),
		askPrices: btree.NewG(16, less),
		bidLevels: make(map[int64]*level),
		askLevels: make(map[int64]*level),
		orders:    make(map[uint64]resting),
	}
}

func (b *book) limit(side byte, price, qty int64, id uint64) {
	for qty > 0 {
		// best opposite level that crosses, else rest the residual
		var bestPrice int64
		var lvl *level
		if side == sideBid {
			p, ok := b.askPrices.Min()
			if !ok || p > price {
				break
			}
			bestPrice, lvl = p, b.askLevels[p]
		} else {
			p, ok := b.bidPrices.Max()
			if !ok || p < price {
				break
			}
			bestPrice, lvl = p, b.bidLevels[p]
		}
		for qty > 0 {
			if lvl.empty() {
				break
			}
			head := lvl.front()
			rec, ok := b.orders[head]
			if !ok { // lazily-cancelled order
				lvl.popFront()
				continue
			}
			m := min(qty, rec.qty)
			rec.qty -= m
			qty -= m
			b.fills++
			b.volume += uint64(m)
			if rec.qty == 0 {
				delete(b.orders, head)
				lvl.popFront()
			} else {
				b.orders[head] = rec
			}
		}
		if lvl.empty() {
			if side == sideBid {
				b.askPrices.Delete(bestPrice)
				delete(b.askLevels, bestPrice)
			} else {
				b.bidPrices.Delete(bestPrice)
				delete(b.bidLevels, bestPrice)
			}
		}
	}
	if qty > 0 {
		prices, levels := b.bidPrices, b.bidLevels
		if side != sideBid {
			prices, levels = b.askPrices, b.askLevels
		}
		lvl, ok := levels[price]
		if !ok {
			lvl = &level{}
			levels[price] = lvl
			prices.ReplaceOrInsert(price)
		}
		lvl.push(id)
		b.orders[id] = resting{price: price, qty: qty}
	}
}

func (b *book) cancel(id uint64) {
	delete(b.orders, id)
}

func (b *book) stats() (uint64, uint64, int, uint64) {
	return b.fills, b.volume, len(b.orders), b.checksum()
}

// checksum over live orders in (price ascending, FIFO) order per side —
// must be identical to the Java and Rust runs on the same workload.
func (b *book) checksum() uint64 {
	var acc uint64
	fold := func(prices *btree.BTreeG[int64], levels map[int64]*level) {
		prices.Ascend(func(p int64) bool {
			lvl := levels[p]
			for _, id := range lvl.ids[lvl.head:] {
				if r, ok := b.orders[id]; ok {
					s := splitMix64{acc ^ id ^ (uint64(r.price) << 20) ^ uint64(r.qty)}
					acc = s.next()
				}
			}
			return true
		})
	}
	fold(b.bidPrices, b.bidLevels)
	fold(b.askPrices, b.askLevels)
	return acc
}
