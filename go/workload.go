package main

// ---------------------------------------------------------------- workload
// splitmix64, bit-identical to the Java and Rust generators.

type splitMix64 struct{ state uint64 }

func (s *splitMix64) next() uint64 {
	s.state += 0x9E3779B97F4A7C15
	z := s.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

const (
	kindLimit  = 0
	kindCancel = 1
	kindReport = 2
	sideBid    = 0
)

type gen struct {
	rng    splitMix64
	mid    int64
	lastID uint64

	// out-params of nextOp (avoids allocating per call)
	kind, side byte
	price, qty int64
	id         uint64
}

func newGen(seed uint64) *gen {
	return &gen{rng: splitMix64{seed}, mid: 1000}
}

func (g *gen) nextOp() {
	r := g.rng.next()
	if r%7 == 0 {
		g.mid += int64((r>>40)%3) - 1
		g.mid = min(max(g.mid, 500), 1500)
	}
	c := r % 100
	if c < 85 {
		g.side = byte((r >> 8) & 1)
		aggressive := c >= 55
		var offset int64
		if aggressive {
			offset = int64((r >> 16) % 10)
		} else {
			offset = 1 + int64((r>>16)%20)
		}
		if (g.side == sideBid) != aggressive {
			g.price = g.mid - offset
		} else {
			g.price = g.mid + offset
		}
		g.qty = 1 + int64((r>>32)%100)
		g.lastID++
		g.id = g.lastID
		g.kind = kindLimit
	} else {
		back := (r >> 16) % 5000
		if g.lastID > back {
			g.id = g.lastID - back
		} else {
			g.id = 0 // saturating
		}
		g.kind = kindCancel
		g.side, g.price, g.qty = 0, 0, 0
	}
}
