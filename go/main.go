// CLOB matching engine on the LMAX pattern in Go, benchmarked.
//
// Architecture: hand-rolled single-producer/single-consumer busy-spin ring
// buffer (Go has no maintained LMAX Disruptor port; the pattern is ~60 lines)
// feeding a single-writer matching core that owns all book state. The exact
// same workload, book algorithm, and measurement protocol are implemented in
// the Java (../java) and Rust (../rust) versions — the point of this
// repository is the comparison. All three print the same deterministic book
// checksum on the same workload.
//
// Modes: throughput <ops> | latency <ops> <rate> | all
package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/btree"
)

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

func (l *level) empty() bool     { return l.head == len(l.ids) }
func (l *level) front() uint64   { return l.ids[l.head] }
func (l *level) popFront()       { l.head++ }
func (l *level) push(id uint64)  { l.ids = append(l.ids, id) }

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

// ---------------------------------------------------------------- SPSC ring
// The LMAX pattern in miniature: a power-of-two ring of pre-allocated events,
// a producer cursor and a consumer cursor, busy-spin waits on both sides.

type event struct {
	kind, side byte
	price, qty int64
	id         uint64
	ts         uint64 // scheduled-publish nanos since run epoch; 0 = don't measure
}

const ringSize = 65_536

type ring struct {
	buf  []event
	mask uint64
	_    [56]byte // keep the cursors on separate cache lines
	prod atomic.Uint64
	_    [56]byte
	cons atomic.Uint64
}

func newRing() *ring {
	return &ring{buf: make([]event, ringSize), mask: ringSize - 1}
}

// claim returns the slot for the next publish, spinning while the ring is full.
func (r *ring) claim() *event {
	next := r.prod.Load() + 1
	for next-r.cons.Load() > ringSize {
		runtime.Gosched()
	}
	return &r.buf[next&r.mask]
}

func (r *ring) publish() {
	r.prod.Store(r.prod.Load() + 1)
}

// ---------------------------------------------------------------- harness

type report struct {
	fills    uint64
	volume   uint64
	resting  int
	checksum uint64
	hist     *hdr.Histogram
}

type runResult struct {
	rep      report
	wall     float64
	gcCount  uint32
	gcPause  time.Duration
	mallocs  uint64
}

func run(totalOps, warmupOps, rate uint64) runResult {
	epoch := time.Now()
	rb := newRing()
	done := make(chan report, 1)

	go func() {
		runtime.LockOSThread()
		b := newBook()
		hist := hdr.New(1, 60_000_000_000, 3)
		seq := uint64(0)
		for {
			for rb.prod.Load() == seq {
				// busy-spin
			}
			seq++
			e := &rb.buf[seq&rb.mask]
			switch e.kind {
			case kindLimit:
				b.limit(e.side, e.price, e.qty, e.id)
			case kindCancel:
				b.cancel(e.id)
			default:
				done <- report{b.fills, b.volume, len(b.orders), b.checksum(), hist}
				return
			}
			if e.ts != 0 {
				lat := uint64(time.Since(epoch).Nanoseconds()) - e.ts
				_ = hist.RecordValue(max(int64(lat), 1))
			}
			rb.cons.Store(seq)
		}
	}()

	runtime.LockOSThread()
	wl := newGen(42)
	var interval uint64
	if rate > 0 {
		interval = 1_000_000_000 / rate
	}
	nextDue := uint64(time.Since(epoch).Nanoseconds())

	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	t0 := time.Now()
	for i := uint64(0); i < totalOps; i++ {
		wl.nextOp()
		var ts uint64
		if interval > 0 {
			nextDue += interval
			for uint64(time.Since(epoch).Nanoseconds()) < nextDue {
				// busy-spin pacing
			}
			if i >= warmupOps {
				ts = nextDue
			}
		}
		e := rb.claim()
		e.kind, e.side = wl.kind, wl.side
		e.price, e.qty, e.id = wl.price, wl.qty, wl.id
		e.ts = ts
		rb.publish()
	}
	e := rb.claim()
	e.kind, e.ts = kindReport, 0
	rb.publish()
	rep := <-done
	wall := time.Since(t0).Seconds()
	runtime.ReadMemStats(&ms1)

	return runResult{
		rep:     rep,
		wall:    wall,
		gcCount: ms1.NumGC - ms0.NumGC,
		gcPause: time.Duration(ms1.PauseTotalNs - ms0.PauseTotalNs),
		mallocs: ms1.Mallocs - ms0.Mallocs,
	}
}

func printResult(r runResult, totalOps uint64, latency bool) {
	fmt.Printf("  gc: %d cycles, %.1f ms total stop-the-world pause; heap allocations: %d\n",
		r.gcCount, float64(r.gcPause.Nanoseconds())/1e6, r.mallocs)
	fmt.Printf("  ops=%d wall=%.3fs throughput=%.0f ops/s\n",
		totalOps, r.wall, float64(totalOps)/r.wall)
	fmt.Printf("  fills=%d volume=%d resting=%d checksum=%016x\n",
		r.rep.fills, r.rep.volume, r.rep.resting, r.rep.checksum)
	if latency {
		h := r.rep.hist
		us := func(q float64) float64 { return float64(h.ValueAtQuantile(q)) / 1000.0 }
		fmt.Printf("  latency µs: p50=%.3f p90=%.3f p99=%.3f p99.9=%.1f p99.99=%.1f max=%.1f (n=%d)\n",
			us(50), us(90), us(99), us(99.9), us(99.99),
			float64(h.Max())/1000.0, h.TotalCount())
	}
}

func main() {
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	argU := func(i int, d uint64) uint64 {
		if len(os.Args) > i {
			if v, err := strconv.ParseUint(os.Args[i], 10, 64); err == nil {
				return v
			}
		}
		return d
	}

	if mode == "throughput" || mode == "all" {
		ops := uint64(10_000_000)
		if mode == "throughput" {
			ops = argU(2, ops)
		}
		fmt.Println("[go] throughput mode")
		printResult(run(ops, ops, 0), ops, false)
	}
	if mode == "latency" || mode == "all" {
		ops, rate := uint64(2_000_000), uint64(250_000)
		if mode == "latency" {
			ops, rate = argU(2, ops), argU(3, rate)
		}
		fmt.Printf("[go] latency mode (%d ops/s paced)\n", rate)
		printResult(run(ops, ops/5, rate), ops, true)
	}
}
