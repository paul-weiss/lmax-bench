package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// ---------------------------------------------------------------- harness

type report struct {
	fills    uint64
	volume   uint64
	resting  int
	checksum uint64
	hist     *hdr.Histogram
}

type runResult struct {
	rep     report
	wall    float64
	gcCount uint32
	gcPause time.Duration
	mallocs uint64
}

func run(totalOps, warmupOps, rate uint64, quiet bool) runResult {
	epoch := time.Now()
	rb := newRing()
	done := make(chan report, 1)

	tuned := os.Getenv("ENGINE") == "tuned"
	// GOGC is honored natively by the runtime; GOGC=off disables collection
	// entirely, which the tuned engine makes safe: with ~10k tiny allocations
	// per run in steady state the heap never grows. The only cycles a default
	// run sees are the pacer reacting to the book's construction. Printed so
	// every result line is self-documenting about the collector it ran under.
	gogc := os.Getenv("GOGC")
	if gogc == "" {
		gogc = "default"
	}
	if !quiet {
		fmt.Printf("  engine=%s gogc=%s\n",
			map[bool]string{true: "tuned", false: "idiomatic"}[tuned], gogc)
	}
	go func() {
		runtime.LockOSThread()
		var b engine
		if tuned {
			b = newTunedBook(totalOps)
		} else {
			b = newBook()
		}
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
				f, v, r, c := b.stats()
				done <- report{f, v, r, c, hist}
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
