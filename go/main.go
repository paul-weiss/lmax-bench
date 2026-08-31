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
)

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

	// Warmup: 1M ops through a full discarded pipeline (its own ring, engine,
	// and generator) before anything is measured. Go has no JIT, but branch
	// predictors, caches, the allocator, and the scheduler all warm up — and
	// every language gets the identical treatment so none is measured cold.
	// The forced collection reclaims the warmup garbage deterministically, so
	// it cannot bill a measured run (with GOGC=off it is the only cycle the
	// process ever runs).
	fmt.Println("[go] warmup: 1,000,000 ops (discarded)")
	run(1_000_000, 1_000_000, 0, true)
	runtime.GC()

	if mode == "throughput" || mode == "all" {
		ops := uint64(10_000_000)
		if mode == "throughput" {
			ops = argU(2, ops)
		}
		fmt.Println("[go] throughput mode")
		printResult(run(ops, ops, 0, false), ops, false)
	}
	if mode == "latency" || mode == "all" {
		ops, rate := uint64(2_000_000), uint64(250_000)
		if mode == "latency" {
			ops, rate = argU(2, ops), argU(3, rate)
		}
		fmt.Printf("[go] latency mode (%d ops/s paced)\n", rate)
		printResult(run(ops, ops/5, rate, false), ops, true)
	}
}
