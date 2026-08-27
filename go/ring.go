package main

import (
	"runtime"
	"sync/atomic"
)

// ---------------------------------------------------------------- SPSC ring
// The LMAX pattern in miniature: a power-of-two ring of pre-allocated events,
// a producer cursor and a consumer cursor, busy-spin waits on both sides.

type event struct {
	kind, side byte
	price, qty int64
	id         uint64
	ts         uint64 // scheduled-publish nanos since run epoch; 0 = don't measure
}

// Power of two, required: slot lookup is then `seq & mask` — a single AND
// computes seq mod ringSize, instead of an integer division per access.
const ringSize = 65_536

type ring struct {
	buf  []event
	mask uint64 // ringSize - 1: all ones, so seq & mask == seq mod ringSize
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
