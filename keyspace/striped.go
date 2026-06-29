package keyspace

import (
	"sync/atomic"
	_ "unsafe" // for go:linkname
)

// stripedInt64 is an int64 counter striped across one cell per logical CPU so
// concurrent writers do not contend on a single cache line. Every mutation in the
// keyspace adds to used_memory through dataBytes, so under a write-heavy load on
// many cores a plain atomic.Int64 serialises every writer on one line: a 10-core
// SET benchmark spent about 92 ns/op of its 172 ns/op on those two atomic adds
// alone. Striping by the running P spreads the adds across cells, so writers on
// different cores touch different lines and the contention goes away. The exact
// value is unaffected, since Load sums every cell.
//
// numStripes is a power of two at or above any realistic GOMAXPROCS, so the P index
// maps to its own cell without collision on common core counts; above that, cells
// alias, which only reintroduces a little contention rather than changing the sum.
const numStripes = 64

// cacheLine pads each cell out to a 64-byte cache line so two stripes never share
// one line and an add on one core cannot invalidate another core's cell.
type cacheLine struct {
	n atomic.Int64
	_ [56]byte
}

type stripedInt64 struct {
	cells [numStripes]cacheLine
}

// Add folds delta into the counter on the current P's cell. procPin returns the P
// index and disables preemption for the single atomic add, so the chosen cell does
// not change mid-add; procUnpin restores it. This is the same P-pinning sync.Pool
// uses to keep its per-P caches contention free.
func (c *stripedInt64) Add(delta int64) {
	pid := runtimeProcPin()
	c.cells[pid&(numStripes-1)].n.Add(delta)
	runtimeProcUnpin()
}

// Load sums every cell for the exact total. It is called off the write path, by
// UsedMemory for INFO and by the maxmemory eviction loop, so summing numStripes
// cells there is not on the per-op hot path the way a single shared add would be.
func (c *stripedInt64) Load() int64 {
	var total int64
	for i := range c.cells {
		total += c.cells[i].n.Load()
	}
	return total
}

//go:linkname runtimeProcPin runtime.procPin
func runtimeProcPin() int

//go:linkname runtimeProcUnpin runtime.procUnpin
func runtimeProcUnpin()
