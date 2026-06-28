package command

import (
	"sync/atomic"
	_ "unsafe" // for go:linkname
)

// stripedUint64 is a uint64 counter striped across one cell per logical CPU so
// concurrent writers do not contend on a single cache line. Every command that
// runs bumps its cmdStat.calls counter, and on the integrated fast path that bump
// is the only shared write left (statCallFast dropped the usec and histogram
// writes). Under a saturating GET or SET load every core hits the same calls word
// for that one command, so a plain atomic.Uint64 serialises every caller on one
// line, which shows up as throughput going flat or negative past two cores. The
// keyspace used_memory counter hit the same wall and the same striping fixed it
// (see keyspace/striped.go); this is that primitive for the command package's
// uint64 call counters.
//
// numCallStripes is a power of two at or above any realistic GOMAXPROCS, so the P
// index maps to its own cell without collision on common core counts; above that,
// cells alias, which only reintroduces a little contention rather than changing
// the sum.
const numCallStripes = 64

// callCell pads each cell out to a 64-byte cache line so two stripes never share
// one line and an add on one core cannot invalidate another core's cell.
type callCell struct {
	n atomic.Uint64
	_ [56]byte
}

type stripedUint64 struct {
	cells [numCallStripes]callCell
}

// Add folds delta into the counter on the current P's cell. procPin returns the P
// index and disables preemption for the single atomic add, so the chosen cell does
// not change mid-add; procUnpin restores it. This is the same P-pinning sync.Pool
// uses to keep its per-P caches contention free.
func (c *stripedUint64) Add(delta uint64) {
	pid := callProcPin()
	c.cells[pid&(numCallStripes-1)].n.Add(delta)
	callProcUnpin()
}

// Load sums every cell for the exact total. It is called off the hot path, by INFO
// commandstats, LATENCY HISTOGRAM, and the metrics endpoint, so summing the cells
// there is not on the per-op path the way a single shared add would be.
func (c *stripedUint64) Load() uint64 {
	var total uint64
	for i := range c.cells {
		total += c.cells[i].n.Load()
	}
	return total
}

// Store sets the counter to v by zeroing every cell and parking the whole value in
// cell zero. CONFIG RESETSTAT calls it with v == 0 to clear the counter in place;
// the only caller passes zero, so concentrating a non-zero v in one cell is fine
// and keeps Load exact either way.
func (c *stripedUint64) Store(v uint64) {
	for i := range c.cells {
		c.cells[i].n.Store(0)
	}
	c.cells[0].n.Store(v)
}

//go:linkname callProcPin runtime.procPin
func callProcPin() int

//go:linkname callProcUnpin runtime.procUnpin
func callProcUnpin()
