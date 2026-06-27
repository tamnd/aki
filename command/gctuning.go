package command

import (
	"math"
	"runtime"
	"runtime/debug"
)

// This file implements the Go runtime knobs from doc 21 section 10.3, exposed as
// the config directives go-gogc, go-memlimit, and go-maxprocs. They let an operator
// tune the Go garbage collector and scheduler at startup or live through CONFIG SET
// without restarting with a different GOGC, GOMEMLIMIT, or GOMAXPROCS environment
// variable.

// ApplyGCTuning applies go-gogc, go-memlimit, and go-maxprocs to the runtime. The
// server command calls it once at startup, and CONFIG SET calls it again whenever
// any of the three changes, so a change takes effect at once.
func (d *Dispatcher) ApplyGCTuning() {
	d.applyGOGC()
	d.applyMemLimit()
	d.applyMaxProcs()
}

// applyGOGC sets the GC target percentage. go-gogc 100 is the runtime default and
// 0 turns the collector off, matching the GOGC=off behavior, since the runtime
// disables GC on a negative percentage.
func (d *Dispatcher) applyGOGC() {
	gogc := int(d.confInt("go-gogc", 100))
	if gogc == 0 {
		gogc = -1
	}
	debug.SetGCPercent(gogc)
}

// applyMemLimit sets the soft memory limit. go-memlimit 0 means no limit, which the
// runtime expresses as math.MaxInt64 rather than a zero-byte ceiling.
func (d *Dispatcher) applyMemLimit() {
	limit := d.confInt("go-memlimit", 0)
	if limit <= 0 {
		limit = math.MaxInt64
	}
	debug.SetMemoryLimit(limit)
}

// applyMaxProcs caps how many OS threads the Go scheduler runs Go code on at once.
// go-maxprocs 0 leaves the runtime default of one P per CPU. That default
// over-parallelizes a request-response workload: every command hops from a
// connection goroutine to its shard worker and back, and with one P per core on a
// many-core box the scheduler spends most of its time in futex wakeups parking and
// waking idle Ps around those hops rather than running command code. A positive
// value pins GOMAXPROCS, trading idle cores for far less scheduler churn. perf note
// 240 measured the gate matrix on a 32-core box: capping to 4 lifts the whole
// pipeline-1 band from about 0.78x to about 0.97x of redis, roughly a 25 percent
// throughput gain on every workload, while pipeline-16 set recovers from 0.84x to
// 0.94x. No single cap wins everywhere though, so the default stays 0.
func (d *Dispatcher) applyMaxProcs() {
	n := int(d.confInt("go-maxprocs", 0))
	if n <= 0 {
		return // leave the runtime default (one P per CPU)
	}
	runtime.GOMAXPROCS(n)
}
