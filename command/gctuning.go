package command

import (
	"math"
	"runtime/debug"
)

// This file implements the Go GC knobs from doc 21 section 10.3, exposed as the
// config directives go-gogc and go-memlimit. They let an operator tune the Go
// garbage collector at startup or live through CONFIG SET without restarting with
// a different GOGC or GOMEMLIMIT environment variable.

// ApplyGCTuning applies go-gogc and go-memlimit to the runtime. The server command
// calls it once at startup, and CONFIG SET calls it again whenever either value
// changes, so a change takes effect at once.
func (d *Dispatcher) ApplyGCTuning() {
	d.applyGOGC()
	d.applyMemLimit()
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
