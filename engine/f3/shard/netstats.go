package shard

import "sync/atomic"

// eventCounter is a single-writer event counter: exactly one goroutine bumps
// it, any goroutine may read it. The bump is a load and a store on the atomic
// type, never an atomic add: the owner is the only writer, so the
// read-modify-write needs no lock prefix, and the atomic type exists only so
// an aggregating reader (INFO, a stats snapshot) sees a defined value under
// the race detector. That keeps the F7 discipline of doc 08 section 9.5: the
// counters are per-owner memory, aggregated on read, nothing shared-atomic on
// the hot path. Owners bump once per syscall-scale event (a wake token, a
// park, a socket read), or fold a pass's count into one add, never one atomic
// per command.
type eventCounter struct{ v atomic.Uint64 }

func (c *eventCounter) add(n uint64) { c.v.Store(c.v.Load() + n) }

func (c *eventCounter) bump() { c.add(1) }

func (c *eventCounter) load() uint64 { return c.v.Load() }

// NetWakes reports this connection's waker traffic: worker wake tokens the
// reader side actually sent (a token is sent only when the wake CAS claims a
// parked worker, so each one is a real cross-goroutine wakeup) and the writer
// goroutine's parks (each one a real block on the park channel, not a spin
// turn). Safe from any goroutine; the counts are the transport's evidence
// surface for the doc 08 section 9.5 akinet counters.
func (c *Conn) NetWakes() (workerWakes, writerParks uint64) {
	return c.wokeWorkers.load(), c.parks.load()
}

// NetWakes sums the workers' side of the waker traffic: connection writer
// wake tokens the workers sent and the workers' parks. Same counting rules as
// Conn.NetWakes: tokens actually sent, blocks actually taken.
func (r *Runtime) NetWakes() (connWakes, workerParks uint64) {
	for _, w := range r.workers {
		connWakes += w.connWakes.load()
		workerParks += w.parks.load()
	}
	return connWakes, workerParks
}

// SetNetInfo registers the transport's INFO renderer: the function receives
// the stats text after the engine sections and appends the driver's own lines
// (the doc 08 section 9.5 "# Net" section). The server layer owns the
// transport counters, so it owns the rendering; the engine only gives the
// section a slot in the INFO gather. Must run before Start: connection writer
// goroutines read the field with plain loads during an INFO gather.
func (r *Runtime) SetNetInfo(f func([]byte) []byte) {
	if r.started {
		panic("shard: SetNetInfo after Start")
	}
	r.netInfo = f
}
