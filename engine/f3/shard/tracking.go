package shard

import "sync/atomic"

// Client-side caching (spec 2064/f3/17, the M11 command-closure milestone;
// redis's CLIENT TRACKING). A tracking client caches values locally and asks the
// server to push an "invalidate" message the moment a key it cached is written by
// anyone. The server-side machinery is a per-connection recorded-key table and a
// write-path invalidation, both network-layer state the shard cannot import, so
// they live in the drivers layer (f3srv/drivers/tracking.go). What lives here is
// the one hot-path seam the owner needs: the arm gate and the invalidation hook.
//
// The seam mirrors keyspace notifications exactly (notify.go). The arm gate is a
// process atomic the drivers layer sets from the count of tracking-on connections,
// read on the owner before the hook, so a server with no tracking client pays one
// relaxed load per write and never the hook. The invalidation itself goes through
// a per-worker hook (Runtime.UseInvalidator) the server layer wires to its tracking
// registry, because the registry is per-Server state; this is the shape UseEvictor,
// UsePublisher, and UseDemoter all share. The call site is publishKeyspaceEvent,
// the universal modified-key path after the per-type events arc: every mutation
// that emits a keyspace event also drives an invalidation, so CSC coverage tracks
// keyspace-notification coverage and both widen together. The one difference is the
// gate: CSC fires regardless of the notify-keyspace-events config, so its check
// sits ahead of the notify mask, on trackingArmed alone.

// trackingArmed is the count of connections that currently have CLIENT TRACKING
// on, published by the drivers layer through SetTrackingArmed. Zero by default, so
// a server with no tracking client returns from the CSC gate after one atomic load
// and never calls the invalidation hook. Process-global like notifyFlags, since a
// write on any shard may invalidate a key any connection cached.
var trackingArmed atomic.Int64

// SetTrackingArmed publishes the live count of tracking-on connections, the value
// the drivers layer's tracking registry recomputes under its own mutex whenever a
// connection turns tracking on or off or disconnects. The owner reads it on the
// write path through trackingArmed.Load; a nonzero count means at least one client
// is caching, so the invalidation hook is worth calling.
func SetTrackingArmed(n int64) { trackingArmed.Store(n) }

// TrackingArmed reports whether any connection currently has tracking on, for a
// caller that wants to gate a batch of work on one load rather than per item.
func TrackingArmed() bool { return trackingArmed.Load() != 0 }

// UseInvalidator registers the client-side-caching invalidation hook the
// keyspace-event emitter calls on every write. The server layer passes a closure
// over its tracking registry (s.tracking.invalidate), which the shard cannot
// import; a runtime with no CSC leaves the hook nil and the emitter skips it. Fixed
// before Start like UsePublisher, so the owner reads it with no synchronization.
// The hook delivers invalidation pushes through the connection out-of-band path,
// which is safe from the owner goroutine, the same guarantee the publisher has.
func (r *Runtime) UseInvalidator(fn func(key []byte, origin *Conn)) {
	if r.started {
		panic("shard: UseInvalidator after Start")
	}
	for _, w := range r.workers {
		w.invalidator = fn
	}
}
