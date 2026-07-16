package shard

import (
	"sync"

	"github.com/tamnd/aki/engine/obs1/store"
)

// The owner between-batches maintenance seam (doc 03 execution model). A type
// package accumulates owner-local background work that must not run on the command
// hot path, the stream gc rewrite of a partially-tombstoned block being the first
// (spec 2064/f3/14 section 6.5): XDEL only flips a tombstone flag, and the deferred
// rewrite that reclaims the dead bytes runs here, at the shard's idle boundary, where
// the queue is drained and no streamed reply is in flight, so no arena snapshot can
// name the bytes a rewrite moves.
//
// The dependency runs one way, the type packages import shard, so shard cannot call
// into them. Instead a package registers a maintainer keyed by the store it owns
// state for, and the worker invokes its store's maintainer at idle. Registration is
// once per store (the type registers when it first creates owner-local state for the
// store), and the maintainer runs only on that store's owner goroutine, so it needs
// no lock of its own; the sync.Map guards only the cross-shard first-touch.
var maintainers sync.Map // *store.Store -> func()

// RegisterMaintainer records the owner-goroutine background step for the shard that
// owns st. The worker runs it at every idle boundary; the step itself must be cheap
// when it has nothing to do (one length check), since idle boundaries are frequent.
// A second registration for the same store replaces the first, which a type that
// registers exactly once never triggers.
func RegisterMaintainer(st *store.Store, fn func()) { maintainers.Store(st, fn) }

// runMaintainer invokes this shard's registered maintainer, if any. It is called
// from the worker's idle branch, on the owner goroutine, with the queue drained and
// no streamed reply pumping.
func (w *worker) runMaintainer() {
	if fn, ok := maintainers.Load(w.st); ok {
		fn.(func())()
	}
}
