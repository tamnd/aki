package shard

import (
	"github.com/tamnd/aki/engine/f3/store"
)

// Runtime is the shard topology: S workers, each pinned to an OS thread and
// owning one store, fixed at startup. Shards never split, merge, or rebalance
// at runtime; resizing S means restarting the process (doc 03 section 2.2).
type Runtime struct {
	workers []*worker
	started bool
}

// New builds a runtime of shards workers, each with its own store of
// arenaBytes tiled into segments of segBytes (non-positive segBytes takes the
// store default). Nothing runs until Start.
func New(shards, arenaBytes, segBytes int) *Runtime {
	if shards < 1 {
		shards = 1
	}
	r := &Runtime{workers: make([]*worker, shards)}
	for i := range r.workers {
		r.workers[i] = newWorker(i, store.New(arenaBytes, segBytes))
	}
	return r
}

// Use registers the op-indexed handler table on every worker: the handler for
// op b sits at index b. Index 0 and OpError are reserved. Use must run before
// Start; the table is fixed for the runtime's life so the owner loop reads it
// with plain loads.
func (r *Runtime) Use(handlers []Handler) {
	if r.started {
		panic("shard: Use after Start")
	}
	for _, w := range r.workers {
		w.handlers = handlers
	}
}

// Shards reports the shard count.
func (r *Runtime) Shards() int { return len(r.workers) }

// ShardOf routes a key to its owner: wyhash mod S, the hash computed once and
// shared with the owner's index probe. The CRC16 slot table with hash-tag
// semantics (doc 03 section 2.1) replaces this route when the multi-key
// slices need slot-honest co-location; nothing below the route decision sees
// the difference.
func (r *Runtime) ShardOf(key []byte) int {
	return int(store.Hash(key) % uint64(len(r.workers)))
}

// Start launches every worker goroutine.
func (r *Runtime) Start() {
	if r.started {
		return
	}
	r.started = true
	for _, w := range r.workers {
		go w.run()
	}
}

// Stop halts every worker after it drains what its queue already holds, and
// waits for the goroutines to exit.
func (r *Runtime) Stop() {
	if !r.started {
		return
	}
	r.started = false
	for _, w := range r.workers {
		w.stop.Store(true)
	}
	for _, w := range r.workers {
		w.wk.wake()
	}
	for _, w := range r.workers {
		<-w.done
	}
}
