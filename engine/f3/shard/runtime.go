package shard

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/store"
)

// Runtime is the shard topology: S workers, each a single goroutine owning
// one store (and optionally locked to an OS thread), fixed at startup. Shards never split, merge, or rebalance
// at runtime; resizing S means restarting the process (doc 03 section 2.2).
type Runtime struct {
	workers []*worker
	started bool

	// txnTicket is the process-global tier-two ticket source (doc 03 section
	// 6.1, intent.go): one atomic touched only by Begin, off the single-key path
	// entirely. The total order it hands out is what makes the intent schedule
	// deadlock-free.
	txnTicket atomic.Uint64

	// netInfo, when set, appends the transport's "# Net" lines to the INFO
	// stats text (doc 08 section 9.5). The server layer owns the transport
	// counters and registers the renderer through SetNetInfo before Start;
	// connection writer goroutines read the field with plain loads during an
	// INFO gather, which the fixed-before-Start rule makes safe.
	netInfo func([]byte) []byte

	// live counts connections currently being served across every driver. The
	// connection writers read it in idleOnce to decide whether to spin before
	// they park (see connSpinHighWater): past the high-water the box is
	// saturated, so a writer parks at once and leaves its core to the shard
	// workers. A driver bumps it as it admits and drops a connection (through
	// ConnOpened and ConnClosed, which its register and unregister already call
	// under their registry lock); NewConn does not touch it, so a test that
	// builds a bare Conn never perturbs the spin decision.
	live atomic.Int64
}

// ConnOpened records that a driver has begun serving a connection, and
// ConnClosed pairs with it when the connection is torn down. They maintain the
// live count that drives the connSpinHighWater park-immediately switch; every
// driver routes through register and unregister, so calling these there keeps
// the count correct across the goroutine, reactor, and uring transports.
func (r *Runtime) ConnOpened() { r.live.Add(1) }

// ConnClosed records that a served connection has been torn down; see
// ConnOpened.
func (r *Runtime) ConnClosed() { r.live.Add(-1) }

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
		r.workers[i].rt = r
	}
	return r
}

// Config is the runtime topology plus the larger-than-memory knobs of doc 09
// section 8: a value-log directory and a resident byte budget, both per
// shard. Sharding is fixed at startup like everything else here.
type Config struct {
	Shards     int
	ArenaBytes int
	SegBytes   int

	// VlogDir, when set, gives every shard its own value log under this
	// directory (one file per shard, fresh-start semantics). Without it the
	// stores are memory-only and ResidentCapBytes is ignored.
	VlogDir string

	// ResidentCapBytes is each shard's resident byte budget; past it a
	// separated or chunked value's bytes spill to the shard's log. 0 means
	// uncapped.
	ResidentCapBytes uint64

	// PinWorkers locks each worker goroutine to an OS thread. Off by default:
	// the single-owner invariant is goroutine affinity and needs no thread,
	// and the labs/f3/m0/11_transport sweep measured the lock as a net loss
	// through the locked-M park/unpark handoff. The knob stays for boxes
	// where thread residency measurably pays.
	PinWorkers bool
}

// Open is New with the value-log configuration: each shard gets its own log
// file so the single-owner contract extends to the disk tier.
func Open(cfg Config) (*Runtime, error) {
	if cfg.Shards < 1 {
		cfg.Shards = 1
	}
	r := &Runtime{workers: make([]*worker, cfg.Shards)}
	for i := range r.workers {
		o := store.Options{ArenaBytes: cfg.ArenaBytes, SegBytes: cfg.SegBytes}
		if cfg.VlogDir != "" {
			o.VlogPath = filepath.Join(cfg.VlogDir, fmt.Sprintf("shard-%03d.vlog", i))
			o.ResidentCapBytes = cfg.ResidentCapBytes
		}
		st, err := store.Open(o)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = r.workers[j].st.Close()
			}
			return nil, err
		}
		r.workers[i] = newWorker(i, st)
		r.workers[i].rt = r
		r.workers[i].pin = cfg.PinWorkers
	}
	return r, nil
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
	// The workers are gone; releasing the value logs here is single-owner by
	// exhaustion.
	for _, w := range r.workers {
		_ = w.st.Close()
	}
}
