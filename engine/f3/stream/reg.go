package stream

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The stream type keeps its per-key objects in an owner-local registry, the same
// seam the list and hash types use: a map from key to the stream, touched only by
// the shard goroutine and hung off a sync.Map keyed by the shard's store pointer,
// which is stable for the worker's life and unique per owner. The sync.Map guards
// nothing but the first-touch creation race between shards; every entry is reached
// and mutated only by its owning shard. Streams have no dedicated Ctx slot, so
// they take this seam until the keyspace-unification slice folds every type into
// one holder.
type reg struct {
	m map[string]*stream
	// waiters holds the blocking-XREAD FIFO per key, and wpool the shared node
	// slab behind them (waiter.go). Both stay empty until the first XREAD BLOCK
	// parks, so a stream workload that never blocks carries only the map header.
	waiters map[string]*waitList
	wpool   waitPool
	// serveOrder is the reusable FIFO-snapshot scratch serveWaiters walks, so a
	// wake that unlinks nodes mid-walk keeps its place without a per-XADD alloc.
	serveOrder []uint32
}

var regs sync.Map // *store.Store -> *reg

// registry returns the shard's stream registry, building it on first use.
func registry(cx *shard.Ctx) *reg {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg)
	}
	v, _ := regs.LoadOrStore(cx.St, &reg{
		m:       make(map[string]*stream),
		waiters: make(map[string]*waitList),
	})
	return v.(*reg)
}

// lookup finds the stream for key. present is nil when no stream exists; wrong is
// true when the key instead holds a string value, which every stream command
// answers with WRONGTYPE. Cross-type collisions with the other collection
// registries are not resolved in this slice, the same deferral those slices carry
// until keyspace unification. An emptied stream (all entries XDEL'd) is kept, not
// dropped: Redis leaves an empty stream in place (invariant that XLEN can read 0).
func (g *reg) lookup(cx *shard.Ctx, key []byte) (s *stream, wrong bool) {
	if s = g.m[string(key)]; s != nil {
		return s, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// waitListFor returns the blocking-XREAD FIFO for key, creating an empty one on
// first block. It lazily initializes the map so a registry built directly in a
// unit test can still park; the real registry() path pre-builds it.
func (g *reg) waitListFor(key []byte) *waitList {
	if g.waiters == nil {
		g.waiters = make(map[string]*waitList)
	}
	wl := g.waiters[string(key)]
	if wl == nil {
		wl = &waitList{pool: &g.wpool, key: string(key), head: nilIdx, tail: nilIdx}
		g.waiters[string(key)] = wl
	}
	return wl
}

// dropWaitersIfEmpty removes a waiter list from the registry once its last waiter
// leaves, so a key blocked on and then served leaves nothing behind.
func (g *reg) dropWaitersIfEmpty(wl *waitList) {
	if wl.n == 0 {
		delete(g.waiters, wl.key)
	}
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
