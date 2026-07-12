package list

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The list type keeps its per-key structures in an owner-local registry: one map
// from key to the inline list, touched only by the shard goroutine, so it holds
// no lock. The set type hangs its registry off Ctx.Coll and the zset off
// Ctx.ZColl; the list has no dedicated Ctx slot yet and shard is owned by
// another slice, so the registry hangs off a map keyed by the shard's store
// pointer, which is stable for the worker's life and unique per owner. Each
// entry is reached and mutated only by its owning shard goroutine; the sync.Map
// guards nothing but the first-touch creation race between shards. The
// keyspace-unification slice folds this into the shared collection holder Ctx
// grows, at which point this map goes away.
type reg struct {
	m       map[string]*list
	waiters map[string]*waitList
	wpool   waitPool
	// ready is the serve-chain worklist: keys a served BLMOVE pushed onto whose
	// own blocked waiters may now be servable. It stays nil until the first move
	// serves (a plain push serving BLPOP/BLMPOP waiters never allocates it) and is
	// truncated back to empty at the end of every serveWaiters call, so its grown
	// capacity is reused across chains without holding keys between drains.
	ready []string
}

var regs sync.Map // *store.Store -> *reg

// registry returns the shard's list registry, building it on first use. The
// store pointer is set once when the worker starts and never changes, so it is a
// stable per-shard key.
func registry(cx *shard.Ctx) *reg {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg)
	}
	v, _ := regs.LoadOrStore(cx.St, &reg{
		m:       make(map[string]*list),
		waiters: make(map[string]*waitList),
	})
	return v.(*reg)
}

// lookup finds the list for key. wrong is true when the key instead holds a
// value in the string store, which every list command answers with WRONGTYPE.
// Cross-type collisions with the set and zset registries are not resolved in
// this slice, the same deferral the set and zset slices carry until keyspace
// unification threads every type through one holder.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (l *list, wrong bool) {
	if l = g.m[string(key)]; l != nil {
		return l, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// drop removes an emptied list from the registry: Redis deletes a list the
// moment its last element leaves.
func (g *reg) drop(key []byte) { delete(g.m, string(key)) }

// waitListFor returns the waiter FIFO for key, creating an empty one on first
// block. It lazily initializes the map so a registry built directly in a unit
// test (with a nil waiters map) can still park; the real registry() path
// pre-builds it.
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

// dropWaitersIfEmpty removes a waiter list from the registry once its last
// waiter leaves, mirroring drop for the value map so a key that was blocked on
// and then drained leaves nothing behind.
func (g *reg) dropWaitersIfEmpty(wl *waitList) {
	if wl.n == 0 {
		delete(g.waiters, wl.key)
	}
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
