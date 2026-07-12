package hash

import (
	"sync"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The hash type keeps its per-key structures in an owner-local registry: one map
// from key to the inline hash, touched only by the shard goroutine, so it holds
// no lock. The set type hangs its registry off Ctx.Coll and the zset off
// Ctx.ZColl; the list and now the hash have no dedicated Ctx slot yet, so the
// registry hangs off a map keyed by the shard's store pointer, which is stable
// for the worker's life and unique per owner (the same seam list/reg.go uses).
// Each entry is reached and mutated only by its owning shard goroutine; the
// sync.Map guards nothing but the first-touch creation race between shards. The
// keyspace-unification slice folds this into the shared collection holder Ctx
// grows, at which point this map goes away.
type reg struct {
	m map[string]*hash
}

var regs sync.Map // *store.Store -> *reg

// registry returns the shard's hash registry, building it on first use. The store
// pointer is set once when the worker starts and never changes, so it is a stable
// per-shard key.
func registry(cx *shard.Ctx) *reg {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg)
	}
	v, _ := regs.LoadOrStore(cx.St, &reg{m: make(map[string]*hash)})
	return v.(*reg)
}

// lookup finds the hash for key. present is nil when no hash exists; wrong is true
// when the key instead holds a value in the string store, which every hash command
// answers with WRONGTYPE. Cross-type collisions with the set, zset, and list
// registries are not resolved in this slice, the same deferral those slices carry
// until keyspace unification threads every type through one holder.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (h *hash, wrong bool) {
	if h = g.m[string(key)]; h != nil {
		return h, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// drop removes an emptied hash from the registry: Redis deletes a hash the moment
// its last field leaves.
func (g *reg) drop(key []byte) { delete(g.m, string(key)) }

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
