package hash

import (
	"math/bits"
	"math/rand/v2"
	"sync"
	"sync/atomic"

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
	m   map[string]*hash
	rng rand.PCG

	// idxScratch is the full index permutation the large-sample distinct draw
	// (HRANDFIELD positive count above the crossover) partial-shuffles in place.
	idxScratch []uint32
	// pickScratch holds the indexes already chosen by the small-sample distinct
	// draw so its rejection loop can skip repeats without a map.
	pickScratch []int
}

var regs sync.Map // *store.Store -> *reg

// hashSeed hands each shard's registry a distinct PCG stream, the same owner-local
// draw discipline set.reg keeps: the counter is touched once per registry at first
// use, never on the draw path, so HRANDFIELD takes no atomic and no lock. The
// stream is deterministic given creation order, which keeps a shard's draws
// reproducible for a replay without a shared global generator.
var hashSeed atomic.Uint64

func freshPCG() rand.PCG {
	n := hashSeed.Add(1)
	return *rand.NewPCG(n*0x9e3779b97f4a7c15+0x243f6a8885a308d3, n*0xbf58476d1ce4e5b9+0x13198a2e03707344)
}

// next returns a uniform integer in [0, n) with no modulo bias (F15 exact
// uniformity): Lemire's multiply-shift with rejection over the owner-local PCG,
// the same unbiased bound set.reg.next uses. n is always positive at the call
// sites, since a draw only runs on a non-empty hash.
func (g *reg) next(n int) int {
	un := uint64(n)
	hi, lo := bits.Mul64(g.rng.Uint64(), un)
	if lo < un {
		thresh := -un % un
		for lo < thresh {
			hi, lo = bits.Mul64(g.rng.Uint64(), un)
		}
	}
	return int(hi)
}

// identityIndex returns a reused []uint32 of 0..n-1, the permutation the
// large-sample distinct draw shuffles in place.
func (g *reg) identityIndex(n int) []uint32 {
	if cap(g.idxScratch) < n {
		g.idxScratch = make([]uint32, n)
	}
	idx := g.idxScratch[:n]
	for i := range idx {
		idx[i] = uint32(i)
	}
	return idx
}

// registry returns the shard's hash registry, building it on first use. The store
// pointer is set once when the worker starts and never changes, so it is a stable
// per-shard key.
func registry(cx *shard.Ctx) *reg {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg)
	}
	v, _ := regs.LoadOrStore(cx.St, &reg{m: make(map[string]*hash), rng: freshPCG()})
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
