package zset

import (
	"math/bits"
	"math/rand/v2"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The zset type keeps its per-key structures in an owner-local registry hung off
// the shard's Ctx.ZColl (spec 2064/f3/12): one map from key to the inline zset,
// touched only by the shard goroutine, so it holds no lock. The string store and
// this registry are separate keyspaces for now; the WRONGTYPE guard keeps a zset
// command off a key the string store owns. Cross-type unification with the set
// registry (a zset and a set colliding on one key, TYPE reporting zset) lands
// with the keyspace slice; this slice keeps the zset surface self-consistent and
// refuses the string-store collision it can resolve.

const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

// reg is the shard's zset registry plus its draw state. The PRNG is owner-local
// (spec 2064/f3/12 section 6.8, mirroring the set draw of doc 11 section 5.6):
// ZRANDMEMBER draws from a per-shard PCG that is never shared and never locked,
// so the draw path takes no atomic and touches no global rand state. The two
// scratch slices back the count-form distinct draws and are reused across
// commands, so a steady-state count draw allocates nothing.
type reg struct {
	m   map[string]*zset
	rng rand.PCG

	// idxScratch is the full index permutation the large-sample distinct draw
	// (ZRANDMEMBER positive count above the crossover) partial-shuffles in place.
	idxScratch []uint32
	// pickScratch holds the indexes already chosen by the small-sample distinct
	// draw so its rejection loop can skip repeats without a map.
	pickScratch []int
}

// zsetSeed hands each shard's registry a distinct PCG stream. The counter is
// touched once per registry, at first use, never on the draw path, so the
// owner-local "never locked" contract holds where it matters.
var zsetSeed atomic.Uint64

func freshPCG() rand.PCG {
	n := zsetSeed.Add(1)
	return *rand.NewPCG(n*0x9e3779b97f4a7c15+0x243f6a8885a308d3, n*0xbf58476d1ce4e5b9+0x13198a2e03707344)
}

// registry returns the shard's zset registry, building it on first use. The Ctx
// and thus the registry live for the worker's whole life, so a zset added on one
// command is there for the next.
func registry(cx *shard.Ctx) *reg {
	if cx.ZColl == nil {
		cx.ZColl = &reg{m: make(map[string]*zset), rng: freshPCG()}
	}
	return cx.ZColl.(*reg)
}

// next returns a uniform integer in [0, n) with no modulo bias (F15 exact
// uniformity): Lemire's multiply-shift with rejection, the same unbiased bound
// math/rand/v2 uses, over the owner-local PCG. n is always positive at the call
// sites, since a draw only runs on a non-empty zset.
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

// lookup finds the zset for key. present is false when no zset exists; wrong is
// true when the key instead holds a value in the string store, which every zset
// command answers with WRONGTYPE.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (z *zset, wrong bool) {
	if z = g.m[string(key)]; z != nil {
		return z, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// drop removes an emptied zset: Redis deletes a zset the moment its last member
// leaves.
func (g *reg) drop(key []byte) { delete(g.m, string(key)) }
