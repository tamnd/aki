package set

import (
	"math/bits"
	"math/rand/v2"
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
)

// The set type keeps its per-key structures in an owner-local registry hung off
// the shard's Ctx.Coll (spec 2064/f3/11): one map from key to the inline set,
// touched only by the shard goroutine, so it holds no lock. The string store
// and this registry are separate keyspaces for now; the WRONGTYPE guard below
// keeps a set command off a key the string store owns, and single-key set.Del
// spans both. TYPE and single-key EXISTS have moved on to span every type
// (their unified handlers consult set.Has here); full cross-type unification (a
// SET overwriting a set, multi-key DEL over sets) lands with the keyspace slice;
// this slice keeps the set surface self-consistent and refuses the cross-type
// collisions it cannot yet resolve.

// reg is the shard's set registry plus its draw state. The PRNG is owner-local
// (doc 11 section 5.6): SPOP and SRANDMEMBER draw from a per-shard PCG that is
// never shared and never locked, so the draw path takes no atomic and touches no
// global rand state. The two scratch slices back the count-form draws and are
// reused across commands, so a steady-state count draw allocates nothing.
type reg struct {
	m   map[string]*set
	rng rand.PCG

	// idxScratch is the full index permutation the large-sample distinct draw
	// (SRANDMEMBER positive count above the crossover) partial-shuffles in place.
	idxScratch []uint32
	// pickScratch holds the indexes already chosen by the small-sample distinct
	// draw so its rejection loop can skip repeats without a map.
	pickScratch []int

	// resident is the running sum of every live set's resident-byte footprint
	// (set.residentBytes), the figure the shard reads to weigh a collection's RAM
	// against the store's arena under memory pressure (spec 2064/f3/06 section 6).
	// It is kept exact by note and drop across the map's inserts and removes, so
	// the shard never walks the registry to size it. Maintained only when acctOn.
	resident uint64
	// acctOn gates the accounting: it is true only when the shard's store runs the
	// cold tier (ColdConfigured). With no cold region to demote a set into the
	// figure would drive nothing, so the registry keeps none and note is one bool
	// load, holding the L9 zero-delta contract for a store with no resident cap.
	acctOn bool
}

// setSeed hands each shard's registry a distinct PCG stream. The counter is
// touched once per registry, at first use, never on the draw path, so the "never
// locked" contract of doc 11 section 5.6 holds where it matters. The stream is
// deterministic given creation order, which keeps a shard's draws reproducible
// for a replay without a shared global generator.
var setSeed atomic.Uint64

func freshPCG() rand.PCG {
	n := setSeed.Add(1)
	return *rand.NewPCG(n*0x9e3779b97f4a7c15+0x243f6a8885a308d3, n*0xbf58476d1ce4e5b9+0x13198a2e03707344)
}

// registry returns the shard's set registry, building it on first use. The
// Ctx and thus the registry live for the worker's whole life, so a set added
// on one command is there for the next.
func registry(cx *shard.Ctx) *reg {
	if cx.Coll == nil {
		cx.Coll = &reg{
			m:      make(map[string]*set),
			rng:    freshPCG(),
			acctOn: cx.St != nil && cx.St.ColdConfigured(),
		}
	}
	return cx.Coll.(*reg)
}

// next returns a uniform integer in [0, n) with no modulo bias (F15 exact
// uniformity): Lemire's multiply-shift with rejection, the same unbiased bound
// math/rand/v2 uses, over the owner-local PCG. n is always positive at the call
// sites, since a draw only runs on a non-empty set.
func (g *reg) next(n int) int {
	un := uint64(n)
	hi, lo := bits.Mul64(g.rng.Uint64(), un)
	if lo < un {
		// Only the low tail of the 2^64 space can bias the result; reject it.
		thresh := -un % un
		for lo < thresh {
			hi, lo = bits.Mul64(g.rng.Uint64(), un)
		}
	}
	return int(hi)
}

// Has reports whether key holds a set on this shard, without building the
// registry when none exists yet. It is the presence probe the unified TYPE
// consults across the collection types: a string value or another collection at
// key is not a set, so Has reads false for those and leaves the type to the
// caller's other probes.
func Has(cx *shard.Ctx, key []byte) bool {
	if cx.Coll == nil {
		return false
	}
	s, _ := cx.Coll.(*reg).lookup(cx, key)
	return s != nil
}

// lookup finds the set for key. present is false when no set exists; wrong is
// true when the key instead holds a value in the string store, which every set
// command answers with WRONGTYPE.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (s *set, wrong bool) {
	if s = g.m[string(key)]; s != nil {
		return s, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"

// note reconciles s's footprint into the running total: it posts the delta since
// the last note, so the total stays the exact sum of every live set's footprint.
// A mutating command calls it before returning on any set that survives the
// command (an emptied set goes through drop instead), which keeps the total exact
// at every command boundary, the only point the shard reads it. It is a single
// bool load when accounting is off. Owner goroutine only.
func (g *reg) note(s *set) {
	if !g.acctOn {
		return
	}
	nb := s.residentBytes()
	g.resident += nb - s.acct
	s.acct = nb
}

// drop removes a set from the registry: Redis deletes a set the moment its last
// member leaves, and the STORE and DEL paths drop a replaced or deleted key. It
// takes the set's last-posted footprint back out of the running total, so the
// total never carries a gone set's bytes.
func (g *reg) drop(key []byte) {
	if g.acctOn {
		if s := g.m[string(key)]; s != nil {
			g.resident -= s.acct
		}
	}
	delete(g.m, string(key))
}

// ResidentBytes is the running sum of every live set's resident-byte footprint on
// this shard, the collection half of the store's memory-pressure figure (spec
// 2064/f3/06 section 6). It is zero when the store runs no cold tier. The shard
// reads it at a demote boundary; the trigger that consumes it lands with the set
// demotion slice. Owner goroutine only.
func (g *reg) ResidentBytes() uint64 { return g.resident }

// ResidentBytes exposes the shard's set-registry resident-byte total to the
// worker's demote loop through the owner-local Coll slot, without the shard
// package reaching into the set registry's shape. It is zero before any set
// command has built a registry on this shard, or when the store runs no cold
// tier. Owner goroutine only.
func ResidentBytes(cx *shard.Ctx) uint64 {
	if g, ok := cx.Coll.(*reg); ok {
		return g.ResidentBytes()
	}
	return 0
}
