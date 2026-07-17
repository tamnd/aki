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

	// resident is the running sum of every live hash's resident-byte footprint
	// (hash.residentBytes), the figure the shard reads to weigh the hash heap
	// against the store's resident cap at a demote boundary (spec 2064/f3/06
	// section 6). It is maintained by note and drop so the shard never walks the
	// registry to size it. Maintained only when acctOn.
	resident uint64
	// acctOn gates the accounting: it is true only when the shard's store runs the
	// cold tier (ColdConfigured). With no cold region to demote a hash into, there
	// is nothing to weigh, so note and drop skip the bookkeeping entirely and the
	// write path stays byte-identical to M0, holding the L9 zero-delta contract for
	// a store with no resident cap.
	acctOn bool
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
	v, _ := regs.LoadOrStore(cx.St, &reg{
		m:      make(map[string]*hash),
		rng:    freshPCG(),
		acctOn: cx.St != nil && cx.St.ColdConfigured(),
	})
	return v.(*reg)
}

// note reconciles h's footprint into the running resident total: it posts the
// delta since the last note, so the total stays the exact sum of every live hash's
// footprint. A mutating command calls it before returning on any hash that survives
// the command (an emptied hash goes through drop instead), which keeps the total
// exact at every command boundary, the only point the shard reads it. It is a
// single bool load when accounting is off. Owner goroutine only.
func (g *reg) note(h *hash) {
	if !g.acctOn {
		return
	}
	nb := h.residentBytes()
	g.resident += nb - h.acct
	h.acct = nb
}

// ResidentBytes is the running sum of every live hash's resident-byte footprint on
// this shard, the collection contribution to the store's memory-pressure figure
// (spec 2064/f3/06 section 6). It is zero when the store runs no cold tier. The
// shard reads it at a demote boundary; the trigger that consumes it lands with the
// hash demotion slice. Owner goroutine only.
func (g *reg) ResidentBytes() uint64 { return g.resident }

// ResidentBytes exposes the shard's hash-registry resident-byte total to the
// worker's demote loop. The hash registry hangs off the shared regs map keyed by
// the shard's store, not a Ctx slot, so this reads that map without building a
// registry on a shard that never ran a hash command: it is zero before the first
// hash command, or when the store runs no cold tier. Owner goroutine only.
func ResidentBytes(cx *shard.Ctx) uint64 {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg).ResidentBytes()
	}
	return 0
}

// Has reports whether key holds a hash on this shard, without building the
// registry when none exists yet: the presence probe the unified TYPE consults
// across the collection types. Like lookup it reaps expired fields first, so a
// hash emptied by field expiry reads as absent. A string value or another
// collection at key reads false, leaving the type to the caller's other probes.
func Has(cx *shard.Ctx, key []byte) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return false
	}
	h, _ := v.(*reg).lookup(cx, key)
	return h != nil
}

// Delete removes key when it holds a hash on this shard and reports whether it
// did: the hash arm of the unified single-key DEL. It builds no registry when
// none exists, so a DEL over a key of another type touches nothing here. lookup
// reaps expired fields first, so a hash already emptied by field expiry deletes
// as absent. Cold chunks a demoted hash left behind are not reclaimed yet, the
// same deferral every collection carries until the cold-reclamation slice
// threads DEL.
func Delete(cx *shard.Ctx, key []byte) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return false
	}
	g := v.(*reg)
	h, _ := g.lookup(cx, key)
	if h == nil {
		return false
	}
	g.drop(key)
	return true
}

// Flush drops every hash on this shard, the hash arm of FLUSHALL and FLUSHDB. It
// clears the map and zeroes the resident-byte total, so a flush leaves the
// registry empty and weighing nothing, matching the store the flush just reset.
// It builds no registry when none exists on this shard.
func Flush(cx *shard.Ctx) {
	v, ok := regs.Load(cx.St)
	if !ok {
		return
	}
	g := v.(*reg)
	g.m = make(map[string]*hash)
	g.resident = 0
}

// Len is the number of hashes this shard holds, the hash contribution to DBSIZE.
// A dropped hash leaves the map, so the map size is the live count; it reads zero
// before any hash command has built a registry on this shard.
func Len(cx *shard.Ctx) int {
	v, ok := regs.Load(cx.St)
	if !ok {
		return 0
	}
	return len(v.(*reg).m)
}

// RangeKeys calls fn with every hash key on this shard, the hash contribution to
// the unified KEYS and SCAN walk. It reaches the registry through regs.Load so a
// shard that ran no hash command builds nothing and yields nothing. It returns
// false when fn asked to stop, halting the outer walk for a bounded scan. The
// slice fn receives is the map key's bytes, valid only for that call; fn copies
// what it keeps.
func RangeKeys(cx *shard.Ctx, fn func(key []byte) bool) bool {
	v, ok := regs.Load(cx.St)
	if !ok {
		return true
	}
	for k := range v.(*reg).m {
		if !fn([]byte(k)) {
			return false
		}
	}
	return true
}

// lookup finds the hash for key. present is nil when no hash exists; wrong is true
// when the key instead holds a value in the string store, which every hash command
// answers with WRONGTYPE. Cross-type collisions with the set, zset, and list
// registries are not resolved in this slice, the same deferral those slices carry
// until keyspace unification threads every type through one holder.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (h *hash, wrong bool) {
	if h = g.m[string(key)]; h != nil {
		// Lazy field-TTL expiry: reap fired fields before the command sees the hash,
		// so every read and write operates on a hash free of expired fields (spec
		// 2064/f3/10 section 6.1). A hash whose last field just expired is deleted,
		// the same way Redis drops a hash the moment it empties.
		h.reap(uint64(cx.NowMs))
		if h.card() == 0 {
			g.drop(key)
			return nil, false
		}
		return h, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// drop removes an emptied hash from the registry: Redis deletes a hash the moment
// its last field leaves. It takes the hash's last-posted footprint back out of the
// running total, so the total never carries a gone hash's bytes.
func (g *reg) drop(key []byte) {
	if g.acctOn {
		if h := g.m[string(key)]; h != nil {
			g.resident -= h.acct
		}
	}
	delete(g.m, string(key))
}

// wrongType is the shared WRONGTYPE reply text, Redis's exact wording.
const wrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
