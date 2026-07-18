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

	// resident is the running sum of every live zset's resident-byte footprint
	// (zset.residentBytes), the figure the shard reads to weigh a collection's RAM
	// against the store's arena under memory pressure (spec 2064/f3/06 section 6).
	// It is kept exact by note and drop across the map's inserts and removes, so
	// the shard never walks the registry to size it. Maintained only when acctOn.
	resident uint64
	// acctOn gates the accounting: it is true only when the shard's store runs the
	// cold tier (ColdConfigured). With no cold region to demote a zset into the
	// figure would drive nothing, so the registry keeps none and note is one bool
	// load, holding the L9 zero-delta contract for a store with no resident cap.
	acctOn bool
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
		cx.ZColl = &reg{
			m:      make(map[string]*zset),
			rng:    freshPCG(),
			acctOn: cx.St != nil && cx.St.ColdConfigured(),
		}
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

// Has reports whether key holds a zset on this shard, without building the
// registry when none exists yet: the presence probe the unified TYPE consults
// across the collection types. A string value or another collection at key reads
// false, leaving the type to the caller's other probes.
func Has(cx *shard.Ctx, key []byte) bool {
	if cx.ZColl == nil {
		return false
	}
	z, _ := cx.ZColl.(*reg).lookup(cx, key)
	return z != nil
}

// Delete removes key when it holds a zset on this shard and reports whether it
// did: the zset arm of the unified single-key DEL. It builds no registry when
// none exists, so a DEL over a key of another type touches nothing here. Cold
// chunks a demoted zset left behind are not reclaimed yet, the same deferral
// every collection carries until the cold-reclamation slice threads DEL.
func Delete(cx *shard.Ctx, key []byte) bool {
	if cx.ZColl == nil {
		return false
	}
	g := cx.ZColl.(*reg)
	if g.live(cx, key) == nil {
		return false
	}
	logDeleteKey(cx, key)
	g.drop(key)
	return true
}

// Flush drops every zset on this shard, the zset arm of FLUSHALL and FLUSHDB. It
// clears the map and zeroes the resident-byte total, so a flush leaves the
// registry empty and weighing nothing, matching the store the flush just reset.
// It builds no registry when none exists.
func Flush(cx *shard.Ctx) {
	if cx.ZColl == nil {
		return
	}
	g := cx.ZColl.(*reg)
	g.m = make(map[string]*zset)
	g.resident = 0
}

// Len is the number of zsets this shard holds, the zset contribution to DBSIZE. A
// dropped zset leaves the map, so the map size is the live count; it reads zero
// before any zset command has built a registry on this shard.
func Len(cx *shard.Ctx) int {
	if cx.ZColl == nil {
		return 0
	}
	return len(cx.ZColl.(*reg).m)
}

// VolatileLen counts the zsets on this shard carrying a key-level TTL, the zset
// contribution to INFO's Keyspace expires field. It walks the registry map
// counting a non-zero deadline whether or not it has passed, matching the
// map-size basis of Len (a lazily-expired-but-unreaped zset still shows in both
// totals until a read drops it). INFO is a cold path, so the O(keys) walk is off
// every command's critical path. It builds no registry when none exists.
func VolatileLen(cx *shard.Ctx) uint64 {
	if cx.ZColl == nil {
		return 0
	}
	var n uint64
	for _, z := range cx.ZColl.(*reg).m {
		if z.expireAt != 0 {
			n++
		}
	}
	return n
}

// RangeKeys calls fn with every zset key on this shard, the zset contribution to
// the unified KEYS and SCAN walk. It builds no registry when none exists, so a
// shard that ran no zset command yields nothing. It returns false when fn asked
// to stop, halting the outer walk for a bounded scan. The slice fn receives is
// the map key's bytes, valid only for that call; fn copies what it keeps.
func RangeKeys(cx *shard.Ctx, fn func(key []byte) bool) bool {
	if cx.ZColl == nil {
		return true
	}
	now := cx.NowMs
	for k, z := range cx.ZColl.(*reg).m {
		// Skip a lazily-expired zset so KEYS and SCAN never surface a key EXISTS
		// would report absent. The skip is read-only (no drop) to match the string
		// store's expiry-aware walk, which reaps nothing during a scan.
		if z.expireAt != 0 && z.expireAt <= now {
			continue
		}
		if !fn([]byte(k)) {
			return false
		}
	}
	return true
}

// live returns the zset at key, or nil when none exists or the zset has lazily
// expired. A zset whose deadline has passed is dropped here and treated as absent,
// so an expired zset is dead to this command and every later one in the epoch
// (spec 2064/f3/16 section 2). This is the one funnel every read, mutate, create,
// and probe path routes through, so the expiry check lives in exactly one place.
// The deadline compare is a single field load against cx.NowMs, predicted away
// for the common zset that carries no TTL.
func (g *reg) live(cx *shard.Ctx, key []byte) *zset {
	z := g.m[string(key)]
	if z == nil {
		return nil
	}
	if z.expireAt != 0 && z.expireAt <= cx.NowMs {
		g.drop(key)
		return nil
	}
	return z
}

// lookup finds the zset for key. present is false when no zset exists or it has
// lazily expired; wrong is true when the key instead holds a value in the string
// store, which every zset command answers with WRONGTYPE.
func (g *reg) lookup(cx *shard.Ctx, key []byte) (z *zset, wrong bool) {
	if z = g.live(cx, key); z != nil {
		return z, false
	}
	if cx.St.Exists(key, cx.NowMs) {
		return nil, true
	}
	return nil, false
}

// note reconciles z's footprint into the running total: it posts the delta since
// the last note, so the total stays the exact sum of every live zset's footprint.
// A mutating command calls it before returning on any zset that survives the
// command (an emptied zset goes through drop instead), which keeps the total exact
// at every command boundary, the only point the shard reads it. It is a single
// bool load when accounting is off. Owner goroutine only.
func (g *reg) note(z *zset) {
	if !g.acctOn {
		return
	}
	nb := z.residentBytes()
	g.resident += nb - z.acct
	z.acct = nb
}

// drop removes an emptied zset: Redis deletes a zset the moment its last member
// leaves, and the STORE path drops a replaced or emptied destination. It takes the
// zset's last-posted footprint back out of the running total, so the total never
// carries a gone zset's bytes.
func (g *reg) drop(key []byte) {
	if g.acctOn {
		if z := g.m[string(key)]; z != nil {
			g.resident -= z.acct
		}
	}
	delete(g.m, string(key))
}

// demote packs a quantum of the named zset's coldest members into cold chunks and
// reconciles the footprint it freed back into the running total. It is the registry
// entry the demote trigger drives (the trigger wiring and the victim pick land in PR
// F); a missing zset or a listpack band is a no-op. Owner goroutine only.
func (g *reg) demote(cx *shard.Ctx, key []byte, quantum int) int {
	z := g.m[string(key)]
	if z == nil {
		return 0
	}
	n := z.demote(cx.St, key, quantum)
	if n > 0 {
		g.note(z)
	}
	return n
}

// ResidentBytes is the running sum of every live zset's resident-byte footprint on
// this shard, the collection half of the store's memory-pressure figure (spec
// 2064/f3/06 section 6). It is zero when the store runs no cold tier. The shard
// reads it at a demote boundary; the trigger that consumes it lands with the zset
// demotion slice. Owner goroutine only.
func (g *reg) ResidentBytes() uint64 { return g.resident }

// ResidentBytes exposes the shard's zset-registry resident-byte total to the
// worker's demote loop through the owner-local ZColl slot, without the shard
// package reaching into the zset registry's shape. It is zero before any zset
// command has built a registry on this shard, or when the store runs no cold tier.
// Owner goroutine only.
func ResidentBytes(cx *shard.Ctx) uint64 {
	if g, ok := cx.ZColl.(*reg); ok {
		return g.ResidentBytes()
	}
	return 0
}
