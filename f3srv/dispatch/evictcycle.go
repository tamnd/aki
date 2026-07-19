package dispatch

import (
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
)

// The maxmemory evictor: the cross-keyspace victim walk the shard boundary calls
// through the hook Runtime.UseEvictor wired (spec 2064/f3/16 sections 6 and 7).
// It lives here because dispatch is the one package that imports every keyspace,
// the same reason the demoter (Demoter) and the cross-keyspace active-expiry
// reaper (expirecycle.go) live here. The shard side is evictcycle.go there.
//
// What this slice does and does not do. It enforces maxmemory as a RAM ceiling by
// shedding victims under an eviction policy, so a memory-bounded cache stops
// growing past its budget and evicted_keys rises, and it validates and acts on
// all ten policy names redis knows (evictpolicy.go). It targets a live-byte figure
// that drops synchronously as each victim is dropped (store.LiveResident plus each
// collection's resident total), so the loop stops the moment it is under budget
// rather than over-evicting against the arena's lagging fill counter (the
// pendingUncertain discipline, spec 2064/f3/16 section 6.4); the boundary's own
// compaction then returns the freed pages to the OS.
//
// Deferred, and honest about it. noeviction's OOM refusal (refusing the write once
// no victim is left) rides the block-not-drop rail and is its own slice, so under
// noeviction this pass finds no policy to act on and returns; a store left on
// noeviction with maxmemory set keeps serving and grows toward the arena's
// physical backpressure rather than the earlier maxmemory OOM. The structural
// victim machinery the spec designs (the SIEVE hand over allocation order, the
// Morris LFU counter) is a perf upgrade not yet built, so the LRU family scores by
// the idle clock, the LFU family falls back to it, and sampling stands in for the
// hand (evictpolicy.go). maxstore, the migration-vs-eviction inversion, and the
// two-budget matrix are the remaining M9 arc. Collection eviction only engages
// where the registry accounts its bytes (the cold tier is configured), so a
// no-cold-tier deployment enforces maxmemory against the string store, the
// memory-bar keyspace, and grows collection footprint unmetered until the
// accounting-under-maxmemory follow-up.

// evMaxMemory, evPolicy, and evSamples are the live eviction settings CONFIG SET
// writes (config.go) and the evictor reads on the owner. They are atomics so the
// client goroutine that runs CONFIG SET and the shard owner that reads them need
// no lock, the same shape the config store's own values have.
var (
	evMaxMemory atomic.Uint64 // budget in bytes, 0 = unbounded
	evPolicy    atomic.Uint32 // a store.Policy* code
	evSamples   atomic.Int64  // maxmemory-samples, keys examined per victim pick
)

func init() {
	// Seed to match the config table's seeds (config.go): unbounded memory,
	// noeviction policy (the zero value), five samples, redis's defaults.
	evSamples.Store(5)
}

// evictBudget bounds how many victims one boundary pass sheds, so a large
// overshoot resolves across a few ticks rather than one long stall (spec
// 2064/f3/16 section 7.7). The boundary fires every drain pass under a write
// burst, so a modest per-pass cap still keeps a shard under its budget.
const evictBudget = 64

// Evictor returns the maxmemory eviction hook for Runtime.UseEvictor, the entry
// the worker's boundary calls under memory pressure. It self-gates on the live
// maxmemory setting, so a store that never sets maxmemory returns after one atomic
// load; the returned count is the keys this pass evicted, which the shard credits
// to evicted_keys.
func Evictor() func(*shard.Ctx) int {
	return func(cx *shard.Ctx) int {
		max := evMaxMemory.Load()
		if max == 0 {
			return 0
		}
		policy := uint8(evPolicy.Load())
		if !store.PolicyEvicts(policy) {
			// noeviction: no victims to consider; the OOM refusal is its own slice.
			return 0
		}
		shards := uint64(cx.Shards())
		if shards == 0 {
			shards = 1
		}
		share := max / shards
		volatileOnly := store.PolicyVolatileOnly(policy)
		sample := int(evSamples.Load())
		if sample < 1 {
			sample = 1
		}
		evicted := 0
		for evicted < evictBudget {
			if shardLiveUsed(cx) <= share {
				break
			}
			key, ks, ok := bestVictim(cx, policy, sample, volatileOnly)
			if !ok {
				// Nothing evictable: an allkeys-* store with no key, or a
				// volatile-* store with no volatile key. Redis answers OOM here
				// rather than widening scope; that refusal is the noeviction-OOM
				// slice, so for now the pass simply stops.
				break
			}
			if !dropVictim(cx, ks, key) {
				break
			}
			// A maxmemory eviction is a real removal a subscriber wants to learn
			// about (the cache-invalidation case); publish the evicted event for the
			// victim's key. Gated on the notify mask, so it costs one load when
			// notifications are off.
			cx.NotifyKeyspaceEvent(shard.NotifyEvicted, "evicted", key)
			evicted++
		}
		return evicted
	}
}

// shardLiveUsed is the shard's live RAM figure the budget bounds: the string
// store's live charge plus index footprint (store.LiveResident) plus every
// collection registry's resident total. Each term drops synchronously as a key is
// dropped, so the eviction loop reading it after each victim never over-evicts.
// A collection whose registry does not account its bytes (no cold tier) reports
// zero, so it does not enter the figure and its keys are left unmetered, the
// deferral the file comment names. Owner goroutine only; O(segments) for the
// string term, boundary-rate.
func shardLiveUsed(cx *shard.Ctx) uint64 {
	return cx.St.LiveResident() +
		set.ResidentBytes(cx) + zset.ResidentBytes(cx) +
		list.ResidentBytes(cx) + hash.ResidentBytes(cx) + stream.ResidentBytes(cx)
}

// keyspace tags for routing a chosen victim back to its owning keyspace's drop.
const (
	ksString = iota
	ksSet
	ksZset
	ksList
	ksHash
	ksStream
)

// bestVictim samples each metered keyspace for its best victim under the policy
// and returns the single highest-scoring one across all of them, the cross-
// keyspace pick redis makes over one global sample. Only a keyspace whose bytes
// enter the budget (the string store always, a collection only where it accounts)
// is sampled, so a chosen victim always reduces shardLiveUsed. ok is false when no
// keyspace offered an eligible victim. The returned key is a fresh copy the drop
// can hold. Owner goroutine only.
func bestVictim(cx *shard.Ctx, policy uint8, sample int, volatileOnly bool) (key []byte, ks int, ok bool) {
	var bestScore int64
	consider := func(k []byte, score int64, cand int, cok bool) {
		if !cok {
			return
		}
		if !ok || score > bestScore {
			key, bestScore, ks, ok = k, score, cand, true
		}
	}
	if k, score, cok := cx.St.EvictVictim(policy, cx.NowMs, sample, volatileOnly); cok {
		consider(k, score, ksString, true)
	}
	if set.EvictAccounted(cx) {
		k, score, cok := set.EvictVictim(cx, policy, sample, volatileOnly)
		consider(k, score, ksSet, cok)
	}
	if zset.EvictAccounted(cx) {
		k, score, cok := zset.EvictVictim(cx, policy, sample, volatileOnly)
		consider(k, score, ksZset, cok)
	}
	if list.EvictAccounted(cx) {
		k, score, cok := list.EvictVictim(cx, policy, sample, volatileOnly)
		consider(k, score, ksList, cok)
	}
	if hash.EvictAccounted(cx) {
		k, score, cok := hash.EvictVictim(cx, policy, sample, volatileOnly)
		consider(k, score, ksHash, cok)
	}
	if stream.EvictAccounted(cx) {
		k, score, cok := stream.EvictVictim(cx, policy, sample, volatileOnly)
		consider(k, score, ksStream, cok)
	}
	return key, ks, ok
}

// dropVictim removes the chosen key from its owning keyspace, logging the delete
// the way DEL does so a replay does not resurrect the evicted key. It reports
// whether a key was actually there, so a stale pick (dropped between the sample
// and here, which the single owner makes impossible in practice) stops the loop
// rather than spinning. Owner goroutine only.
func dropVictim(cx *shard.Ctx, ks int, key []byte) bool {
	switch ks {
	case ksString:
		return cx.St.EvictKey(key)
	case ksSet:
		return set.EvictKey(cx, key)
	case ksZset:
		return zset.EvictKey(cx, key)
	case ksList:
		return list.EvictKey(cx, key)
	case ksHash:
		return hash.EvictKey(cx, key)
	case ksStream:
		return stream.EvictKey(cx, key)
	}
	return false
}

// parseMemoryBytes reads a redis memory quantity: a plain integer of bytes, or a
// number with a b/k/kb/m/mb/g/gb suffix, matching redis's memtoull. The bare
// k/m/g suffixes are decimal (1000-based) and the b-suffixed kb/mb/gb are binary
// (1024-based), redis's own split. ok is false on a malformed value, which lets
// CONFIG SET reject it rather than store a budget the evictor cannot read.
func parseMemoryBytes(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	// Split the trailing unit letters from the leading digits.
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, false
	}
	var n uint64
	for j := 0; j < i; j++ {
		n = n*10 + uint64(s[j]-'0')
	}
	unit := s[i:]
	var mul uint64
	switch lowerUnit(unit) {
	case "", "b":
		mul = 1
	case "k":
		mul = 1000
	case "kb":
		mul = 1024
	case "m":
		mul = 1000 * 1000
	case "mb":
		mul = 1024 * 1024
	case "g":
		mul = 1000 * 1000 * 1000
	case "gb":
		mul = 1024 * 1024 * 1024
	default:
		return 0, false
	}
	return n * mul, true
}

// lowerUnit lowercases the short unit suffix for the case-insensitive match
// redis does on memory units.
func lowerUnit(u string) string {
	b := make([]byte, len(u))
	for i := 0; i < len(u); i++ {
		c := u[i]
		if c >= 'A' && c <= 'Z' {
			c += 0x20
		}
		b[i] = c
	}
	return string(b)
}
