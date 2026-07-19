package store

// The maxmemory-policy vocabulary and the one victim comparator every keyspace
// scores against (spec 2064/f3/16 sections 6.5 and 7.3). This lives in store,
// the leaf package, so the shard, the dispatch evictor, and every collection
// package can name a policy and score a candidate against one definition rather
// than each carrying its own copy. The policy governs eviction only; migration
// is always cold-by-access (F12), so nothing here reads the cold tier.
//
// The honest edge for f3's first eviction slice: the spec's structural machinery
// (the SIEVE hand over allocation order, the 5-bit Morris LFU counter in the
// evict byte) is a perf-structural upgrade that has not landed. Until it does the
// LRU family scores by the per-key idle clock the store already stamps (str.go
// IdleSeconds), the LFU family falls back to that same idle clock (there is no
// frequency counter yet, so OBJECT FREQ stays a stub), and the LRM pair scores by
// the same clock too, which f3 also stamps on write. Random takes whatever the
// sample surfaces first, and Go's randomized map iteration is the sampler. The
// volatile-ttl fast path is the one policy that reads a field of its own, the
// deadline. When the hand and the counter land the comparator changes here and
// the keyspaces are untouched, the same seam the reapers use.

// The ten policies redis parses, plus the redis 8.6 write-recency pair (lrm).
// noeviction is the default and the zero value so a store that never sets a
// policy behaves as redis's default does.
const (
	PolicyNoeviction uint8 = iota
	PolicyAllkeysLRU
	PolicyVolatileLRU
	PolicyAllkeysLFU
	PolicyVolatileLFU
	PolicyAllkeysRandom
	PolicyVolatileRandom
	PolicyVolatileTTL
	PolicyAllkeysLRM
	PolicyVolatileLRM
)

// policyNames maps each policy to the exact spelling redis's CONFIG GET
// maxmemory-policy returns, so a client reads back the name it set.
var policyNames = [...]string{
	PolicyNoeviction:     "noeviction",
	PolicyAllkeysLRU:     "allkeys-lru",
	PolicyVolatileLRU:    "volatile-lru",
	PolicyAllkeysLFU:     "allkeys-lfu",
	PolicyVolatileLFU:    "volatile-lfu",
	PolicyAllkeysRandom:  "allkeys-random",
	PolicyVolatileRandom: "volatile-random",
	PolicyVolatileTTL:    "volatile-ttl",
	PolicyAllkeysLRM:     "allkeys-lrm",
	PolicyVolatileLRM:    "volatile-lrm",
}

// ParsePolicy resolves a maxmemory-policy name to its code, rejecting anything
// redis would reject so CONFIG SET maxmemory-policy answers the same error on a
// typo rather than storing a name no eviction pass understands. The match is
// exact and case-sensitive, matching redis's own table lookup.
func ParsePolicy(name string) (uint8, bool) {
	for p, n := range policyNames {
		if n == name {
			return uint8(p), true
		}
	}
	return PolicyNoeviction, false
}

// PolicyName returns the redis spelling of a policy code, the value CONFIG GET
// reflects. An out-of-range code reports the default, the same forgiving shape
// the config store already has.
func PolicyName(p uint8) string {
	if int(p) >= len(policyNames) {
		return policyNames[PolicyNoeviction]
	}
	return policyNames[p]
}

// PolicyEvicts reports whether a policy sheds keys under pressure at all.
// noeviction is the only one that does not: under it the store keeps every key
// and refuses the write that cannot fit (the OOM path, its own slice), so an
// eviction pass under noeviction has no victims to consider and returns at once.
func PolicyEvicts(p uint8) bool { return p != PolicyNoeviction }

// PolicyVolatileOnly reports whether a policy restricts victims to keys that
// carry an expiry deadline. The volatile-* family does; the allkeys-* family
// considers every key. A volatile-* policy with no volatile keys finds no
// victim, which redis resolves to OOM rather than widening scope to immortal
// keys (spec 2064/f3/16 section 6.5), the same no-victim outcome this reports.
func PolicyVolatileOnly(p uint8) bool {
	switch p {
	case PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom,
		PolicyVolatileTTL, PolicyVolatileLRM:
		return true
	}
	return false
}

// EvictScore ranks one candidate for its policy: higher score means a better
// victim, so an evictor comparing candidates across keyspaces picks the highest
// and drops it. idleSecs is seconds since the key's last touch (the LRU clock),
// expireAt is its millisecond deadline or zero for none. Within a single
// eviction pass every candidate is scored by the same policy, so the scores are
// always comparable even though the families read different fields.
//
//   - LRU/LFU/LRM: the idlest key wins, so the score is the idle seconds. LFU
//     shares this until the frequency counter lands (see the file comment); LRM
//     shares it because f3 stamps the clock on write as well as read.
//   - volatile-ttl: the soonest-to-expire key wins. Deadlines are absolute ms,
//     so negating expireAt makes a nearer deadline the higher score; a key with
//     no deadline never reaches this path (the volatile filter drops it first).
//   - random: every candidate ties at zero, so the sampler's order decides, and
//     Go's randomized map iteration makes that a random pick.
func EvictScore(policy uint8, idleSecs uint32, expireAt int64) int64 {
	switch policy {
	case PolicyAllkeysLRU, PolicyVolatileLRU,
		PolicyAllkeysLFU, PolicyVolatileLFU,
		PolicyAllkeysLRM, PolicyVolatileLRM:
		return int64(idleSecs)
	case PolicyVolatileTTL:
		return -expireAt
	default:
		// allkeys-random, volatile-random, and any unexpected code: no
		// preference, the sample order picks.
		return 0
	}
}
