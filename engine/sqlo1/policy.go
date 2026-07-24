package sqlo1

// The maxmemory-policy surface, doc 11 section 5. The Redis policy
// names are honored as tiering flavors: pressure demotes records to
// disk instead of deleting them, so a policy here only changes how the
// evictor ranks demotion victims, never whether data survives (E-I6).
// Destructive eviction exists solely behind the server's hard-evict
// opt-in, which deletes through the command path per the same ranking.

// EvictPolicy selects the demotion ranking flavor. The zero value is
// noeviction, which is also the recommended default: pure tiering with
// the doc 04 WATT-lite score.
type EvictPolicy uint8

const (
	PolicyNoEviction EvictPolicy = iota
	PolicyAllkeysLRU
	PolicyAllkeysLFU
	PolicyAllkeysRandom
	PolicyVolatileLRU
	PolicyVolatileLFU
	PolicyVolatileRandom
	PolicyVolatileTTL
)

// policyNames maps each policy to its Redis config name, in constant
// order so String and ParseEvictPolicy stay one table.
var policyNames = [...]string{
	PolicyNoEviction:     "noeviction",
	PolicyAllkeysLRU:     "allkeys-lru",
	PolicyAllkeysLFU:     "allkeys-lfu",
	PolicyAllkeysRandom:  "allkeys-random",
	PolicyVolatileLRU:    "volatile-lru",
	PolicyVolatileLFU:    "volatile-lfu",
	PolicyVolatileRandom: "volatile-random",
	PolicyVolatileTTL:    "volatile-ttl",
}

func (p EvictPolicy) String() string {
	if int(p) < len(policyNames) {
		return policyNames[p]
	}
	return "noeviction"
}

// ParseEvictPolicy resolves a Redis maxmemory-policy name; the name
// comparison is exact and lowercase like Redis's own config parser.
func ParseEvictPolicy(name string) (EvictPolicy, bool) {
	for p, n := range policyNames {
		if n == name {
			return EvictPolicy(p), true
		}
	}
	return PolicyNoEviction, false
}

// volatileFirst reports whether volatile keys demote before
// non-volatile ones at equal rank (the volatile-* families).
// volatile-ttl orders volatile keys ahead structurally through its
// score, so it does not need the tiebreak.
func (p EvictPolicy) volatileFirst() bool {
	switch p {
	case PolicyVolatileLRU, PolicyVolatileLFU, PolicyVolatileRandom:
		return true
	}
	return false
}

// Ranking flavors behind the policy names: the allkeys and volatile
// variants of a flavor share a score and differ only in the tiebreak.
type evictFlavor uint8

const (
	flavorWatt evictFlavor = iota
	flavorLRU
	flavorLFU
	flavorRandom
	flavorTTL
)

func (p EvictPolicy) flavor() evictFlavor {
	switch p {
	case PolicyAllkeysLRU, PolicyVolatileLRU:
		return flavorLRU
	case PolicyAllkeysLFU, PolicyVolatileLFU:
		return flavorLFU
	case PolicyAllkeysRandom, PolicyVolatileRandom:
		return flavorRandom
	case PolicyVolatileTTL:
		return flavorTTL
	}
	return flavorWatt
}
