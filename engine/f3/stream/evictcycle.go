package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The maxmemory evictor's stream arm (spec 2064/f3/16 sections 6.4 and 7.3): the
// sampled victim pick, the durable drop, and the accounting gate the dispatch
// evictor drives when the shard overshoots its budget. It scores candidates
// through the one store.EvictScore comparator, so a stream victim weighs against a
// string, a set, or a zset on one scale. The registry hangs off the shared regs
// map keyed by the shard's store (the same seam ReapExpired reaches through), and
// Go's randomized map iteration is the sampler. An emptied-but-kept stream (XLEN
// reads 0) is still a live key the sample surfaces like any other.

// EvictVictim samples up to sample keys from this shard's stream registry and
// returns the best victim for the policy (highest store.EvictScore), key copied.
// volatileOnly skips a key whose expireAt is 0. ok is false when the registry is
// absent/empty or, under volatileOnly, holds no volatile key. Go's randomized map
// iteration is the sampler. Owner-only.
func EvictVictim(cx *shard.Ctx, policy uint8, sample int, volatileOnly bool) (key []byte, score int64, ok bool) {
	if sample <= 0 {
		return nil, 0, false
	}
	v, ok := regs.Load(cx.St)
	if !ok {
		return nil, 0, false
	}
	g := v.(*reg)
	seen := 0
	best := ""
	var bestScore int64
	found := false
	for k, s := range g.m {
		if seen >= sample {
			break
		}
		if volatileOnly && s.expireAt == 0 {
			continue
		}
		seen++
		idle := store.IdleSecondsFrom(s.clock, cx.NowMs)
		sc := store.EvictScore(policy, uint32(idle), s.expireAt)
		if !found || sc > bestScore {
			bestScore = sc
			best = k
			found = true
		}
	}
	if !found {
		return nil, 0, false
	}
	return append([]byte(nil), best...), bestScore, true
}

// EvictKey drops the stream at key and reports whether one was there, logging the
// delete the way the package's single-key Delete does (eviction must be durable so
// replay does not resurrect it). Owner-only.
func EvictKey(cx *shard.Ctx, key []byte) bool {
	return Delete(cx, key)
}

// EvictAccounted reports whether this shard's stream registry tracks resident bytes
// (the acctOn gate). The evictor only samples a keyspace whose footprint it can
// measure against the budget, so an unaccounted registry is skipped. Owner-only.
func EvictAccounted(cx *shard.Ctx) bool {
	if v, ok := regs.Load(cx.St); ok {
		return v.(*reg).acctOn
	}
	return false
}
