package dispatch

import (
	"github.com/tamnd/aki/engine/f3/hash"
	"github.com/tamnd/aki/engine/f3/list"
	"github.com/tamnd/aki/engine/f3/set"
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/stream"
	"github.com/tamnd/aki/engine/f3/zset"
)

// The active-expiry cycle's cross-keyspace reaper (spec 2064/f3/16 section 3).
// dispatch is the one package that imports every keyspace, so the reap that must
// sweep all six lives here and registers into the shard worker through the hook
// shard exposes (expirecycle.go); the worker calls it at every idle boundary. Each
// arm reaps its own type against one shared instant and returns how many keys it
// dropped; the string store is reached straight off the owner's Ctx. The per-
// keyspace budget bounds each arm independently, so one crowded type cannot starve
// the others in a single sweep, and a shard that never used a type pays only that
// arm's nil-registry check.
func init() {
	shard.RegisterExpiryReaper(reapExpired)
}

func reapExpired(cx *shard.Ctx, nowMs int64, budget int) int {
	n := cx.St.ReapExpired(nowMs, budget)
	n += set.ReapExpired(cx, nowMs, budget)
	n += zset.ReapExpired(cx, nowMs, budget)
	n += list.ReapExpired(cx, nowMs, budget)
	n += hash.ReapExpired(cx, nowMs, budget)
	n += stream.ReapExpired(cx, nowMs, budget)
	return n
}
