package zset

import "github.com/tamnd/aki/engine/f3/shard"

// ReapExpired is the active-expiry cycle's zset arm (spec 2064/f3/16 section 3):
// it drops zsets whose key-level deadline has passed, the same zsets peek reaps
// lazily on touch, so the background cycle only bounds how long an untouched
// expired zset lingers and never changes what any command observes. It examines at
// most budget entries per call, and since Go randomizes map iteration each call
// samples a fresh slice of the keyspace the way redis's cycle samples random keys;
// whatever it does not reach this pass a later pass or a first access reaps. A
// reaped zset is dropped exactly the way peek drops one (g.drop reconciles the
// footprint), with no delete logged: the durable TTL re-derives the same expiry on
// replay, which is why the lazy drop logs none either. It builds no registry when
// none exists. Returns the number dropped. Owner goroutine only.
func ReapExpired(cx *shard.Ctx, nowMs int64, budget int) int {
	if cx.ZColl == nil || budget <= 0 {
		return 0
	}
	g := cx.ZColl.(*reg)
	seen, reaped := 0, 0
	for k, z := range g.m {
		if seen >= budget {
			break
		}
		seen++
		if z.expireAt != 0 && z.expireAt <= nowMs {
			// Publish the expired event for the reaped key, the same notification the
			// lazy path sends on a touch. Gated on the notify mask.
			cx.NotifyKeyspaceEvent(shard.NotifyExpired, "expired", []byte(k))
			g.drop([]byte(k))
			reaped++
		}
	}
	return reaped
}
