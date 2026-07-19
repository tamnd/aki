package list

import "github.com/tamnd/aki/engine/f3/shard"

// ReapExpired is the active-expiry cycle's list arm (spec 2064/f3/16 section 3): it
// drops lists whose key-level deadline has passed, the same lists peek reaps lazily
// on touch, so the background cycle only bounds how long an untouched expired list
// lingers and never changes what any command observes. It examines at most budget
// entries per call, and since Go randomizes map iteration each call samples a fresh
// slice of the keyspace the way redis's cycle samples random keys. A reaped list is
// dropped exactly the way peek drops one (g.drop reconciles the footprint), with no
// delete logged: the durable TTL re-derives the same expiry on replay, which is why
// the lazy drop logs none either. It builds no registry when none exists. Returns
// the number dropped. Owner goroutine only.
func ReapExpired(cx *shard.Ctx, nowMs int64, budget int) int {
	if budget <= 0 {
		return 0
	}
	v, ok := regs.Load(cx.St)
	if !ok {
		return 0
	}
	g := v.(*reg)
	seen, reaped := 0, 0
	for k, l := range g.m {
		if seen >= budget {
			break
		}
		seen++
		if l.expireAt != 0 && l.expireAt <= nowMs {
			// Publish the expired event for the reaped key, the same notification the
			// lazy path sends on a touch. Gated on the notify mask.
			cx.NotifyKeyspaceEvent(shard.NotifyExpired, "expired", []byte(k))
			g.drop([]byte(k))
			reaped++
		}
	}
	return reaped
}
