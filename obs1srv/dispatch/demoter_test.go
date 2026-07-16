package dispatch

import (
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The composed collection demoter (dispatch.go Demoter). The hook fans to the set,
// the zset, the list, and the hash demoters, each weighing the other three
// registries' footprint against the shared resident cap. The per-type combined-budget
// behavior lives in the set, zset, list, and hash trigger tests; this holds the
// contract the worker relies on at the fan itself: the hook is safe to call every
// demote boundary on a shard that has built no collection registry yet, the
// string-only path.

// TestDemoterSafeWithoutRegistries holds the string-only shard: a Ctx that never ran
// a SADD, a ZADD, an RPUSH, or an HSET carries no collection registry (Coll and ZColl
// nil and no list or hash registry in the regs map) and no cold tier, so the composed
// hook fans to all four demoters, each returns zero without a panic, and the sum is
// zero. The worker calls this unconditionally at every boundary, so it must be inert
// before the first collection command.
func TestDemoterSafeWithoutRegistries(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	demote := Demoter()
	if n := demote(cx); n != 0 {
		t.Fatalf("demoter on a bare ctx moved %d, want 0", n)
	}
	// Idempotent: a second call with no registries is still a no-op.
	if n := demote(cx); n != 0 {
		t.Fatalf("second demoter call moved %d, want 0", n)
	}
}
