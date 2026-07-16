package dispatch

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The composed collection demoter (dispatch.go Demoter). The hook fans to the set
// and the zset demoters, each weighing the other's registry footprint against the
// shared resident cap. The per-type combined-budget behavior lives in the set and
// zset trigger tests; this holds the contract the worker relies on at the fan
// itself: the hook is safe to call every demote boundary on a shard that has built
// neither registry yet, the string-only path.

// TestDemoterSafeWithoutRegistries holds the string-only shard: a Ctx that never ran
// a SADD or a ZADD carries neither collection registry (Coll and ZColl both nil) and
// no cold tier, so the composed hook fans to both demoters, each returns zero without
// a panic, and the sum is zero. The worker calls this unconditionally at every
// boundary, so it must be inert before the first collection command.
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
