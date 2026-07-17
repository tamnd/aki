package zset

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// zsetExpireCtx builds a shard Ctx over a plain (cold-off) store with the clock at
// nowMs, the minimum a zset command needs: the registry and its lazy-expiry funnel
// read cx.NowMs, and lookup consults cx.St.Exists for the WRONGTYPE probe.
func zsetExpireCtx(nowMs int64) *shard.Ctx {
	return &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: nowMs}
}

// TestZsetLiveLazyExpiry checks that a zset whose deadline has passed is dropped by
// the live funnel and reported absent, and that the drop removes it from the map
// so it is gone to every later command in the epoch.
func TestZsetLiveLazyExpiry(t *testing.T) {
	cx := zsetExpireCtx(100)
	g := registry(cx)
	addKey(g, "z", "a", "b")
	g.m["z"].expireAt = 150

	// Before the deadline the zset is live.
	if g.live(cx, []byte("z")) == nil {
		t.Fatal("zset should be live before its deadline")
	}

	// At and after the deadline the zset is dropped and reported absent.
	cx.NowMs = 150
	if g.live(cx, []byte("z")) != nil {
		t.Fatal("zset at its deadline should be reported absent")
	}
	if _, ok := g.m["z"]; ok {
		t.Fatal("an expired zset should be dropped from the map")
	}
	if Has(cx, []byte("z")) {
		t.Fatal("Has should read false for an expired zset")
	}
}

// TestZsetExpireCreateAfterExpiry is the create-path hazard the plan calls out: a
// fresh add on an expired key must build a new zset with no TTL, never resurrect
// the shadowed expired one. Zadd's create path is `z := g.live(...); if z == nil
// { z = newZset() }`, so this drives that funnel directly (a unit test cannot
// build the concrete shard.Reply the handler takes; the driver test covers the
// wired ZADD end to end).
func TestZsetExpireCreateAfterExpiry(t *testing.T) {
	cx := zsetExpireCtx(100)
	g := registry(cx)
	addKey(g, "z", "old")
	g.m["z"].expireAt = 150

	cx.NowMs = 200
	// The create funnel drops the expired zset and reports absence, so the handler
	// takes its create branch instead of adding into the stale zset.
	if g.live(cx, []byte("z")) != nil {
		t.Fatal("create funnel should drop the expired zset and report absent")
	}
	if _, ok := g.m["z"]; ok {
		t.Fatal("expired zset should be gone from the map before the create")
	}
	// Zadd then builds a fresh zset from the new member; it carries no deadline and
	// none of the old members.
	z := newZset()
	g.m["z"] = z
	z.update([]byte("new"), 1, flags{})
	if z.expireAt != 0 {
		t.Fatalf("recreated zset should carry no deadline, got %d", z.expireAt)
	}
	if _, ok := z.score([]byte("new")); !ok {
		t.Fatal("recreated zset should hold the new member")
	}
	if _, ok := z.score([]byte("old")); ok {
		t.Fatal("recreated zset should not hold the old member")
	}
}

// TestZsetDeadlinePersist exercises the Deadline and Persist backends the unified
// TTL and PERSIST handlers drive.
func TestZsetDeadlinePersist(t *testing.T) {
	cx := zsetExpireCtx(100)
	g := registry(cx)
	addKey(g, "z", "a")

	// A live zset with no deadline reports at==0, ok==true.
	if at, ok := Deadline(cx, []byte("z")); !ok || at != 0 {
		t.Fatalf("no-TTL zset: got (%d,%v), want (0,true)", at, ok)
	}
	// Persist on a zset with no deadline removes nothing.
	if Persist(cx, []byte("z")) {
		t.Fatal("Persist on a no-TTL zset should report false")
	}

	g.m["z"].expireAt = 500
	if at, ok := Deadline(cx, []byte("z")); !ok || at != 500 {
		t.Fatalf("TTL zset: got (%d,%v), want (500,true)", at, ok)
	}
	if !Persist(cx, []byte("z")) {
		t.Fatal("Persist on a zset with a deadline should report true")
	}
	if at, _ := Deadline(cx, []byte("z")); at != 0 {
		t.Fatalf("deadline should be cleared, got %d", at)
	}

	// An absent key reports not present from both.
	if _, ok := Deadline(cx, []byte("missing")); ok {
		t.Fatal("absent key should report not present")
	}
	if Persist(cx, []byte("missing")) {
		t.Fatal("Persist on an absent key should report false")
	}
}

// TestZsetRangeKeysSkipsExpired checks that KEYS and SCAN never surface a zset key
// that has lazily expired, matching EXISTS.
func TestZsetRangeKeysSkipsExpired(t *testing.T) {
	cx := zsetExpireCtx(100)
	g := registry(cx)
	addKey(g, "live", "a")
	addKey(g, "dead", "a")
	g.m["dead"].expireAt = 150

	cx.NowMs = 200
	var seen []string
	RangeKeys(cx, func(key []byte) bool {
		seen = append(seen, string(key))
		return true
	})
	if len(seen) != 1 || seen[0] != "live" {
		t.Fatalf("RangeKeys should yield only the live key, got %v", seen)
	}
}
