package set

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// setExpireCtx builds a shard Ctx over a plain (cold-off) store with the clock at
// nowMs, the minimum a set command needs: the registry and its lazy-expiry funnel
// read cx.NowMs, and lookup consults cx.St.Exists for the WRONGTYPE probe.
func setExpireCtx(nowMs int64) *shard.Ctx {
	return &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: nowMs}
}

// TestSetLiveLazyExpiry checks that a set whose deadline has passed is dropped by
// the live funnel and reported absent, and that the drop removes it from the map
// so it is gone to every later command in the epoch.
func TestSetLiveLazyExpiry(t *testing.T) {
	cx := setExpireCtx(100)
	g := registry(cx)
	addKey(g, "s", "a", "b")
	g.m["s"].expireAt = 150

	// Before the deadline the set is live.
	if g.live(cx, []byte("s")) == nil {
		t.Fatal("set should be live before its deadline")
	}

	// At and after the deadline the set is dropped and reported absent.
	cx.NowMs = 150
	if g.live(cx, []byte("s")) != nil {
		t.Fatal("set at its deadline should be reported absent")
	}
	if _, ok := g.m["s"]; ok {
		t.Fatal("an expired set should be dropped from the map")
	}
	if Has(cx, []byte("s")) {
		t.Fatal("Has should read false for an expired set")
	}
}

// TestSetExpireCreateAfterExpiry is the create-path hazard the plan calls out: a
// fresh add on an expired key must build a new set with no TTL, never resurrect
// the shadowed expired one. Sadd's create path is `s := g.live(...); if s == nil
// { s = newSet(...) }`, so this drives that funnel directly (a unit test cannot
// build the concrete shard.Reply the handler takes; the driver test covers the
// wired SADD end to end).
func TestSetExpireCreateAfterExpiry(t *testing.T) {
	cx := setExpireCtx(100)
	g := registry(cx)
	addKey(g, "s", "old")
	g.m["s"].expireAt = 150

	cx.NowMs = 200
	// The create funnel drops the expired set and reports absence, so the handler
	// takes its create branch instead of adding into the stale set.
	if g.live(cx, []byte("s")) != nil {
		t.Fatal("create funnel should drop the expired set and report absent")
	}
	if _, ok := g.m["s"]; ok {
		t.Fatal("expired set should be gone from the map before the create")
	}
	// Sadd then builds a fresh set from the new member; it carries no deadline and
	// none of the old members.
	s := newSet([]byte("new"))
	g.m["s"] = s
	s.add([]byte("new"))
	if s.expireAt != 0 {
		t.Fatalf("recreated set should carry no deadline, got %d", s.expireAt)
	}
	if !s.has([]byte("new")) || s.has([]byte("old")) {
		t.Fatal("recreated set should hold only the new member")
	}
}

// TestSetDeadlinePersist exercises the Deadline and Persist backends the unified
// TTL and PERSIST handlers drive.
func TestSetDeadlinePersist(t *testing.T) {
	cx := setExpireCtx(100)
	g := registry(cx)
	addKey(g, "s", "a")

	// A live set with no deadline reports at==0, ok==true.
	if at, ok := Deadline(cx, []byte("s")); !ok || at != 0 {
		t.Fatalf("no-TTL set: got (%d,%v), want (0,true)", at, ok)
	}
	// Persist on a set with no deadline removes nothing.
	if Persist(cx, []byte("s")) {
		t.Fatal("Persist on a no-TTL set should report false")
	}

	g.m["s"].expireAt = 500
	if at, ok := Deadline(cx, []byte("s")); !ok || at != 500 {
		t.Fatalf("TTL set: got (%d,%v), want (500,true)", at, ok)
	}
	if !Persist(cx, []byte("s")) {
		t.Fatal("Persist on a set with a deadline should report true")
	}
	if at, _ := Deadline(cx, []byte("s")); at != 0 {
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

// TestSetRangeKeysSkipsExpired checks that KEYS and SCAN never surface a set key
// that has lazily expired, matching EXISTS.
func TestSetRangeKeysSkipsExpired(t *testing.T) {
	cx := setExpireCtx(100)
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
