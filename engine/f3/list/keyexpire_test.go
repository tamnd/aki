package list

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// keyExpireCtx builds a shard Ctx over a plain (cold-off) store with the clock at
// nowMs, the minimum a list command needs: the registry and its lazy-expiry funnel
// read cx.NowMs, and lookup consults cx.St.Exists for the WRONGTYPE probe. Each call
// makes a fresh store, so its registry is a distinct entry in the shared regs map.
func keyExpireCtx(nowMs int64) *shard.Ctx {
	return &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: nowMs}
}

// TestListLiveLazyExpiry checks that a list whose deadline has passed is dropped by
// the live funnel and reported absent, and that the drop removes it from the map so
// it is gone to every later command in the epoch.
func TestListLiveLazyExpiry(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	g.m["l"] = seedList("a", "b")
	g.m["l"].expireAt = 150

	// Before the deadline the list is live.
	if g.live(cx, []byte("l")) == nil {
		t.Fatal("list should be live before its deadline")
	}

	// At and after the deadline the list is dropped and reported absent.
	cx.NowMs = 150
	if g.live(cx, []byte("l")) != nil {
		t.Fatal("list at its deadline should be reported absent")
	}
	if _, ok := g.m["l"]; ok {
		t.Fatal("an expired list should be dropped from the map")
	}
	if Has(cx, []byte("l")) {
		t.Fatal("Has should read false for an expired list")
	}
}

// TestListExpireCreateAfterExpiry is the create-path hazard the plan calls out: a
// fresh push on an expired key must build a new list with no TTL, never resurrect
// the shadowed expired one. pushCmd's create path is `l, _ := g.lookup(...); if l
// == nil { l = newList() }`, and lookup routes through live, so this drives that
// funnel directly (a unit test cannot build the concrete shard.Reply the handler
// takes; the driver test covers the wired RPUSH end to end).
func TestListExpireCreateAfterExpiry(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	g.m["l"] = seedList("old")
	g.m["l"].expireAt = 150

	cx.NowMs = 200
	// The create funnel drops the expired list and reports absence, so the handler
	// takes its create branch instead of pushing into the stale list.
	if l, _ := g.lookup(cx, []byte("l")); l != nil {
		t.Fatal("create funnel should drop the expired list and report absent")
	}
	if _, ok := g.m["l"]; ok {
		t.Fatal("expired list should be gone from the map before the create")
	}
	// pushCmd then builds a fresh list from the new element; it carries no deadline
	// and none of the old elements.
	l := newList()
	g.m["l"] = l
	l.pushBack([]byte("new"))
	if l.expireAt != 0 {
		t.Fatalf("recreated list should carry no deadline, got %d", l.expireAt)
	}
	if l.length() != 1 || string(l.inlineAt(0)) != "new" {
		t.Fatal("recreated list should hold only the new element")
	}
}

// TestListDeadlinePersist exercises the Deadline and Persist backends the unified
// TTL and PERSIST handlers drive.
func TestListDeadlinePersist(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	g.m["l"] = seedList("a")

	// A live list with no deadline reports at==0, ok==true.
	if at, ok := Deadline(cx, []byte("l")); !ok || at != 0 {
		t.Fatalf("no-TTL list: got (%d,%v), want (0,true)", at, ok)
	}
	// Persist on a list with no deadline removes nothing.
	if Persist(cx, []byte("l")) {
		t.Fatal("Persist on a no-TTL list should report false")
	}

	g.m["l"].expireAt = 500
	if at, ok := Deadline(cx, []byte("l")); !ok || at != 500 {
		t.Fatalf("TTL list: got (%d,%v), want (500,true)", at, ok)
	}
	if !Persist(cx, []byte("l")) {
		t.Fatal("Persist on a list with a deadline should report true")
	}
	if at, _ := Deadline(cx, []byte("l")); at != 0 {
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

// TestListRangeKeysSkipsExpired checks that KEYS and SCAN never surface a list key
// that has lazily expired, matching EXISTS.
func TestListRangeKeysSkipsExpired(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	g.m["live"] = seedList("a")
	g.m["dead"] = seedList("a")
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
