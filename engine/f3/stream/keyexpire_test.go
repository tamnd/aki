package stream

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// keyExpireCtx builds a shard Ctx over a plain (cold-off) store with the clock at
// nowMs, the minimum a stream command needs: the registry and its lazy-expiry funnel
// read cx.NowMs, and lookup consults cx.St.Exists for the WRONGTYPE probe. Each call
// makes a fresh store, so its registry is a distinct entry in the shared regs map.
func keyExpireCtx(nowMs int64) *shard.Ctx {
	return &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: nowMs}
}

// TestStreamLiveLazyExpiry checks that a stream whose deadline has passed is dropped
// by the live funnel and reported absent, and that the drop removes it from the map
// so it is gone to every later command in the epoch. This is the one path besides
// DEL that takes a stream out of the map: an emptied stream is otherwise kept.
func TestStreamLiveLazyExpiry(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	addEntry(g, "st", 1, "f", "v")
	g.m["st"].expireAt = 150

	// Before the deadline the stream is live.
	if g.live(cx, []byte("st")) == nil {
		t.Fatal("stream should be live before its deadline")
	}

	// At and after the deadline the stream is dropped and reported absent.
	cx.NowMs = 150
	if g.live(cx, []byte("st")) != nil {
		t.Fatal("stream at its deadline should be reported absent")
	}
	if _, ok := g.m["st"]; ok {
		t.Fatal("an expired stream should be dropped from the map")
	}
	if Has(cx, []byte("st")) {
		t.Fatal("Has should read false for an expired stream")
	}
}

// TestStreamExpireCreateAfterExpiry is the create-path hazard the plan calls out: a
// fresh XADD on an expired key must build a new stream with no TTL, never resurrect
// the shadowed expired one. Xadd's create path is `s, _ := g.lookup(...); if s ==
// nil { s = newStream() }`, and lookup routes through live, so this drives that
// funnel directly (a unit test cannot build the concrete shard.Reply the handler
// takes; the driver test covers the wired XADD end to end).
func TestStreamExpireCreateAfterExpiry(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	addEntry(g, "st", 1, "old", "v")
	g.m["st"].expireAt = 150

	cx.NowMs = 200
	// The create funnel drops the expired stream and reports absence, so the handler
	// takes its create branch instead of appending into the stale stream.
	if s, _ := g.lookup(cx, []byte("st")); s != nil {
		t.Fatal("create funnel should drop the expired stream and report absent")
	}
	if _, ok := g.m["st"]; ok {
		t.Fatal("expired stream should be gone from the map before the create")
	}
	// Xadd then builds a fresh stream from the new entry; it carries no deadline and
	// none of the old entries.
	s := addEntry(g, "st", 5, "new", "v")
	if s.expireAt != 0 {
		t.Fatalf("recreated stream should carry no deadline, got %d", s.expireAt)
	}
	if s.length != 1 {
		t.Fatalf("recreated stream should hold only the new entry, got length %d", s.length)
	}
}

// TestStreamDeadlinePersist exercises the Deadline and Persist backends the unified
// TTL and PERSIST handlers drive.
func TestStreamDeadlinePersist(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	addEntry(g, "st", 1, "f", "v")

	// A live stream with no deadline reports at==0, ok==true.
	if at, ok := Deadline(cx, []byte("st")); !ok || at != 0 {
		t.Fatalf("no-TTL stream: got (%d,%v), want (0,true)", at, ok)
	}
	// Persist on a stream with no deadline removes nothing.
	if Persist(cx, []byte("st")) {
		t.Fatal("Persist on a no-TTL stream should report false")
	}

	g.m["st"].expireAt = 500
	if at, ok := Deadline(cx, []byte("st")); !ok || at != 500 {
		t.Fatalf("TTL stream: got (%d,%v), want (500,true)", at, ok)
	}
	if !Persist(cx, []byte("st")) {
		t.Fatal("Persist on a stream with a deadline should report true")
	}
	if at, _ := Deadline(cx, []byte("st")); at != 0 {
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

// TestStreamRangeKeysSkipsExpired checks that KEYS and SCAN never surface a stream
// key that has lazily expired, matching EXISTS.
func TestStreamRangeKeysSkipsExpired(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	addEntry(g, "live", 1, "f", "v")
	addEntry(g, "dead", 1, "f", "v")
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

// TestStreamExpiredDeleteReportsAbsent checks that the key-level TTL and DEL share
// the drop path: an expired stream that DEL lands on reports already-gone (false),
// since the live funnel dropped it first.
func TestStreamExpiredDeleteReportsAbsent(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	addEntry(g, "st", 1, "f", "v")
	g.m["st"].expireAt = 150

	cx.NowMs = 200
	if Delete(cx, []byte("st")) {
		t.Fatal("DEL on an expired stream should report it already absent")
	}
	if _, ok := g.m["st"]; ok {
		t.Fatal("the expired stream should be gone from the map")
	}
}
