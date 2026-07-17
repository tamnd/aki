package hash

import (
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// keyExpireCtx builds a shard Ctx over a plain (cold-off) store with the clock at
// nowMs. Each call makes a fresh store, so its hash registry (keyed off the store
// pointer in the shared regs map) is distinct from every other test's.
func keyExpireCtx(nowMs int64) *shard.Ctx {
	return &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: nowMs}
}

// TestHashKeyLiveLazyExpiry checks that a hash whose key-level deadline has passed
// is dropped whole by the live funnel and reported absent, fields and all, and that
// the drop removes it from the map so it is gone to every later command.
func TestHashKeyLiveLazyExpiry(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	setKey(g, "h", "f", "v", "g", "w")
	g.m["h"].expireAt = 150

	// Before the deadline the hash is live.
	if g.live(cx, []byte("h")) == nil {
		t.Fatal("hash should be live before its key deadline")
	}

	// At and after the deadline the hash is dropped and reported absent.
	cx.NowMs = 150
	if g.live(cx, []byte("h")) != nil {
		t.Fatal("hash at its key deadline should be reported absent")
	}
	if _, ok := g.m["h"]; ok {
		t.Fatal("an expired hash should be dropped from the map")
	}
	if Has(cx, []byte("h")) {
		t.Fatal("Has should read false for a key-expired hash")
	}
}

// TestHashKeyExpireCreateAfterExpiry is the create-path hazard: a fresh write on a
// key-expired hash must build a new hash with no key TTL, never resurrect the
// shadowed one. getOrCreate routes through g.live, so this drives that funnel
// directly (a unit test cannot build the concrete shard.Reply the handler takes;
// the driver test covers the wired HSET end to end).
func TestHashKeyExpireCreateAfterExpiry(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	setKey(g, "h", "old", "v")
	g.m["h"].expireAt = 150

	cx.NowMs = 200
	// The create funnel drops the expired hash and reports absence, so getOrCreate
	// takes its create branch instead of writing into the stale hash.
	if g.live(cx, []byte("h")) != nil {
		t.Fatal("create funnel should drop the key-expired hash and report absent")
	}
	if _, ok := g.m["h"]; ok {
		t.Fatal("expired hash should be gone from the map before the create")
	}
	// getOrCreate then builds a fresh hash for the new field; it carries no key TTL
	// and none of the old fields.
	_, h, wrong := getOrCreate(cx, []byte("h"))
	if wrong || h == nil {
		t.Fatal("getOrCreate should build a fresh hash after expiry")
	}
	h.set([]byte("new"), []byte("v"))
	if h.expireAt != 0 {
		t.Fatalf("recreated hash should carry no key deadline, got %d", h.expireAt)
	}
	if !h.has([]byte("new")) || h.has([]byte("old")) {
		t.Fatal("recreated hash should hold only the new field")
	}
}

// TestHashKeyDeadlinePersist exercises the Deadline and Persist backends the
// unified TTL and PERSIST handlers drive, and confirms the key-level Persist is
// distinct from a field TTL: it clears the key deadline only.
func TestHashKeyDeadlinePersist(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	setKey(g, "h", "f", "v")

	// A live hash with no key deadline reports at==0, ok==true.
	if at, ok := Deadline(cx, []byte("h")); !ok || at != 0 {
		t.Fatalf("no-TTL hash: got (%d,%v), want (0,true)", at, ok)
	}
	// Persist on a hash with no key deadline removes nothing.
	if Persist(cx, []byte("h")) {
		t.Fatal("Persist on a no-TTL hash should report false")
	}

	g.m["h"].expireAt = 500
	if at, ok := Deadline(cx, []byte("h")); !ok || at != 500 {
		t.Fatalf("TTL hash: got (%d,%v), want (500,true)", at, ok)
	}
	if !Persist(cx, []byte("h")) {
		t.Fatal("Persist on a hash with a key deadline should report true")
	}
	if at, _ := Deadline(cx, []byte("h")); at != 0 {
		t.Fatalf("key deadline should be cleared, got %d", at)
	}

	// An absent key reports not present from both.
	if _, ok := Deadline(cx, []byte("missing")); ok {
		t.Fatal("absent key should report not present")
	}
	if Persist(cx, []byte("missing")) {
		t.Fatal("Persist on an absent key should report false")
	}
}

// TestHashKeyRangeKeysSkipsExpired checks that KEYS and SCAN never surface a hash
// key whose key-level deadline has lazily passed, matching EXISTS.
func TestHashKeyRangeKeysSkipsExpired(t *testing.T) {
	cx := keyExpireCtx(100)
	g := registry(cx)
	setKey(g, "live", "f", "v")
	setKey(g, "dead", "f", "v")
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
