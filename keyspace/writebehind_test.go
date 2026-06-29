package keyspace

import (
	"bytes"
	"testing"
)

// TestSetVersionGuardBtree proves the B-tree sink rejects a reordered older
// write. Two concurrent same-key blind SETs can reach the shard worker in
// version-reversed order, because the blind SET path assigns its version and
// stages without a per-key lock. Here that reorder is driven deterministically:
// version 2 is applied, then version 1. The hot cache (cput) is version-guarded
// already, so the regression only shows through the B-tree once the hot entry is
// gone. Invalidating the hot cache forces the read down to the tree, which must
// still return the newer value. Before the guard the tree held the version-1
// body and the read regressed; after it, the version-2 body survives.
func TestSetVersionGuardBtree(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	key := []byte("k")
	newer := []byte("v2")
	older := []byte("v1")

	if err := db.SetWithVersion(key, newer, TypeString, EncRaw, -1, 2); err != nil {
		t.Fatalf("SetWithVersion v2: %v", err)
	}
	if err := db.SetWithVersion(key, older, TypeString, EncRaw, -1, 1); err != nil {
		t.Fatalf("SetWithVersion v1: %v", err)
	}

	// Hot-cache hit: cput's own guard keeps the newer body.
	if b, _, ok := db.HotGet(key); !ok || !bytes.Equal(b, newer) {
		t.Fatalf("HotGet = %q (ok=%v), want %q", b, ok, newer)
	}

	// Force the read past the hot cache and the pending table down to the B-tree.
	db.hc.Load().cinvalidate(key)
	b, _, found, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("key missing from B-tree after reordered writes")
	}
	if !bytes.Equal(b, newer) {
		t.Fatalf("B-tree value = %q, want %q (older write regressed the tree)", b, newer)
	}
}

// TestPrepareWriteBehindVersionGuard proves the wbPending sink rejects a
// reordered older stage. A late version-1 PrepareWriteBehind must not replace a
// version-2 entry already staged, or a read that misses the hot cache would serve
// the older body from the pending table.
func TestPrepareWriteBehindVersionGuard(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	key := []byte("k")
	newer := []byte("v2")
	older := []byte("v1")
	mkHdr := func(body []byte, ver uint64) ValueHeader {
		return ValueHeader{
			Type:     TypeString,
			Encoding: EncRaw,
			TTLms:    -1,
			Version:  ver,
			BodyLen:  uint32(len(body)),
			RefCount: 1,
			Flags:    FlagInlineBody,
		}
	}

	db.PrepareWriteBehind(key, newer, mkHdr(newer, 2))
	db.PrepareWriteBehind(key, older, mkHdr(older, 1))

	b, _, ok, _ := db.getWBPending(string(key))
	if !ok {
		t.Fatal("wbPending entry missing")
	}
	if !bytes.Equal(b, newer) {
		t.Fatalf("wbPending value = %q, want %q (older stage regressed the table)", b, newer)
	}
	// The reordered stage must not have inflated the provisional key count.
	if got := db.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (reordered stage double-counted)", got)
	}
}

// TestWriteBehindExpiredApplyClearsPending reproduces the DBSIZE drift that hit
// short-TTL keys under the deferred (commitEverySec) write-behind path. The
// engine stages a SET with PrepareWriteBehind, which bumps the per-shard
// pendingUncertain counter so Len() counts the key before the async B-tree write
// lands. The write worker then calls SetWithVersion. When the key was set with a
// very short TTL (SET k v PX 1) and that TTL elapses before the worker runs,
// set takes its write-time-expiry branch: it deletes the key and returns. The
// bug was that this branch skipped removeWBPending, so the wbPending entry and
// its pendingUncertain increment stayed live, and Len()/DBSIZE over-reported the
// gone key forever.
func TestWriteBehindExpiredApplyClearsPending(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	key := []byte("vol")
	body := []byte("v")
	// Stage the write the way updateWriteBehind does: a fresh version, an inline
	// header, and an absolute TTL that is already in the past at apply time.
	version := ks.NextVersionForKey(key)
	pastTTL := nowMillis() - 1
	hdr := ValueHeader{
		Type:     TypeString,
		Encoding: EncRaw,
		TTLms:    pastTTL,
		Version:  version,
		BodyLen:  uint32(len(body)),
		RefCount: 1,
		Flags:    FlagInlineBody | FlagHasTTL,
	}
	db.PrepareWriteBehind(key, body, hdr)

	// The staged key is provisionally counted until the write worker applies it.
	if got := db.Len(); got != 1 {
		t.Fatalf("Len after PrepareWriteBehind = %d, want 1", got)
	}

	// The async worker applies the B-tree write. The TTL has already elapsed, so
	// set takes the expiry branch instead of writing the key.
	if err := db.SetWithVersion(key, body, TypeString, EncRaw, pastTTL, version); err != nil {
		t.Fatalf("SetWithVersion: %v", err)
	}

	// The key is gone and the provisional count must have been cleared.
	if got := db.Len(); got != 0 {
		t.Fatalf("Len after expired apply = %d, want 0 (pendingUncertain leaked)", got)
	}
	exists, err := db.Exists(key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("expired key still reports as existing")
	}
}

// TestWriteBehindNormalApplyClearsPending is the non-expiring counterpart: a
// staged write with no TTL must leave Len() at exactly 1 after the worker
// applies it, proving the fix does not double-count or under-count the ordinary
// path that already worked.
func TestWriteBehindNormalApplyClearsPending(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	key := []byte("live")
	body := []byte("v")
	version := ks.NextVersionForKey(key)
	hdr := ValueHeader{
		Type:     TypeString,
		Encoding: EncRaw,
		TTLms:    -1,
		Version:  version,
		BodyLen:  uint32(len(body)),
		RefCount: 1,
		Flags:    FlagInlineBody,
	}
	db.PrepareWriteBehind(key, body, hdr)
	if got := db.Len(); got != 1 {
		t.Fatalf("Len after PrepareWriteBehind = %d, want 1", got)
	}
	if err := db.SetWithVersion(key, body, TypeString, EncRaw, -1, version); err != nil {
		t.Fatalf("SetWithVersion: %v", err)
	}
	if got := db.Len(); got != 1 {
		t.Fatalf("Len after normal apply = %d, want 1", got)
	}
}
