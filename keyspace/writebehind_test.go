package keyspace

import "testing"

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
	version := ks.NextVersion()
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
	version := ks.NextVersion()
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
