package keyspace

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/store"
	"github.com/tamnd/aki/vfs"
)

// openHL opens an in-memory keyspace with the hybrid-log string path engaged and
// returns its database 0. The store is sized small (16 shards, 64 KiB pages) so a
// test allocates kilobytes, not the production 256 shards times 1 MiB.
func openHL(t *testing.T) *DB {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "hl.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	tun := store.Tunables{Shards: 16, PageSize: 1 << 16, IndexHintPerShard: 256}
	ks, err := Open(p, WithHybridLog(tun))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db, err := ks.DB(0)
	if err != nil {
		t.Fatalf("DB(0): %v", err)
	}
	return db
}

// TestHLSetGetRoundTrip is the core S1 contract: a value set on the hybrid-log
// path reads back with its body and the type/encoding metadata the command layer
// depends on, and DBSIZE counts it.
func TestHLSetGetRoundTrip(t *testing.T) {
	db := openHL(t)
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("key:%d", i))
		v := []byte(fmt.Sprintf("val:%d", i))
		if err := db.Set(k, v, TypeString, EncRaw, -1); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("key:%d", i))
		want := []byte(fmt.Sprintf("val:%d", i))
		body, hdr, found, err := db.Get(k)
		if err != nil || !found {
			t.Fatalf("Get %d: found=%v err=%v", i, found, err)
		}
		if !bytes.Equal(body, want) {
			t.Fatalf("Get %d body = %q, want %q", i, body, want)
		}
		if hdr.Type != TypeString || hdr.Encoding != EncRaw {
			t.Fatalf("Get %d header type=%d enc=%d, want string/raw", i, hdr.Type, hdr.Encoding)
		}
		if hdr.Version == 0 {
			t.Fatalf("Get %d header version is zero", i)
		}
	}
	if got := db.Len(); got != 2000 {
		t.Fatalf("Len = %d, want 2000", got)
	}
}

// TestHLOverwrite checks an overwrite replaces the value, bumps the version, and
// does not double-count the key.
func TestHLOverwrite(t *testing.T) {
	db := openHL(t)
	k := []byte("k")
	if err := db.Set(k, []byte("first"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("Set first: %v", err)
	}
	_, h1, _, _ := db.Get(k)
	if err := db.Set(k, []byte("second"), TypeString, EncRaw, -1); err != nil {
		t.Fatalf("Set second: %v", err)
	}
	body, h2, found, _ := db.Get(k)
	if !found || !bytes.Equal(body, []byte("second")) {
		t.Fatalf("Get after overwrite = %q found=%v, want second", body, found)
	}
	if h2.Version <= h1.Version {
		t.Fatalf("overwrite version %d did not advance past %d", h2.Version, h1.Version)
	}
	if got := db.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (overwrite double-counted)", got)
	}
}

// TestHLDelete checks delete removes the key and reports presence correctly.
func TestHLDelete(t *testing.T) {
	db := openHL(t)
	k := []byte("gone")
	db.Set(k, []byte("v"), TypeString, EncRaw, -1)
	ok, err := db.Delete(k)
	if err != nil || !ok {
		t.Fatalf("Delete present key: ok=%v err=%v", ok, err)
	}
	if _, _, found, _ := db.Get(k); found {
		t.Fatal("key still present after delete")
	}
	if ok, _ := db.Delete(k); ok {
		t.Fatal("Delete of absent key returned true")
	}
	if got := db.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
}

// TestHLExpiry checks a key with an already-past TTL is never stored, and a key
// with a future TTL carries the expiry in its header. Lazy expiry on read is
// covered by the immediate-past case through hlGet's expired branch using a TTL
// set just behind now.
func TestHLExpiry(t *testing.T) {
	db := openHL(t)
	// An already-expired TTL behaves as a delete: the key never lands.
	if err := db.Set([]byte("dead"), []byte("v"), TypeString, EncRaw, 1); err != nil {
		t.Fatalf("Set expired: %v", err)
	}
	if _, _, found, _ := db.Get([]byte("dead")); found {
		t.Fatal("key with past TTL is present")
	}
	// A future TTL is carried in the header and reported by HasTTL.
	future := nowMillis() + 60_000
	if err := db.Set([]byte("live"), []byte("v"), TypeString, EncRaw, future); err != nil {
		t.Fatalf("Set future: %v", err)
	}
	_, hdr, found, _ := db.Get([]byte("live"))
	if !found || !hdr.HasTTL() || hdr.TTLms != future {
		t.Fatalf("future-TTL key: found=%v hasTTL=%v ttl=%d want %d", found, hdr.HasTTL(), hdr.TTLms, future)
	}
}

// TestHLLazyExpiryOnRead drives the read-path lazy delete directly: a record
// whose header TTL is already in the past is written into the store (bypassing
// hlSet's write-time expiry check) so the read path is the one that must notice
// the expiry, report the key absent, and drop it from the index.
func TestHLLazyExpiryOnRead(t *testing.T) {
	db := openHL(t)
	s, err := db.ensureHL()
	if err != nil {
		t.Fatalf("ensureHL: %v", err)
	}
	// Build a cell whose header TTL is one ms in the past and write it straight to
	// the store, the state a key reaches when its TTL lapses after it was stored.
	h := ValueHeader{Type: TypeString, Encoding: EncRaw, Flags: FlagInlineBody | FlagHasTTL, TTLms: nowMillis() - 1, Version: db.ks.NextVersionForKey([]byte("x")), BodyLen: 1, RefCount: 1}
	cell := append(h.AppendTo(nil), 'v')
	if err := s.Set([]byte("x"), cell); err != nil {
		t.Fatalf("store.Set: %v", err)
	}
	if _, _, found, _ := db.Get([]byte("x")); found {
		t.Fatal("expired key readable on the read path")
	}
	// The lazy delete on read must have dropped it from the index.
	if _, ok, _ := s.Get([]byte("x")); ok {
		t.Fatal("expired key not dropped from the index by lazy read expiry")
	}
}

// TestHLDisabledByDefault confirms a keyspace opened without WithHybridLog still
// runs on the B-tree, so the gate is off unless asked for.
func TestHLDisabledByDefault(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "plain.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	defer p.Close()
	ks, err := Open(p)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db, _ := ks.DB(0)
	if db.newHL != nil {
		t.Fatal("hybrid log engaged without WithHybridLog")
	}
	if db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1); db.hl.Load() != nil {
		t.Fatal("B-tree write built a hybrid-log store")
	}
}
