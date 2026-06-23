package keyspace

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newKS creates a pager on an in-memory file and opens a keyspace over it.
func newKS(t *testing.T) (*Keyspace, *pager.Pager, vfs.VFS) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	ks, err := Open(p)
	if err != nil {
		t.Fatalf("open keyspace: %v", err)
	}
	return ks, p, fs
}

func mustDB(t *testing.T, ks *Keyspace, i int) *DB {
	t.Helper()
	db, err := ks.DB(i)
	if err != nil {
		t.Fatalf("DB(%d): %v", i, err)
	}
	return db
}

func TestSetGet(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if err := db.Set([]byte("foo"), []byte("bar"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	body, hdr, found, err := db.Get([]byte("foo"))
	if err != nil || !found {
		t.Fatalf("Get foo = found %v err %v", found, err)
	}
	if string(body) != "bar" {
		t.Fatalf("body = %q want bar", body)
	}
	if hdr.Type != TypeString || hdr.Encoding != EncRaw {
		t.Fatalf("hdr type/enc = %d/%d", hdr.Type, hdr.Encoding)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d want 1", db.Len())
	}
}

func TestGetMissing(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if _, _, found, err := db.Get([]byte("nope")); err != nil || found {
		t.Fatalf("Get on empty db = found %v err %v", found, err)
	}
}

func TestOverwriteKeepsCount(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	_ = db.Set([]byte("k"), []byte("v1"), TypeString, EncRaw, -1)
	_ = db.Set([]byte("k"), []byte("v2"), TypeString, EncRaw, -1)
	body, _, _, _ := db.Get([]byte("k"))
	if string(body) != "v2" {
		t.Fatalf("body = %q want v2", body)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d want 1 after overwrite", db.Len())
	}
}

func TestDelete(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	_ = db.Set([]byte("a"), []byte("1"), TypeString, EncRaw, -1)
	ok, err := db.Delete([]byte("a"))
	if err != nil || !ok {
		t.Fatalf("Delete a = %v %v", ok, err)
	}
	if _, _, found, _ := db.Get([]byte("a")); found {
		t.Fatal("a present after delete")
	}
	if db.Len() != 0 {
		t.Fatalf("Len = %d want 0", db.Len())
	}
	if ok, _ := db.Delete([]byte("a")); ok {
		t.Fatal("second delete returned true")
	}
}

func TestDatabasesAreSeparate(t *testing.T) {
	ks, _, _ := newKS(t)
	db0 := mustDB(t, ks, 0)
	db1 := mustDB(t, ks, 1)
	_ = db0.Set([]byte("k"), []byte("in0"), TypeString, EncRaw, -1)
	_ = db1.Set([]byte("k"), []byte("in1"), TypeString, EncRaw, -1)
	b0, _, _, _ := db0.Get([]byte("k"))
	b1, _, _, _ := db1.Get([]byte("k"))
	if string(b0) != "in0" || string(b1) != "in1" {
		t.Fatalf("db0=%q db1=%q want in0/in1", b0, b1)
	}
}

func TestDBRange(t *testing.T) {
	ks, _, _ := newKS(t)
	if _, err := ks.DB(16); err == nil {
		t.Fatal("DB(16) on a 16-db keyspace should fail")
	}
	if _, err := ks.DB(-1); err == nil {
		t.Fatal("DB(-1) should fail")
	}
}

func TestLazyExpiry(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	clock := int64(1000)
	old := nowMillis
	nowMillis = func() int64 { return clock }
	defer func() { nowMillis = old }()

	// Expires at t=2000.
	if err := db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, 2000); err != nil {
		t.Fatal(err)
	}
	if _, _, found, _ := db.Get([]byte("k")); !found {
		t.Fatal("key should be live before expiry")
	}
	if db.totalExpireCount() != 1 {
		t.Fatalf("expireCount = %d want 1", db.totalExpireCount())
	}

	clock = 2000 // now at/after expiry
	if _, _, found, _ := db.Get([]byte("k")); found {
		t.Fatal("key should be expired")
	}
	// Get returns not-found immediately; the B-tree entry is cleaned up by
	// the next active expiry cycle to keep Get free of write operations.
	if _, err := ks.ActiveExpireCycle(); err != nil {
		t.Fatalf("expire cycle: %v", err)
	}
	if db.Len() != 0 {
		t.Fatalf("Len = %d want 0 after expiry cycle", db.Len())
	}
	if db.totalExpireCount() != 0 {
		t.Fatalf("expireCount = %d want 0 after expiry cycle", db.totalExpireCount())
	}
}

func TestSetInPastDeletes(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	clock := int64(5000)
	old := nowMillis
	nowMillis = func() int64 { return clock }
	defer func() { nowMillis = old }()

	_ = db.Set([]byte("k"), []byte("v"), TypeString, EncRaw, -1)
	// A TTL already in the past must not store the key.
	if err := db.Set([]byte("k"), []byte("v2"), TypeString, EncRaw, 1000); err != nil {
		t.Fatal(err)
	}
	if _, _, found, _ := db.Get([]byte("k")); found {
		t.Fatal("key with past TTL should not exist")
	}
	if db.Len() != 0 {
		t.Fatalf("Len = %d want 0", db.Len())
	}
}

func TestBinarySafeKeys(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	keys := [][]byte{{0x00}, {0x00, 0x01}, []byte("a\x00b"), {0xff, 0xfe}}
	for i, k := range keys {
		if err := db.Set(k, []byte{byte(i)}, TypeString, EncRaw, -1); err != nil {
			t.Fatalf("Set %x: %v", k, err)
		}
	}
	for i, k := range keys {
		b, _, found, err := db.Get(k)
		if err != nil || !found || len(b) != 1 || b[0] != byte(i) {
			t.Fatalf("Get %x = %x found %v err %v", k, b, found, err)
		}
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "p.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	db0 := mustDB(t, ks, 0)
	db3 := mustDB(t, ks, 3)
	for i := range 300 {
		_ = db0.Set(fmt.Appendf(nil, "k%04d", i), fmt.Appendf(nil, "v%d", i), TypeString, EncRaw, -1)
	}
	_ = db3.Set([]byte("only"), []byte("here"), TypeString, EncRaw, -1)
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "p.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	rdb0 := mustDB(t, ks2, 0)
	if rdb0.Len() != 300 {
		t.Fatalf("db0 Len after reopen = %d want 300", rdb0.Len())
	}
	for i := range 300 {
		b, _, found, err := rdb0.Get(fmt.Appendf(nil, "k%04d", i))
		if err != nil || !found {
			t.Fatalf("after reopen Get k%04d = found %v err %v", i, found, err)
		}
		if want := fmt.Sprintf("v%d", i); string(b) != want {
			t.Fatalf("after reopen k%04d = %q want %q", i, b, want)
		}
	}
	rdb3 := mustDB(t, ks2, 3)
	if b, _, found, _ := rdb3.Get([]byte("only")); !found || string(b) != "here" {
		t.Fatalf("db3 only = %q found %v", b, found)
	}
}

func TestHashSlotHashTag(t *testing.T) {
	// Keys sharing a hash tag must map to the same slot.
	if HashSlot([]byte("{user}.a")) != HashSlot([]byte("{user}.b")) {
		t.Fatal("hash-tagged keys should share a slot")
	}
	// A known Redis vector: CRC16("123456789") = 0x31C3.
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16(123456789) = %#x want 0x31C3", got)
	}
	// Empty tag content is ignored; the whole key is hashed.
	if HashSlot([]byte("{}.a")) == HashSlot([]byte("{}.b")) {
		t.Fatal("empty hash tag should not group keys")
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	h := ValueHeader{
		Type: TypeHash, Encoding: EncListpack, Flags: FlagInlineBody | FlagHasTTL,
		TTLms: 1234567890, Version: 42, LRULFU: 7, BodyRef: 0, BodyLen: 99, RefCount: 1,
	}
	b := h.AppendTo(nil)
	if len(b) != HeaderSize {
		t.Fatalf("encoded len = %d want %d", len(b), HeaderSize)
	}
	got, n, ok := parseHeader(b)
	if !ok || n != HeaderSize {
		t.Fatalf("parse ok %v n %d", ok, n)
	}
	if got != h {
		t.Fatalf("round trip = %+v want %+v", got, h)
	}
}

func TestCompositeKeyExtract(t *testing.T) {
	for _, k := range []string{"", "a", "longer-key", "with\x00null"} {
		ck := compositeKey([]byte(k))
		if !bytes.Equal(rawKey(ck), []byte(k)) {
			t.Fatalf("rawKey round trip for %q = %q", k, rawKey(ck))
		}
	}
}
