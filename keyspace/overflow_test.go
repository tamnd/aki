package keyspace

import (
	"bytes"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// bigBody builds a deterministic body of n bytes that does not repeat in a way
// that would hide a chunk-boundary bug.
func bigBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

func TestOverflowRoundTrip(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	// A body well past a single 4096-byte page forces a multi-page chain.
	want := bigBody(20000)
	if err := db.Set([]byte("k"), want, TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	got, hdr, found, err := db.Get([]byte("k"))
	if err != nil || !found {
		t.Fatalf("Get = found %v err %v", found, err)
	}
	if hdr.Flags&FlagInlineBody != 0 {
		t.Fatal("large body should not be inline")
	}
	if hdr.BodyRef == 0 {
		t.Fatal("BodyRef should point at the overflow head")
	}
	if hdr.BodyLen != uint32(len(want)) {
		t.Fatalf("BodyLen = %d want %d", hdr.BodyLen, len(want))
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body mismatch: got %d bytes want %d", len(got), len(want))
	}
}

func TestInlineThreshold(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	// 128 bytes is the largest inline body; 129 spills.
	if err := db.Set([]byte("small"), bigBody(maxInlineBody), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	_, h, _, _ := db.Get([]byte("small"))
	if h.Flags&FlagInlineBody == 0 {
		t.Fatal("128-byte body should be inline")
	}
	if err := db.Set([]byte("big"), bigBody(maxInlineBody+1), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	_, h, _, _ = db.Get([]byte("big"))
	if h.Flags&FlagInlineBody != 0 {
		t.Fatal("129-byte body should overflow")
	}
}

func TestOverflowOverwriteFreesOldChain(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if err := db.Set([]byte("k"), bigBody(20000), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	before := p.FreeCount()
	// Overwriting with another large body must release the first chain's pages.
	if err := db.Set([]byte("k"), bigBody(20000), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if got := p.FreeCount(); got <= before {
		t.Fatalf("free count did not grow: before %d after %d", before, got)
	}
	body, _, found, _ := db.Get([]byte("k"))
	if !found || len(body) != 20000 {
		t.Fatalf("after overwrite len = %d found %v", len(body), found)
	}
}

func TestOverflowDeleteFreesChain(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if err := db.Set([]byte("k"), bigBody(20000), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	before := p.FreeCount()
	ok, err := db.Delete([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("Delete = %v %v", ok, err)
	}
	if got := p.FreeCount(); got <= before {
		t.Fatalf("delete did not free chain: before %d after %d", before, got)
	}
}

func TestOverflowShrinkToInlineFreesChain(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if err := db.Set([]byte("k"), bigBody(20000), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	before := p.FreeCount()
	// Replacing a large value with a tiny one frees the overflow pages.
	if err := db.Set([]byte("k"), []byte("tiny"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if got := p.FreeCount(); got <= before {
		t.Fatalf("shrink did not free chain: before %d after %d", before, got)
	}
	body, h, _, _ := db.Get([]byte("k"))
	if h.Flags&FlagInlineBody == 0 || string(body) != "tiny" {
		t.Fatalf("after shrink body = %q inline=%v", body, h.Flags&FlagInlineBody != 0)
	}
}

func TestOverflowPersistAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "ov.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatal(err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	db := mustDB(t, ks, 0)
	want := bigBody(50000)
	if err := db.Set([]byte("k"), want, TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p2, err := pager.Open(fs, "ov.aki", pager.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatal(err)
	}
	db2 := mustDB(t, ks2, 0)
	got, _, found, err := db2.Get([]byte("k"))
	if err != nil || !found {
		t.Fatalf("after reopen found %v err %v", found, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("after reopen body mismatch: got %d want %d", len(got), len(want))
	}
}

func TestOverflowExactPageMultiple(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	// A body that is an exact multiple of the per-page chunk capacity exercises
	// the boundary where the last page is completely full.
	cap := ks.ovChunkCap()
	want := bigBody(cap * 3)
	if err := db.Set([]byte("k"), want, TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	got, _, found, err := db.Get([]byte("k"))
	if err != nil || !found {
		t.Fatalf("Get = found %v err %v", found, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body mismatch: got %d want %d", len(got), len(want))
	}
}
