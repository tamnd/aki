package btree

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newTree creates a pager on an in-memory file and returns a fresh tree. A
// small page size makes splits happen with a handful of keys, so the multi-level
// paths get exercised without inserting thousands of entries.
func newTree(t *testing.T, pageSize uint32) (*Tree, *pager.Pager) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.aki", pager.Options{PageSize: pageSize})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	tr, err := Create(p)
	if err != nil {
		t.Fatalf("create tree: %v", err)
	}
	return tr, p
}

func TestGetMissingOnEmpty(t *testing.T) {
	tr, _ := newTree(t, 0)
	if _, ok, err := tr.Get([]byte("nope")); err != nil || ok {
		t.Fatalf("Get on empty = ok %v err %v", ok, err)
	}
}

func TestPutGet(t *testing.T) {
	tr, _ := newTree(t, 0)
	if err := tr.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatal(err)
	}
	v, ok, err := tr.Get([]byte("foo"))
	if err != nil || !ok || string(v) != "bar" {
		t.Fatalf("Get foo = %q ok %v err %v", v, ok, err)
	}
}

func TestPutReplace(t *testing.T) {
	tr, _ := newTree(t, 0)
	_ = tr.Put([]byte("k"), []byte("v1"))
	_ = tr.Put([]byte("k"), []byte("v2"))
	v, _, _ := tr.Get([]byte("k"))
	if string(v) != "v2" {
		t.Fatalf("replace = %q want v2", v)
	}
}

func TestDelete(t *testing.T) {
	tr, _ := newTree(t, 0)
	_ = tr.Put([]byte("a"), []byte("1"))
	_ = tr.Put([]byte("b"), []byte("2"))
	ok, err := tr.Delete([]byte("a"))
	if err != nil || !ok {
		t.Fatalf("Delete a = %v %v", ok, err)
	}
	if _, ok, _ := tr.Get([]byte("a")); ok {
		t.Fatal("a still present after delete")
	}
	if v, ok, _ := tr.Get([]byte("b")); !ok || string(v) != "2" {
		t.Fatalf("b after delete = %q ok %v", v, ok)
	}
	if ok, _ := tr.Delete([]byte("a")); ok {
		t.Fatal("second delete of a returned true")
	}
}

// TestManyKeysForceSplits inserts enough keys to split the root several times on
// a small page, then reads every key back and confirms a full in-order scan.
func TestManyKeysForceSplits(t *testing.T) {
	tr, _ := newTree(t, 4096)
	const n = 2000
	keys := make([]string, n)
	for i := range n {
		k := fmt.Sprintf("key:%06d", i)
		keys[i] = k
		if err := tr.Put([]byte(k), fmt.Appendf(nil, "val:%d", i)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	if tr.Root() == 0 {
		t.Fatal("root is page 0")
	}

	for i, k := range keys {
		v, ok, err := tr.Get([]byte(k))
		if err != nil || !ok {
			t.Fatalf("Get %s = ok %v err %v", k, ok, err)
		}
		if want := fmt.Sprintf("val:%d", i); string(v) != want {
			t.Fatalf("Get %s = %q want %q", k, v, want)
		}
	}

	// A full cursor scan must return every key once, in sorted order.
	sort.Strings(keys)
	c := tr.Cursor()
	if err := c.First(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for c.Valid() {
		got = append(got, string(c.Key()))
		if err := c.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != len(keys) {
		t.Fatalf("scan returned %d keys want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("scan order at %d: %q want %q", i, got[i], keys[i])
		}
	}
}

func TestSeek(t *testing.T) {
	tr, _ := newTree(t, 4096)
	for i := 0; i < 100; i += 2 { // even keys only: 0,2,4,...
		_ = tr.Put(fmt.Appendf(nil, "k%03d", i), []byte("x"))
	}
	c := tr.Cursor()
	// Seeking an odd key lands on the next even key.
	if err := c.Seek([]byte("k001")); err != nil {
		t.Fatal(err)
	}
	if !c.Valid() || string(c.Key()) != "k002" {
		t.Fatalf("seek k001 -> %q", c.Key())
	}
	// Seeking past the end leaves the cursor invalid.
	if err := c.Seek([]byte("k999")); err != nil {
		t.Fatal(err)
	}
	if c.Valid() {
		t.Fatalf("seek past end is valid at %q", c.Key())
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "p.aki", pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	tr, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		_ = tr.Put(fmt.Appendf(nil, "k%04d", i), fmt.Appendf(nil, "v%d", i))
	}
	root := tr.Root()
	if err := p.Commit(pager.CommitInfo{}); err != nil {
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
	tr2 := Open(p2, root)
	for i := range 500 {
		v, ok, err := tr2.Get(fmt.Appendf(nil, "k%04d", i))
		if err != nil || !ok {
			t.Fatalf("after reopen Get k%04d = ok %v err %v", i, ok, err)
		}
		if want := fmt.Sprintf("v%d", i); string(v) != want {
			t.Fatalf("after reopen k%04d = %q want %q", i, v, want)
		}
	}
}

func TestBinarySafeKeys(t *testing.T) {
	tr, _ := newTree(t, 0)
	keys := [][]byte{
		{0x00},
		{0x00, 0x01},
		{0xff, 0x00, 0xff},
		[]byte("with\x00null"),
	}
	for i, k := range keys {
		if err := tr.Put(k, []byte{byte(i)}); err != nil {
			t.Fatalf("Put %x: %v", k, err)
		}
	}
	for i, k := range keys {
		v, ok, err := tr.Get(k)
		if err != nil || !ok || len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("Get %x = %x ok %v err %v", k, v, ok, err)
		}
	}
}

func TestCellTooLarge(t *testing.T) {
	tr, _ := newTree(t, 4096)
	// A value larger than a page cannot be stored inline.
	big := bytes.Repeat([]byte("x"), 5000)
	if err := tr.Put([]byte("k"), big); err == nil {
		t.Fatal("expected ErrCellTooLarge for oversized value")
	}
}

// TestInPlaceUpsertSameLength checks the cheapest in-place path: replacing a
// value with one of the same length overwrites the bytes where they sit and
// returns the old value, without disturbing any other key.
func TestInPlaceUpsertSameLength(t *testing.T) {
	tr, _ := newTree(t, 4096)
	_ = tr.Put([]byte("a"), []byte("111"))
	_ = tr.Put([]byte("b"), []byte("222"))
	old, err := tr.Upsert([]byte("a"), []byte("999"))
	if err != nil || string(old) != "111" {
		t.Fatalf("Upsert a = old %q err %v want old 111", old, err)
	}
	if v, _, _ := tr.Get([]byte("a")); string(v) != "999" {
		t.Fatalf("a after same-length upsert = %q want 999", v)
	}
	if v, _, _ := tr.Get([]byte("b")); string(v) != "222" {
		t.Fatalf("neighbour b corrupted = %q want 222", v)
	}
}

// TestInPlaceUpsertGrowShrink hammers one key with values that grow and shrink,
// which forces the fresh-cell-in-the-gap path and accumulates dead space until
// the page falls back to a full re-encode that compacts it. The value must read
// back correctly the whole way through, and a neighbour key must stay intact.
func TestInPlaceUpsertGrowShrink(t *testing.T) {
	tr, _ := newTree(t, 4096)
	_ = tr.Put([]byte("guard"), []byte("keepme"))
	for i := range 4000 {
		val := bytes.Repeat([]byte{byte('a' + i%26)}, 1+i%200)
		old, err := tr.Upsert([]byte("k"), val)
		if err != nil {
			t.Fatalf("Upsert iter %d: %v", i, err)
		}
		_ = old
		got, ok, err := tr.Get([]byte("k"))
		if err != nil || !ok || !bytes.Equal(got, val) {
			t.Fatalf("iter %d Get k = %q ok %v err %v want %q", i, got, ok, err, val)
		}
		if g, _, _ := tr.Get([]byte("guard")); string(g) != "keepme" {
			t.Fatalf("iter %d guard corrupted = %q", i, g)
		}
	}
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("invariants after grow/shrink: %v", err)
	}
}

// TestInPlaceUpsertAgainstOracle drives a mix of inserts and value-changing
// updates with varied sizes against a Go map oracle. It exercises the new-key
// insert path, both update paths, and the fallback split path, then checks every
// key reads back its last written value and the tree stays well formed.
func TestInPlaceUpsertAgainstOracle(t *testing.T) {
	tr, _ := newTree(t, 4096)
	oracle := map[string]string{}
	// A small, deterministic LCG so the run is reproducible without a seed.
	rng := uint32(2463534242)
	next := func() uint32 { rng ^= rng << 13; rng ^= rng >> 17; rng ^= rng << 5; return rng }
	for i := range 20000 {
		key := fmt.Sprintf("key:%04d", next()%1500)
		val := bytes.Repeat([]byte{byte('A' + int(next())%26)}, int(next())%120)
		old, err := tr.Upsert([]byte(key), val)
		if err != nil {
			t.Fatalf("Upsert iter %d key %q: %v", i, key, err)
		}
		prev, had := oracle[key]
		if had {
			if string(old) != prev {
				t.Fatalf("iter %d key %q old = %q want %q", i, key, old, prev)
			}
		} else if old != nil {
			t.Fatalf("iter %d key %q first write returned old %q", i, key, old)
		}
		oracle[key] = string(val)
	}
	for key, want := range oracle {
		got, ok, err := tr.Get([]byte(key))
		if err != nil || !ok || string(got) != want {
			t.Fatalf("final Get %q = %q ok %v err %v want %q", key, got, ok, err, want)
		}
	}
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("invariants after oracle run: %v", err)
	}
}
