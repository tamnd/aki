package keyspace

import (
	"fmt"
	"testing"
)

// TestHybridCollRoundTrip writes a tree-form collection through CollUpdate and reads
// it back through CollRead, checking the rows survive and iterate in sorted order,
// the way the btree sub-tree did.
func TestHybridCollRoundTrip(t *testing.T) {
	db := openHL(t)
	key := []byte("h")
	rows := map[string]string{"b": "2", "a": "1", "c": "3", "delta": "4"}
	if err := db.CollUpdate(key, TypeHash, EncHashtable, func(w *CollWriter) error {
		for k, v := range rows {
			if _, e := w.Put([]byte(k), []byte(v)); e != nil {
				return e
			}
		}
		w.SetCount(uint64(len(rows)))
		return nil
	}); err != nil {
		t.Fatalf("CollUpdate: %v", err)
	}

	// The metadata header must report the coll form so the command layer routes here.
	h, found, err := db.CollMetaHeader(key)
	if err != nil || !found {
		t.Fatalf("CollMetaHeader found=%v err=%v", found, err)
	}
	if !h.IsColl() || h.Type != TypeHash || h.Encoding != EncHashtable {
		t.Fatalf("header coll=%v type=%d enc=%d, want hash/hashtable coll", h.IsColl(), h.Type, h.Encoding)
	}

	var order []string
	got := map[string]string{}
	ok, err := db.CollRead(key, func(r *CollReader) error {
		if r.Count() != uint64(len(rows)) {
			t.Fatalf("Count = %d, want %d", r.Count(), len(rows))
		}
		c := r.Cursor()
		for e := c.First(); c.Valid(); e = c.Next() {
			if e != nil {
				return e
			}
			order = append(order, string(c.Key()))
			got[string(c.Key())] = string(c.Value())
		}
		// Point Get must find a present and an absent subkey.
		if v, present, _ := r.Get([]byte("a")); !present || string(v) != "1" {
			t.Fatalf("Get a = %q present=%v, want 1", v, present)
		}
		if _, present, _ := r.Get([]byte("zzz")); present {
			t.Fatal("Get zzz present, want absent")
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("CollRead ok=%v err=%v", ok, err)
	}
	for k, v := range rows {
		if got[k] != v {
			t.Fatalf("row %q = %q, want %q", k, got[k], v)
		}
	}
	want := []string{"a", "b", "c", "delta"}
	if !sliceEq(order, want) {
		t.Fatalf("cursor order = %v, want %v (sorted)", order, want)
	}
}

// TestHybridCollSeekAndUpdate checks a second CollUpdate continues the existing
// collection (read-modify-write), Delete drops a row, and Seek positions a cursor.
func TestHybridCollSeekAndUpdate(t *testing.T) {
	db := openHL(t)
	key := []byte("z")
	seed := []string{"m1", "m3", "m5", "m7"}
	if err := db.CollUpdate(key, TypeSet, EncHashtable, func(w *CollWriter) error {
		for _, m := range seed {
			w.Put([]byte(m), []byte{})
		}
		w.SetCount(uint64(len(seed)))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Continue the set: add one, remove one.
	if err := db.CollUpdate(key, TypeSet, EncHashtable, func(w *CollWriter) error {
		if v, present, _ := w.Get([]byte("m3")); !present || len(v) != 0 {
			t.Fatalf("Get m3 present=%v", present)
		}
		if created, _ := w.Put([]byte("m4"), []byte{}); !created {
			t.Fatal("Put m4 created=false, want true")
		}
		if removed, _ := w.Delete([]byte("m5")); !removed {
			t.Fatal("Delete m5 removed=false, want true")
		}
		w.SetCount(w.Count() + 1 - 1)
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	var after []string
	db.CollRead(key, func(r *CollReader) error {
		c := r.Cursor()
		for c.Seek([]byte("m4")); c.Valid(); c.Next() {
			after = append(after, string(c.Key()))
		}
		return nil
	})
	if !sliceEq(after, []string{"m4", "m7"}) {
		t.Fatalf("after seek m4 = %v, want [m4 m7]", after)
	}
}

// TestHybridCollEmptyTeardown checks a collection whose count drops to zero is
// removed from the store, matching Redis deleting an emptied key.
func TestHybridCollEmptyTeardown(t *testing.T) {
	db := openHL(t)
	key := []byte("e")
	db.CollUpdate(key, TypeHash, EncHashtable, func(w *CollWriter) error {
		w.Put([]byte("only"), []byte("v"))
		w.SetCount(1)
		return nil
	})
	if n := db.Len(); n != 1 {
		t.Fatalf("Len after create = %d, want 1", n)
	}
	db.CollUpdate(key, TypeHash, EncHashtable, func(w *CollWriter) error {
		w.Delete([]byte("only"))
		w.SetCount(0)
		return nil
	})
	if n := db.Len(); n != 0 {
		t.Fatalf("Len after empty = %d, want 0 (key should be gone)", n)
	}
	if _, found, _ := db.CollMetaHeader(key); found {
		t.Fatal("emptied key still found")
	}
}

// TestHybridCollTTLAndCopy checks CollSetTTL stamps the cell in place without
// losing rows, and CollCopyTo makes an independent copy preserving rows and TTL.
func TestHybridCollTTLAndCopy(t *testing.T) {
	db := openHL(t)
	src := []byte("src")
	db.CollUpdate(src, TypeHash, EncHashtable, func(w *CollWriter) error {
		w.Put([]byte("f"), []byte("v"))
		w.SetCount(1)
		return nil
	})
	when := nowMillis() + 100000
	ok, err := db.CollSetTTL(src, when)
	if err != nil || !ok {
		t.Fatalf("CollSetTTL ok=%v err=%v", ok, err)
	}
	h, _, _ := db.CollMetaHeader(src)
	if !h.HasTTL() || h.TTLms != when {
		t.Fatalf("after SetTTL hasTTL=%v ttl=%d, want %d", h.HasTTL(), h.TTLms, when)
	}
	// Rows must survive the TTL stamp.
	db.CollRead(src, func(r *CollReader) error {
		if v, present, _ := r.Get([]byte("f")); !present || string(v) != "v" {
			t.Fatalf("after SetTTL Get f = %q present=%v", v, present)
		}
		return nil
	})

	dst := []byte("dst")
	ok, err = db.CollCopyTo(src, db, dst)
	if err != nil || !ok {
		t.Fatalf("CollCopyTo ok=%v err=%v", ok, err)
	}
	dh, found, _ := db.CollMetaHeader(dst)
	if !found || !dh.IsColl() || dh.Type != TypeHash || !dh.HasTTL() || dh.TTLms != when {
		t.Fatalf("dst header found=%v coll=%v ttl=%d, want copied with ttl %d", found, dh.IsColl(), dh.TTLms, when)
	}
	db.CollRead(dst, func(r *CollReader) error {
		if v, present, _ := r.Get([]byte("f")); !present || string(v) != "v" {
			t.Fatalf("dst Get f = %q present=%v", v, present)
		}
		return nil
	})
	// The copy is independent: mutating dst must not touch src.
	db.CollUpdate(dst, TypeHash, EncHashtable, func(w *CollWriter) error {
		w.Put([]byte("f"), []byte("changed"))
		w.SetCount(1)
		return nil
	})
	db.CollRead(src, func(r *CollReader) error {
		if v, _, _ := r.Get([]byte("f")); string(v) != "v" {
			t.Fatalf("src f = %q after dst mutation, want v (copy not independent)", v)
		}
		return nil
	})
}

// TestHybridCollManyRowsSpill exercises a collection large enough to span store
// pages, so the cell serialization survives a read-back off the log.
func TestHybridCollManyRowsSpill(t *testing.T) {
	db := openHL(t)
	key := []byte("big")
	const n = 1000
	db.CollUpdate(key, TypeZSet, EncSkiplist, func(w *CollWriter) error {
		for i := 0; i < n; i++ {
			w.Put([]byte(fmt.Sprintf("e%05d", i)), []byte(fmt.Sprintf("%d", i)))
		}
		w.SetCount(n)
		return nil
	})
	count := 0
	prev := ""
	db.CollRead(key, func(r *CollReader) error {
		c := r.Cursor()
		for e := c.First(); c.Valid(); e = c.Next() {
			if e != nil {
				return e
			}
			k := string(c.Key())
			if prev != "" && k <= prev {
				t.Fatalf("rows out of order: %q then %q", prev, k)
			}
			prev = k
			count++
		}
		return nil
	})
	if count != n {
		t.Fatalf("read back %d rows, want %d", count, n)
	}
}

func sliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
