package keyspace

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/btree"
)

// putColl is a small helper that writes member -> value rows and keeps the count
// equal to the number of distinct subkeys, the way a hash or set caller would.
func putColl(t *testing.T, db *DB, key string, pairs map[string]string) {
	t.Helper()
	err := db.CollUpdate([]byte(key), TypeHash, EncHashtable, func(w *CollWriter) error {
		for sub, val := range pairs {
			created, err := w.Put([]byte(sub), []byte(val))
			if err != nil {
				return err
			}
			if created {
				w.SetCount(w.Count() + 1)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CollUpdate %s: %v", key, err)
	}
}

func TestCollUpdateAndRead(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	putColl(t, db, "h", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	// The user key is one logical key in the main tree.
	if db.Len() != 1 {
		t.Fatalf("Len = %d want 1", db.Len())
	}

	// The header reports the btree-backed form with the requested encoding.
	h, found, err := db.CollMetaHeader([]byte("h"))
	if err != nil || !found {
		t.Fatalf("CollMetaHeader found %v err %v", found, err)
	}
	if !h.IsColl() {
		t.Fatalf("header not marked as collection: flags %08b", h.Flags)
	}
	if h.Type != TypeHash || h.Encoding != EncHashtable {
		t.Fatalf("header type/enc = %d/%d", h.Type, h.Encoding)
	}

	// CollRead sees every row and the count.
	ok, err := db.CollRead([]byte("h"), func(r *CollReader) error {
		if r.Count() != 3 {
			t.Fatalf("count = %d want 3", r.Count())
		}
		for _, sub := range []string{"f1", "f2", "f3"} {
			v, present, err := r.Get([]byte(sub))
			if err != nil {
				return err
			}
			if !present {
				t.Fatalf("missing subkey %s", sub)
			}
			if want := "v" + sub[1:]; string(v) != want {
				t.Fatalf("%s = %q want %q", sub, v, want)
			}
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("CollRead ok %v err %v", ok, err)
	}
}

func TestCollReadFallsBackForBlobAndMissing(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	// Missing key: ok=false so the caller takes its blob path.
	ok, err := db.CollRead([]byte("nope"), func(r *CollReader) error { return nil })
	if err != nil || ok {
		t.Fatalf("missing CollRead ok %v err %v", ok, err)
	}

	// A plain blob value is not btree-backed: ok=false.
	if err := db.Set([]byte("s"), []byte("plain"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	ok, err = db.CollRead([]byte("s"), func(r *CollReader) error { return nil })
	if err != nil || ok {
		t.Fatalf("blob CollRead ok %v err %v", ok, err)
	}
}

func TestCollUpdateDeletesEmptyKey(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	inUse := func() int { return int(p.PageCount()) - p.FreeCount() }

	putColl(t, db, "h", map[string]string{"f1": "v1", "f2": "v2"})
	withColl := inUse()

	// Remove every element; count hits zero so the key and its sub-tree go away.
	err := db.CollUpdate([]byte("h"), TypeHash, EncHashtable, func(w *CollWriter) error {
		for _, sub := range []string{"f1", "f2"} {
			existed, err := w.Delete([]byte(sub))
			if err != nil {
				return err
			}
			if existed {
				w.SetCount(w.Count() - 1)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("CollUpdate clear: %v", err)
	}

	if db.Len() != 0 {
		t.Fatalf("Len = %d want 0 after clearing", db.Len())
	}
	if _, found, _ := db.CollMetaHeader([]byte("h")); found {
		t.Fatal("key still present after clearing")
	}
	if got := inUse(); got >= withColl {
		t.Fatalf("clearing freed no pages: in-use %d, was %d", got, withColl)
	}
}

func TestCollOverwriteByPlainSetDropsSubTree(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	inUse := func() int { return int(p.PageCount()) - p.FreeCount() }

	// Build a chunky collection so the sub-tree has several pages.
	pairs := map[string]string{}
	for i := 0; i < 500; i++ {
		pairs[fmt.Sprintf("f%04d", i)] = fmt.Sprintf("val-%04d", i)
	}
	putColl(t, db, "h", pairs)
	withColl := inUse()
	if withColl <= 2 {
		t.Fatalf("expected a multi-page sub-tree, in-use = %d", withColl)
	}

	// Overwrite the same key with a plain string. The sub-tree must be torn down.
	if err := db.Set([]byte("h"), []byte("now a string"), TypeString, EncRaw, -1); err != nil {
		t.Fatal(err)
	}
	if got := inUse(); got >= withColl {
		t.Fatalf("overwrite freed no sub-tree pages: in-use %d, was %d", got, withColl)
	}
	body, h, found, err := db.Get([]byte("h"))
	if err != nil || !found {
		t.Fatalf("Get after overwrite found %v err %v", found, err)
	}
	if h.IsColl() || string(body) != "now a string" {
		t.Fatalf("overwrite left coll state: isColl %v body %q", h.IsColl(), body)
	}
}

func TestCollDeleteDropsSubTree(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	inUse := func() int { return int(p.PageCount()) - p.FreeCount() }
	empty := inUse()

	pairs := map[string]string{}
	for i := 0; i < 300; i++ {
		pairs[fmt.Sprintf("f%04d", i)] = fmt.Sprintf("val-%04d", i)
	}
	putColl(t, db, "h", pairs)
	if inUse() <= empty+1 {
		t.Fatal("expected sub-tree to allocate pages")
	}

	existed, err := db.Delete([]byte("h"))
	if err != nil || !existed {
		t.Fatalf("Delete existed %v err %v", existed, err)
	}
	// The main shard B-tree root page is allocated on first write and stays even
	// when the shard is empty again, so the floor is one page above the pre-write
	// baseline. Everything the collection sub-tree held must be back.
	if got := inUse(); got != empty+1 {
		t.Fatalf("after Delete in-use = %d want %d (sub-tree leaked)", got, empty+1)
	}
}

func TestCollUpdatePreservesTTL(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	// Seed the key as a coll with a TTL by writing the meta directly through a
	// first CollUpdate, then set a TTL via a plain re-set is not how callers work;
	// instead verify the round-trip preserves an existing TTL across a second op.
	deadline := NowMillis() + 100000
	err := db.CollUpdate([]byte("h"), TypeHash, EncHashtable, func(w *CollWriter) error {
		_, err := w.Put([]byte("f1"), []byte("v1"))
		w.SetCount(1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// Stamp a TTL by re-reading, rewriting the header through ExpireAt-equivalent.
	// The facility preserves whatever TTL the prior header carried, so simulate a
	// prior TTL by writing the meta header with a deadline through a raw set path.
	if err := setCollTTLForTest(db, "h", deadline); err != nil {
		t.Fatalf("seed ttl: %v", err)
	}

	// A follow-up element op must keep the TTL.
	err = db.CollUpdate([]byte("h"), TypeHash, EncHashtable, func(w *CollWriter) error {
		_, err := w.Put([]byte("f2"), []byte("v2"))
		w.SetCount(2)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	h, found, err := db.CollMetaHeader([]byte("h"))
	if err != nil || !found {
		t.Fatalf("found %v err %v", found, err)
	}
	if !h.HasTTL() || h.TTLms != deadline {
		t.Fatalf("ttl not preserved: hasTTL %v ttl %d want %d", h.HasTTL(), h.TTLms, deadline)
	}
}

// setCollTTLForTest rewrites the meta header of a btree-backed key to carry an
// absolute TTL, standing in for the command-layer EXPIRE path which is not part
// of the keyspace package.
func setCollTTLForTest(db *DB, key string, deadlineMs int64) error {
	s := ShardOf([]byte(key))
	db.shards[s].mu.Lock()
	defer db.shards[s].mu.Unlock()
	t := db.loadShardTree(s)
	ck := appendCompositeKey(nil, []byte(key))
	h, body, found, err := db.read(t, ck)
	if err != nil || !found {
		return fmt.Errorf("read meta: found %v err %v", found, err)
	}
	h.Flags |= FlagHasTTL
	h.TTLms = deadlineMs
	cell := h.AppendTo(make([]byte, 0, HeaderSize+len(body)))
	cell = append(cell, body...)
	_, err = t.Upsert(ck, cell)
	db.shards[s].rootPage = t.Root()
	db.hc.Load().cinvalidate([]byte(key))
	return err
}

// TestCollUpdateFreshTreeFreedOnError checks that a failed populate of a brand
// new collection does not leak the sub-tree it created.
func TestCollUpdateFreshTreeFreedOnError(t *testing.T) {
	ks, p, _ := newKS(t)
	db := mustDB(t, ks, 0)
	inUse := func() int { return int(p.PageCount()) - p.FreeCount() }
	empty := inUse()

	sentinel := fmt.Errorf("boom")
	err := db.CollUpdate([]byte("h"), TypeHash, EncHashtable, func(w *CollWriter) error {
		if _, perr := w.Put([]byte("f1"), []byte("v1")); perr != nil {
			return perr
		}
		w.SetCount(1)
		return sentinel
	})
	if err != sentinel {
		t.Fatalf("err = %v want sentinel", err)
	}
	// ensureShardTree allocates the main shard root before fn runs, so the floor
	// is empty+1; the fresh sub-tree we created for the failed op must be freed.
	if got := inUse(); got != empty+1 {
		t.Fatalf("fresh sub-tree leaked on error: in-use %d want %d", got, empty+1)
	}
	if _, found, _ := db.CollMetaHeader([]byte("h")); found {
		t.Fatal("key created despite error")
	}
}

// TestDropTreeRoundTrip is a guard that the btree teardown the facility relies on
// is wired to the same pager.
func TestDropTreeRoundTrip(t *testing.T) {
	ks, p, _ := newKS(t)
	tr, err := btree.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	_ = ks
	if err := tr.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := btree.DropTree(p, tr.Root()); err != nil {
		t.Fatalf("DropTree: %v", err)
	}
}
