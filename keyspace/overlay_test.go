package keyspace

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newDBTB opens a fresh single-database keyspace for a benchmark or test. It is
// the testing.TB-typed sibling of newKS/mustDB, which only accept *testing.T.
func newDBTB(tb testing.TB) *DB {
	tb.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		tb.Fatalf("create pager: %v", err)
	}
	tb.Cleanup(func() { _ = p.Close() })
	ks, err := Open(p)
	if err != nil {
		tb.Fatalf("open keyspace: %v", err)
	}
	db, err := ks.DB(0)
	if err != nil {
		tb.Fatalf("DB(0): %v", err)
	}
	return db
}

// roundtripLiveColl materializes a resident copy of key out of its sub-tree,
// hands it to mutate, then folds the result back through a CollUpdate writer. It
// stands in for the residency cycle the overlay slice will run: first touch
// reads the sub-tree once, element writes hit memory, a fold writes the delta.
func roundtripLiveColl(t *testing.T, db *DB, key string, mutate func(lc *liveColl)) {
	t.Helper()
	var lc *liveColl
	ok, err := db.CollRead([]byte(key), func(r *CollReader) error {
		var e error
		lc, e = materializeLiveColl(r, TypeHash, EncHashtable, -1, false)
		return e
	})
	if err != nil || !ok {
		t.Fatalf("materialize %s: ok=%v err=%v", key, ok, err)
	}
	mutate(lc)
	if err := db.CollUpdate([]byte(key), TypeHash, EncHashtable, lc.fold); err != nil {
		t.Fatalf("fold %s: %v", key, err)
	}
}

func TestLiveCollAbsorbAndFold(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)

	putColl(t, db, "h", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	// Absorb a new field, overwrite an existing one, delete a third. None of these
	// touch the sub-tree until the fold.
	roundtripLiveColl(t, db, "h", func(lc *liveColl) {
		if lc.count() != 3 {
			t.Fatalf("materialized count = %d want 3", lc.count())
		}
		if created := lc.put([]byte("f4"), []byte("v4")); !created {
			t.Fatalf("put f4 reported not created")
		}
		if created := lc.put([]byte("f1"), []byte("v1b")); created {
			t.Fatalf("overwrite f1 reported created")
		}
		if existed := lc.del([]byte("f2")); !existed {
			t.Fatalf("del f2 reported not existed")
		}
		if lc.count() != 3 {
			t.Fatalf("post-mutation count = %d want 3", lc.count())
		}
	})

	// The sub-tree now reflects the folded delta: f1 overwritten, f2 gone, f4 added.
	ok, err := db.CollRead([]byte("h"), func(r *CollReader) error {
		if r.Count() != 3 {
			t.Fatalf("folded count = %d want 3", r.Count())
		}
		want := map[string]string{"f1": "v1b", "f3": "v3", "f4": "v4"}
		for sub, val := range want {
			v, present, err := r.Get([]byte(sub))
			if err != nil {
				return err
			}
			if !present || string(v) != val {
				t.Fatalf("%s = %q present=%v want %q", sub, v, present, val)
			}
		}
		if _, present, _ := r.Get([]byte("f2")); present {
			t.Fatalf("f2 still present after fold")
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
}

// TestLiveCollDeltaOnly checks that a fold writes only the subkeys touched since
// the last fold, the property that makes a fold cheaper than rewriting the whole
// collection. A clean copy folds without reporting any dirty work.
func TestLiveCollDeltaOnly(t *testing.T) {
	lc := newLiveColl(TypeHash, EncHashtable, -1, false)
	lc.put([]byte("a"), []byte("1"))
	lc.put([]byte("b"), []byte("2"))
	if !lc.dirty() {
		t.Fatalf("fresh puts left copy clean")
	}
	if got := len(lc.dirtyPut); got != 2 {
		t.Fatalf("dirtyPut = %d want 2", got)
	}

	// put then del of the same subkey nets to a delete, not a put.
	lc.put([]byte("c"), []byte("3"))
	lc.del([]byte("c"))
	if _, ok := lc.dirtyPut["c"]; ok {
		t.Fatalf("c left in dirtyPut after delete")
	}
	if _, ok := lc.dirtyDel["c"]; !ok {
		t.Fatalf("c missing from dirtyDel")
	}
}

// TestLiveCollGet checks the in-memory read path returns absorbed writes before
// any fold, which is what an HGET against a resident key would consult.
func TestLiveCollGet(t *testing.T) {
	lc := newLiveColl(TypeHash, EncHashtable, -1, false)
	lc.put([]byte("k"), []byte("v"))
	if v, ok := lc.get([]byte("k")); !ok || string(v) != "v" {
		t.Fatalf("get k = %q ok=%v", v, ok)
	}
	lc.del([]byte("k"))
	if _, ok := lc.get([]byte("k")); ok {
		t.Fatalf("get k still present after del")
	}
}

// BenchmarkAbsorbVsCollUpdate is the premise check for the overlay. Each op is one
// element write against a hot, already-populated collection. The absorb arm keeps
// one resident liveColl materialized for the whole run (the steady state the
// overlay lives in) and folds once every foldEvery writes; the btree arm runs the
// live HSET path, a CollUpdate per write. foldEvery is the residency window: how
// many writes amortize one fold and one materialize. The win grows with it,
// because a fold collapses foldEvery shard-tree descents and metadata round-trips
// into one. The fold still upserts one sub-tree row per dirty subkey, so the
// sub-tree descent is not what the overlay removes; the shard-tree round-trip is.
// That distinction is the point of measuring across windows rather than at one.
func BenchmarkAbsorbVsCollUpdate(b *testing.B) {
	const collSize = 256
	subs := make([][]byte, collSize)
	vals := make([][]byte, collSize)
	for i := range subs {
		subs[i] = []byte(fmt.Sprintf("field%05d", i))
		vals[i] = []byte(fmt.Sprintf("value%05d", i))
	}
	pick := func(i int) int { return i % collSize }

	for _, foldEvery := range []int{16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("absorb/fold-every-%d", foldEvery), func(b *testing.B) {
			db := newDBTB(b)
			seedColl(b, db, "h", subs, vals)
			var lc *liveColl
			_, _ = db.CollRead([]byte("h"), func(r *CollReader) error {
				var e error
				lc, e = materializeLiveColl(r, TypeHash, EncHashtable, -1, false)
				return e
			})
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := pick(i)
				lc.put(subs[j], vals[j])
				if (i+1)%foldEvery == 0 {
					_ = db.CollUpdate([]byte("h"), TypeHash, EncHashtable, lc.fold)
				}
			}
			b.StopTimer()
			_ = db.CollUpdate([]byte("h"), TypeHash, EncHashtable, lc.fold)
		})
	}

	b.Run("per-op CollUpdate", func(b *testing.B) {
		db := newDBTB(b)
		seedColl(b, db, "h", subs, vals)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := pick(i)
			_ = db.CollUpdate([]byte("h"), TypeHash, EncHashtable, func(w *CollWriter) error {
				created, err := w.Put(subs[j], vals[j])
				if err != nil {
					return err
				}
				if created {
					w.SetCount(w.Count() + 1)
				}
				return nil
			})
		}
	})
}

// seedColl writes the initial element rows so both benchmark arms start from the
// same populated collection.
func seedColl(tb testing.TB, db *DB, key string, subs, vals [][]byte) {
	tb.Helper()
	err := db.CollUpdate([]byte(key), TypeHash, EncHashtable, func(w *CollWriter) error {
		for j := range subs {
			created, err := w.Put(subs[j], vals[j])
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
		tb.Fatalf("seed %s: %v", key, err)
	}
}
