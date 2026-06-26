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

// readAllColl collects every element row of a coll-form key into a map, going
// through CollRead so it observes the resident copy when the key is in the overlay
// and the element sub-tree otherwise. It is how the slice-1 tests below assert that
// a resident, post-fold, and reopened key all return the same set.
func readAllColl(t *testing.T, db *DB, key string) map[string]string {
	t.Helper()
	out := map[string]string{}
	ok, err := db.CollRead([]byte(key), func(r *CollReader) error {
		cur := r.Cursor()
		for err := cur.First(); cur.Valid(); err = cur.Next() {
			if err != nil {
				return err
			}
			out[string(cur.Key())] = string(cur.Value())
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("readAllColl %s: ok=%v err=%v", key, ok, err)
	}
	return out
}

// TestOverlayResidentThenFoldEvict drives the residency cycle end to end at the
// keyspace layer: with the gate on, writes to an existing coll-form hash engage the
// overlay and the key goes resident; reads observe every absorbed write; disabling
// the overlay folds the resident copy back into its sub-tree and drops the residency
// map, after which the sub-tree alone returns the full set.
func TestOverlayResidentThenFoldEvict(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if _, err := ks.SetHashOverlay(true); err != nil {
		t.Fatalf("enable overlay: %v", err)
	}
	// CollUpdate is always coll form at the keyspace layer, so the seed makes h
	// btree-backed; the follow-up writes then see prevIsTree and engage the overlay.
	putColl(t, db, "h", map[string]string{"f000": "v000"})
	const n = 50
	for i := 1; i < n; i++ {
		putColl(t, db, "h", map[string]string{fmt.Sprintf("f%03d", i): fmt.Sprintf("v%03d", i)})
	}
	s := ShardOf([]byte("h"))
	if db.shards[s].live["h"] == nil {
		t.Fatalf("key not resident after absorbed writes")
	}
	if got := readAllColl(t, db, "h"); len(got) != n {
		t.Fatalf("resident read len = %d want %d", len(got), n)
	}

	// Disabling folds every resident copy back into its sub-tree and drops the
	// residency map, so no stale copy outlives the overlay.
	if _, err := ks.SetHashOverlay(false); err != nil {
		t.Fatalf("disable overlay: %v", err)
	}
	if db.shards[s].live != nil {
		t.Fatalf("residency map survived disable: %v", db.shards[s].live)
	}
	got := readAllColl(t, db, "h")
	if len(got) != n {
		t.Fatalf("post-fold read len = %d want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("f%03d", i)
		if got[k] != fmt.Sprintf("v%03d", i) {
			t.Fatalf("%s = %q after fold", k, got[k])
		}
	}
}

// TestOverlayCrossesFoldThreshold absorbs enough writes to cross the inline fold
// threshold more than once, proving the inline folds along the way never drop or
// corrupt an element. A trailing fold-all leaves the copy resident and clean and the
// read set is still complete.
func TestOverlayCrossesFoldThreshold(t *testing.T) {
	ks, _, _ := newKS(t)
	db := mustDB(t, ks, 0)
	if _, err := ks.SetHashOverlay(true); err != nil {
		t.Fatalf("enable overlay: %v", err)
	}
	putColl(t, db, "h", map[string]string{"seed": "seed"})
	const n = 600 // crosses the 256 fold threshold twice
	for i := 0; i < n; i++ {
		putColl(t, db, "h", map[string]string{fmt.Sprintf("f%04d", i): fmt.Sprintf("v%04d", i)})
	}
	if got := readAllColl(t, db, "h"); len(got) != n+1 {
		t.Fatalf("resident read len = %d want %d", len(got), n+1)
	}

	if _, err := ks.FoldAllOverlay(); err != nil {
		t.Fatalf("fold all: %v", err)
	}
	got := readAllColl(t, db, "h")
	if len(got) != n+1 {
		t.Fatalf("post fold-all read len = %d want %d", len(got), n+1)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("f%04d", i)
		if got[k] != fmt.Sprintf("v%04d", i) {
			t.Fatalf("%s = %q after fold-all", k, got[k])
		}
	}
}

// TestOverlayFoldOnPersistReopen is the durability invariant: absorbed writes that
// never reached the sub-tree on their own must survive a fold-all, a commit, and a
// reopen. A copy left unfolded at the persist boundary would silently lose its
// writes, so this is the test that gates the overlay's safety.
func TestOverlayFoldOnPersistReopen(t *testing.T) {
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "ov.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	ks, err := Open(p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db := mustDB(t, ks, 0)
	if _, err := ks.SetHashOverlay(true); err != nil {
		t.Fatalf("enable overlay: %v", err)
	}
	putColl(t, db, "h", map[string]string{"seed": "seed"})
	const n = 300
	for i := 0; i < n; i++ {
		putColl(t, db, "h", map[string]string{fmt.Sprintf("f%04d", i): fmt.Sprintf("v%04d", i)})
	}
	// The persist boundary: fold every resident copy back into its sub-tree, then
	// commit. FoldAllOverlay is what the command layer runs before SAVE/BGSAVE/an RDB
	// snapshot/an AOF rewrite/a clean shutdown.
	if _, err := ks.FoldAllOverlay(); err != nil {
		t.Fatalf("fold all: %v", err)
	}
	if err := ks.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	p2, err := pager.Open(fs, "ov.aki", pager.Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = p2.Close() }()
	ks2, err := Open(p2)
	if err != nil {
		t.Fatalf("reopen keyspace: %v", err)
	}
	db2 := mustDB(t, ks2, 0)
	got := readAllColl(t, db2, "h")
	if len(got) != n+1 {
		t.Fatalf("reopened read len = %d want %d", len(got), n+1)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("f%04d", i)
		if got[k] != fmt.Sprintf("v%04d", i) {
			t.Fatalf("%s = %q after reopen", k, got[k])
		}
	}
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
