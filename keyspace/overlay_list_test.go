package keyspace

import (
	"fmt"
	"testing"

	"github.com/tamnd/aki/encoding"
)

// testPosRow encodes an absolute list position the same way the command layer's
// listPosRow does: a signed int64 with the sign bit flipped, big-endian, so the
// btree's bytewise row order equals numeric position order even when head runs
// negative. The keyspace package does not own this encoding, so the test supplies
// it, exactly as a wired command path would.
func testPosRow(pos int64) []byte {
	return encoding.AppendU64BE(make([]byte, 0, 8), uint64(pos)^(1<<63))
}

// unitSize is a fixed per-element byte cost for tests that only check that the
// byte total moves in step with pushes and pops; the real path passes lpEntrySize.
func unitSize([]byte) uint64 { return 1 }

// fakeTree is an in-memory stand-in for the element sub-tree, so the positional
// read merge can be tested without a btree. It returns rows by absolute position.
type fakeTree map[int64][]byte

func (ft fakeTree) get(pos int64) ([]byte, bool, error) {
	v, ok := ft[pos]
	return v, ok, nil
}

func TestLiveListPushCountAndBytes(t *testing.T) {
	// A fresh list, right push 3 then left push 2: count climbs, the window moves
	// the way listTreePush moves it, and the reply length is read off the window.
	ll := newLiveList(0, 0, 0, EncListpack, -1, false, 0)
	if got := ll.pushRight([][]byte{[]byte("a"), []byte("b"), []byte("c")}, unitSize); got != 3 {
		t.Fatalf("rpush new len = %d want 3", got)
	}
	if ll.head != 0 || ll.tail != 3 {
		t.Fatalf("window after rpush = [%d,%d) want [0,3)", ll.head, ll.tail)
	}
	if got := ll.pushLeft([][]byte{[]byte("x"), []byte("y")}, unitSize); got != 5 {
		t.Fatalf("lpush new len = %d want 5", got)
	}
	if ll.head != -2 || ll.tail != 3 {
		t.Fatalf("window after lpush = [%d,%d) want [-2,3)", ll.head, ll.tail)
	}
	if ll.count() != 5 {
		t.Fatalf("count = %d want 5", ll.count())
	}
	if ll.byteTotal() != 5 {
		t.Fatalf("bytes = %d want 5", ll.byteTotal())
	}
}

func TestLiveListLPushReversedOrder(t *testing.T) {
	// LPUSH x y z leaves the list as z y x, head first, because each value lands one
	// position below the previous. The resident copy must reproduce that.
	ll := newLiveList(0, 0, 0, EncListpack, -1, false, 0)
	ll.pushLeft([][]byte{[]byte("x"), []byte("y"), []byte("z")}, unitSize)
	got, err := ll.rangeElems(0, -1, fakeTree{}.get)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	want := []string{"z", "y", "x"}
	if len(got) != len(want) {
		t.Fatalf("range len = %d want %d", len(got), len(want))
	}
	for i, w := range want {
		if string(got[i]) != w {
			t.Fatalf("range[%d] = %q want %q", i, got[i], w)
		}
	}
}

func TestLiveListReadMergesPendingOverTree(t *testing.T) {
	// A list whose positions 0,1,2 are already folded into the tree, then a right
	// push lands 3,4 in pending. Reads must see all five in order, drawing the first
	// three from the tree and the last two from pending.
	tree := fakeTree{0: []byte("a"), 1: []byte("b"), 2: []byte("c")}
	ll := newLiveList(0, 3, 3, EncListpack, -1, false, 0)
	ll.pushRight([][]byte{[]byte("d"), []byte("e")}, unitSize)

	// LINDEX across the seam.
	for i, want := range []string{"a", "b", "c", "d", "e"} {
		v, ok, err := ll.index(int64(i), tree.get)
		if err != nil || !ok {
			t.Fatalf("index %d: ok=%v err=%v", i, ok, err)
		}
		if string(v) != want {
			t.Fatalf("index %d = %q want %q", i, v, want)
		}
	}
	// Negative index counts from the tail.
	v, ok, _ := ll.index(-1, tree.get)
	if !ok || string(v) != "e" {
		t.Fatalf("index -1 = %q ok=%v want e", v, ok)
	}
	// LRANGE the whole list.
	got, err := ll.rangeElems(0, -1, tree.get)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	want := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("range len = %d want %d", len(got), len(want))
	}
	for i, w := range want {
		if string(got[i]) != w {
			t.Fatalf("range[%d] = %q want %q", i, got[i], w)
		}
	}
}

func TestLiveListPopFromBothEnds(t *testing.T) {
	// Three folded elements plus two pending pushes. A left pop draws from the tree
	// and records a delete; a right pop draws from pending and just forgets it.
	tree := fakeTree{0: []byte("a"), 1: []byte("b"), 2: []byte("c")}
	ll := newLiveList(0, 3, 3, EncListpack, -1, false, 0)
	ll.pushRight([][]byte{[]byte("d"), []byte("e")}, unitSize)

	left, err := ll.popLeft(1, tree.get, unitSize)
	if err != nil || len(left) != 1 || string(left[0]) != "a" {
		t.Fatalf("popLeft = %v err=%v want [a]", left, err)
	}
	if _, marked := ll.dels[0]; !marked {
		t.Fatalf("popLeft of folded pos 0 did not mark a delete")
	}
	if ll.head != 1 {
		t.Fatalf("head after popLeft = %d want 1", ll.head)
	}

	right, err := ll.popRight(1, tree.get, unitSize)
	if err != nil || len(right) != 1 || string(right[0]) != "e" {
		t.Fatalf("popRight = %v err=%v want [e]", right, err)
	}
	if _, stillPending := ll.pending[4]; stillPending {
		t.Fatalf("popRight of pending pos 4 left it in pending")
	}
	if _, marked := ll.dels[4]; marked {
		t.Fatalf("popRight of pending pos 4 wrongly marked a tree delete")
	}
	if ll.tail != 4 {
		t.Fatalf("tail after popRight = %d want 4", ll.tail)
	}
	if ll.count() != 3 {
		t.Fatalf("count after pops = %d want 3 (b c d)", ll.count())
	}
	if ll.byteTotal() != 3 {
		t.Fatalf("bytes after pops = %d want 3", ll.byteTotal())
	}
}

// TestLiveListFoldRoundtrip folds a resident list into a real coll sub-tree and
// reads it back through a CollReader cursor, proving the folded rows reproduce the
// list head to tail and the window lands in the metadata. This is the one test
// that exercises the injected position encoder against the actual btree.
func TestLiveListFoldRoundtrip(t *testing.T) {
	db := newDBTB(t)
	key := []byte("mylist")

	// Build a resident list of five elements (a..e) entirely in memory, then fold it
	// into a fresh sub-tree through CollUpdate, the same writer the wired path uses.
	ll := newLiveList(0, 0, 0, EncListpack, -1, false, 0)
	ll.pushRight([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}, unitSize)
	if err := db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error {
		return ll.fold(w, testPosRow)
	}); err != nil {
		t.Fatalf("fold into sub-tree: %v", err)
	}

	// The sub-tree now holds the five rows in position order, and the metadata window
	// reports head 0, tail 5, count 5.
	ok, err := db.CollRead(key, func(r *CollReader) error {
		if r.Count() != 5 {
			t.Fatalf("folded count = %d want 5", r.Count())
		}
		if r.Head() != 0 || r.Tail() != 5 {
			t.Fatalf("folded window = [%d,%d) want [0,5)", r.Head(), r.Tail())
		}
		var got []string
		c := r.Cursor()
		if e := c.First(); e != nil {
			return e
		}
		for c.Valid() {
			got = append(got, string(c.Value()))
			if e := c.Next(); e != nil {
				return e
			}
		}
		want := []string{"a", "b", "c", "d", "e"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("folded order = %v want %v", got, want)
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
}

// BenchmarkListPushAbsorbVsCollUpdate validates the absorb-vs-descend premise for
// the list push path: a run of right pushes absorbed into a resident liveList and
// folded every N is compared against the same pushes each run through its own
// CollUpdate, which is the synchronous per-push descent the wired fast path
// removes. The absorb variants should beat the per-op variant by the fold batch
// factor, the same way BenchmarkAbsorbVsCollUpdate shows it for hash.
func BenchmarkListPushAbsorbVsCollUpdate(b *testing.B) {
	val := []byte("element-payload-of-modest-size")
	one := [][]byte{val}

	for _, foldEvery := range []int{16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("absorb/fold-every-%d", foldEvery), func(b *testing.B) {
			db := newDBTB(b)
			key := []byte("l")
			if err := db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error { return nil }); err != nil {
				b.Fatalf("seed: %v", err)
			}
			ll := newLiveList(0, 0, 0, EncQuicklist, -1, false, 0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ll.pushRight(one, unitSize)
				if (i+1)%foldEvery == 0 {
					_ = db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error {
						return ll.fold(w, testPosRow)
					})
				}
			}
			b.StopTimer()
			_ = db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error {
				return ll.fold(w, testPosRow)
			})
		})
	}

	b.Run("per-op CollUpdate", func(b *testing.B) {
		db := newDBTB(b)
		key := []byte("l")
		if err := db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error { return nil }); err != nil {
			b.Fatalf("seed: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error {
				if _, err := w.Put(testPosRow(w.Tail()), val); err != nil {
					return err
				}
				w.SetTail(w.Tail() + 1)
				w.SetCount(uint64(w.Tail() - w.Head()))
				return nil
			})
		}
	})
}

// TestLiveListFoldDeltaAfterPop folds a list, pops across the seam, then folds
// again, checking the second fold writes only the delta (the delete) and the
// window narrows.
func TestLiveListFoldDeltaAfterPop(t *testing.T) {
	db := newDBTB(t)
	key := []byte("dl")

	ll := newLiveList(0, 0, 0, EncListpack, -1, false, 0)
	ll.pushRight([][]byte{[]byte("a"), []byte("b"), []byte("c")}, unitSize)
	if err := db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error {
		return ll.fold(w, testPosRow)
	}); err != nil {
		t.Fatalf("first fold: %v", err)
	}
	if ll.dirty() {
		t.Fatalf("resident copy dirty after fold")
	}

	// Pop the head (folded pos 0) and fold the delta back.
	got, err := ll.popLeft(1, func(pos int64) ([]byte, bool, error) {
		// pos 0 was folded; serve it from the tree via a direct read.
		var v []byte
		var found bool
		_, e := db.CollRead(key, func(r *CollReader) error {
			vv, ok, ee := r.Get(testPosRow(pos))
			if ok {
				v = append([]byte(nil), vv...)
				found = true
			}
			return ee
		})
		return v, found, e
	}, unitSize)
	if err != nil || len(got) != 1 || string(got[0]) != "a" {
		t.Fatalf("popLeft = %v err=%v want [a]", got, err)
	}
	if err := db.CollUpdate(key, TypeList, EncQuicklist, func(w *CollWriter) error {
		return ll.fold(w, testPosRow)
	}); err != nil {
		t.Fatalf("second fold: %v", err)
	}

	ok, err := db.CollRead(key, func(r *CollReader) error {
		if r.Count() != 2 {
			t.Fatalf("count after pop+fold = %d want 2", r.Count())
		}
		if r.Head() != 1 || r.Tail() != 3 {
			t.Fatalf("window after pop+fold = [%d,%d) want [1,3)", r.Head(), r.Tail())
		}
		if _, present, _ := r.Get(testPosRow(0)); present {
			t.Fatalf("popped pos 0 still in sub-tree after fold")
		}
		return nil
	})
	if err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
}
