package btree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newOrderStatTree builds a fresh order-statistic tree over an in-memory pager.
// Keys are made wide by the callers so a 4096-byte page holds only a few dozen
// cells, which forces the multi-level interior structure (and the per-child count
// maintenance) with a few thousand inserts instead of hundreds of thousands.
func newOrderStatTree(t *testing.T) (*Tree, *pager.Pager) {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "ostat.aki", pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	tr, err := CreateOrderStat(p)
	if err != nil {
		t.Fatalf("create order-stat tree: %v", err)
	}
	if !tr.OrderStat() {
		t.Fatal("CreateOrderStat returned a plain tree")
	}
	return tr, p
}

// wideKey pads i out to a wide, sortable, distinct key so splits happen often.
func wideKey(i int) []byte {
	return fmt.Appendf(nil, "k%08d:%0100d", i, i)
}

// recountSubtree returns the true number of leaf rows under pgno and, for every
// interior node it visits, checks that each stored child count equals the count
// it recomputes from scratch. It is the independent oracle: it never trusts a
// stored count, it sums leaf cell counts bottom up. A drift of even one anywhere
// in insert, split, or the rightmost-child path shows up here.
func recountSubtree(t *testing.T, tr *Tree, pgno uint32, depth int) uint64 {
	t.Helper()
	if depth > maxDepth {
		t.Fatalf("recount deeper than %d levels at page %d", maxDepth, pgno)
	}
	n, err := tr.readNode(pgno)
	if err != nil {
		t.Fatalf("read page %d: %v", pgno, err)
	}
	if n.leaf {
		if n.counts != nil {
			t.Fatalf("leaf page %d carries counts", pgno)
		}
		return uint64(len(n.keys))
	}
	if len(n.counts) != len(n.children) {
		t.Fatalf("interior page %d has %d children but %d counts",
			pgno, len(n.children), len(n.counts))
	}
	var total uint64
	for i, child := range n.children {
		got := recountSubtree(t, tr, child, depth+1)
		if uint64(n.counts[i]) != got {
			t.Fatalf("interior page %d child %d: stored count %d, recount %d",
				pgno, i, n.counts[i], got)
		}
		total += got
	}
	return total
}

// verifyCounts checks the whole tree's stored counts against a fresh recount and
// asserts the total equals want, the number of distinct keys the tree should
// hold.
func verifyCounts(t *testing.T, tr *Tree, want int) {
	t.Helper()
	got := recountSubtree(t, tr, tr.root, 0)
	if got != uint64(want) {
		t.Fatalf("tree holds %d rows by recount, want %d", got, want)
	}
}

// TestOrderStatCountsConsistentRandomInserts inserts a few thousand keys in a
// shuffled order, verifying the stored counts against a full recount periodically
// and at the end. The shuffle drives splits at every level and exercises both the
// in-place count bump (no split) and the split count recompute.
func TestOrderStatCountsConsistentRandomInserts(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	rng := rand.New(rand.NewSource(0x05ada))

	const n = 4000
	order := rng.Perm(n)
	for step, i := range order {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if step%500 == 499 {
			verifyCounts(t, tr, step+1)
		}
	}
	verifyCounts(t, tr, n)

	// Every key must still read back, so the count maintenance did not corrupt the
	// tree structure itself.
	for i := range n {
		if _, ok, err := tr.Get(wideKey(i)); err != nil || !ok {
			t.Fatalf("get %d after inserts: ok %v err %v", i, ok, err)
		}
	}
}

// TestOrderStatCountsConsistentMixedInsertDelete builds a tree, then runs a long
// stream of mixed inserts and deletes against a model set, recounting the stored
// counts against a full independent walk periodically and at the end. Deletes thin
// out interior subtrees unevenly, so this exercises the per-level decrement on
// both cell counts and the rightmost-header count, and proves a delete shrinks
// every ancestor's count by exactly one (and a delete of an absent key shrinks
// nothing).
func TestOrderStatCountsConsistentMixedInsertDelete(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	rng := rand.New(rand.NewSource(0x0de1e7e))

	const span = 6000
	present := make(map[int]bool)
	// Seed with a base population so deletes have something to remove.
	for _, i := range rng.Perm(span)[:span/2] {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
		present[i] = true
	}
	verifyCounts(t, tr, len(present))

	const ops = 20000
	for step := range ops {
		i := rng.Intn(span)
		if rng.Intn(2) == 0 {
			if err := tr.Put(wideKey(i), []byte("v")); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
			present[i] = true
		} else {
			removed, err := tr.Delete(wideKey(i))
			if err != nil {
				t.Fatalf("delete %d: %v", i, err)
			}
			if removed != present[i] {
				t.Fatalf("delete %d reported %v, model says present %v", i, removed, present[i])
			}
			delete(present, i)
		}
		if step%1000 == 999 {
			verifyCounts(t, tr, len(present))
		}
	}
	verifyCounts(t, tr, len(present))

	// Every surviving key must read back, every removed key must miss.
	for i := range span {
		_, ok, err := tr.Get(wideKey(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if ok != present[i] {
			t.Fatalf("get %d found %v, model says present %v", i, ok, present[i])
		}
	}
}

// TestOrderStatCountsDeleteAbsentUnchanged deletes keys that were never inserted
// and checks no count moves, since removing nothing removes no row.
func TestOrderStatCountsDeleteAbsentUnchanged(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	const n = 2000
	for i := range n {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)

	for i := n; i < n+500; i++ {
		removed, err := tr.Delete(wideKey(i))
		if err != nil {
			t.Fatalf("delete absent %d: %v", i, err)
		}
		if removed {
			t.Fatalf("delete absent %d reported removed", i)
		}
	}
	verifyCounts(t, tr, n)
}

// TestOrderStatCountsDeleteToEmpty inserts a population then deletes every key,
// asserting the counts stay exact all the way down to an empty tree.
func TestOrderStatCountsDeleteToEmpty(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	rng := rand.New(rand.NewSource(0xe33))
	const n = 3000
	for i := range n {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)

	left := n
	for _, i := range rng.Perm(n) {
		removed, err := tr.Delete(wideKey(i))
		if err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
		if !removed {
			t.Fatalf("delete %d reported not removed", i)
		}
		left--
		if left%500 == 0 {
			verifyCounts(t, tr, left)
		}
	}
	verifyCounts(t, tr, 0)
}

// TestOrderStatCountsAppendAscending inserts in ascending key order, which sends
// every split down the rightmost edge and so leans on the rightmost-child count
// stored in the per-type header rather than in a cell. A bug in that header slot
// would pass the random test (which spreads splits around) but fail here.
func TestOrderStatCountsAppendAscending(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	const n = 3000
	for i := range n {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)
}

// TestOrderStatCountsUnchangedByReplace replaces the value under every key with a
// value of a different length (so the leaf cell is rewritten and may even overflow
// and split) and checks the row counts never move, because a replace adds no row.
func TestOrderStatCountsUnchangedByReplace(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	const n = 2000
	for i := range n {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)

	// Rewrite with a longer value. A same-key write must not change any count.
	long := make([]byte, 64)
	for i := range long {
		long[i] = 'x'
	}
	for i := range n {
		if err := tr.Put(wideKey(i), long); err != nil {
			t.Fatalf("replace %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)
}

// TestOrderStatPlainTreeUnaffected is the regression guard: a plain tree built the
// normal way must carry no counts on any page, take the unaugmented format, and
// read back every key. This pins that the order-statistic work is fully gated and
// the headline keyspace path is untouched.
func TestOrderStatPlainTreeUnaffected(t *testing.T) {
	tr, _ := newTree(t, 4096)
	if tr.OrderStat() {
		t.Fatal("plain tree reports order-stat")
	}
	const n = 3000
	for i := range n {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Walk every page: no interior page may carry counts on a plain tree.
	assertNoCounts(t, tr, tr.root, 0)
	for i := range n {
		if _, ok, err := tr.Get(wideKey(i)); err != nil || !ok {
			t.Fatalf("get %d: ok %v err %v", i, ok, err)
		}
	}
}

// TestOrderStatRankSelectRoundTrip inserts a few thousand keys, then checks Rank
// and SelectAt against the known sorted order: SelectAt(i) returns the i-th key,
// Rank of that key returns i and reports present, and the two are inverses across
// the whole tree. It also checks the boundary cases (rank of a key below all keys
// is 0, of a key above all keys is the cardinality, and SelectAt past the end
// misses).
func TestOrderStatRankSelectRoundTrip(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	rng := rand.New(rand.NewSource(0x5e1ec7))
	const n = 4000
	for _, i := range rng.Perm(n) {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)

	// wideKey is constructed so ascending i gives ascending key bytes, so rank i is
	// exactly the key built from i.
	for i := range n {
		got, ok, err := tr.SelectAt(uint64(i))
		if err != nil || !ok {
			t.Fatalf("selectAt %d: ok %v err %v", i, ok, err)
		}
		want := wideKey(i)
		if string(got) != string(want) {
			t.Fatalf("selectAt %d = %q, want %q", i, got, want)
		}
		rank, present, err := tr.Rank(want)
		if err != nil || !present {
			t.Fatalf("rank of key %d: present %v err %v", i, present, err)
		}
		if rank != uint64(i) {
			t.Fatalf("rank of key %d = %d, want %d", i, rank, i)
		}
	}

	// A key below every stored key ranks 0; a key above every stored key ranks n.
	low := []byte("a")
	if r, present, err := tr.Rank(low); err != nil || present || r != 0 {
		t.Fatalf("rank of below-all key: r %d present %v err %v", r, present, err)
	}
	high := []byte("z")
	if r, present, err := tr.Rank(high); err != nil || present || r != uint64(n) {
		t.Fatalf("rank of above-all key: r %d present %v err %v", r, present, err)
	}
	if _, ok, err := tr.SelectAt(uint64(n)); err != nil || ok {
		t.Fatalf("selectAt past end: ok %v err %v", ok, err)
	}
}

// TestOrderStatRankAbsentKey checks Rank reports not-present for a key that falls
// between two stored keys but still returns the correct rank (the number of keys
// strictly below it).
func TestOrderStatRankAbsentKey(t *testing.T) {
	tr, _ := newOrderStatTree(t)
	const n = 2000
	// Insert even indices only, so odd-index keys are absent but land between two
	// present keys.
	for i := 0; i < 2*n; i += 2 {
		if err := tr.Put(wideKey(i), []byte("v")); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	verifyCounts(t, tr, n)

	for k := range n {
		absent := wideKey(2*k + 1) // sits just above the k-th present key (index 2k)
		rank, present, err := tr.Rank(absent)
		if err != nil {
			t.Fatalf("rank absent %d: %v", k, err)
		}
		if present {
			t.Fatalf("rank reported absent key %d present", k)
		}
		// Keys 0,2,...,2k are below it: that is k+1 keys.
		if rank != uint64(k+1) {
			t.Fatalf("rank of absent key above present %d = %d, want %d", k, rank, k+1)
		}
	}
}

// TestOrderStatRankSelectPlainTreeRejected pins that Rank and SelectAt refuse a
// plain tree, since it carries no counts to descend by.
func TestOrderStatRankSelectPlainTreeRejected(t *testing.T) {
	tr, _ := newTree(t, 4096)
	if err := tr.Put(wideKey(1), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, _, err := tr.Rank(wideKey(1)); err != ErrNotOrderStat {
		t.Fatalf("Rank on plain tree: err %v, want ErrNotOrderStat", err)
	}
	if _, _, err := tr.SelectAt(0); err != ErrNotOrderStat {
		t.Fatalf("SelectAt on plain tree: err %v, want ErrNotOrderStat", err)
	}
}

func assertNoCounts(t *testing.T, tr *Tree, pgno uint32, depth int) {
	t.Helper()
	if depth > maxDepth {
		t.Fatalf("plain tree deeper than %d at page %d", maxDepth, pgno)
	}
	n, err := tr.readNode(pgno)
	if err != nil {
		t.Fatalf("read page %d: %v", pgno, err)
	}
	if n.counts != nil {
		t.Fatalf("plain tree page %d carries counts", pgno)
	}
	if n.leaf {
		return
	}
	for _, child := range n.children {
		assertNoCounts(t, tr, child, depth+1)
	}
}
