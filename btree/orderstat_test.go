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
