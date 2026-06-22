package btree

import (
	"fmt"
	"strings"
	"testing"
)

func TestCheckInvariantsEmpty(t *testing.T) {
	tr, _ := newTree(t, 0)
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("empty tree: %v", err)
	}
}

// padVal returns a value big enough that a 4096-byte page holds only a few
// entries, so a few hundred keys build a multi-level tree the walk must descend.
func padVal(i int) []byte {
	return []byte(fmt.Sprintf("val:%d:", i) + strings.Repeat("x", 400))
}

func TestCheckInvariantsManyKeys(t *testing.T) {
	tr, _ := newTree(t, 4096)
	for i := range 500 {
		k := []byte(fmt.Sprintf("key:%05d", i))
		if err := tr.Put(k, padVal(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("after 500 puts: %v", err)
	}
}

func TestCheckInvariantsAfterDeletes(t *testing.T) {
	tr, _ := newTree(t, 4096)
	for i := range 300 {
		_ = tr.Put([]byte(fmt.Sprintf("key:%05d", i)), padVal(i))
	}
	for i := 0; i < 300; i += 3 {
		if _, err := tr.Delete([]byte(fmt.Sprintf("key:%05d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := CheckInvariants(tr); err != nil {
		t.Fatalf("after deletes: %v", err)
	}
}

// TestCheckInvariantsCatchesUnsortedKeys corrupts a leaf so its keys are out of
// order, then asserts the checker reports it.
func TestCheckInvariantsCatchesUnsortedKeys(t *testing.T) {
	tr, _ := newTree(t, 4096)
	for i := range 200 {
		_ = tr.Put([]byte(fmt.Sprintf("key:%05d", i)), padVal(i))
	}

	leaf := firstLeaf(t, tr)
	n, err := tr.readNode(leaf)
	if err != nil {
		t.Fatal(err)
	}
	if len(n.keys) < 2 {
		t.Fatalf("leaf %d has only %d keys, need at least 2 to reorder", leaf, len(n.keys))
	}
	n.keys[0], n.keys[1] = n.keys[1], n.keys[0]
	n.vals[0], n.vals[1] = n.vals[1], n.vals[0]
	if err := tr.writeNode(leaf, n); err != nil {
		t.Fatal(err)
	}

	err = CheckInvariants(tr)
	if err == nil {
		t.Fatal("expected an error from a leaf with reordered keys")
	}
	if !strings.Contains(err.Error(), "ascending") {
		t.Fatalf("error %q does not mention ascending order", err)
	}
}

// TestCheckInvariantsCatchesRangeViolation corrupts the last separator key of an
// interior node to a value larger than every real key, so the rightmost child's
// keys fall below the lower bound that separator now implies.
func TestCheckInvariantsCatchesRangeViolation(t *testing.T) {
	tr, _ := newTree(t, 4096)
	for i := range 400 {
		_ = tr.Put([]byte(fmt.Sprintf("key:%05d", i)), padVal(i))
	}

	root := tr.Root()
	n, err := tr.readNode(root)
	if err != nil {
		t.Fatal(err)
	}
	if n.leaf {
		t.Skip("tree did not grow past a single leaf")
	}
	// Raising the largest separator keeps the node's own keys ascending but makes
	// the rightmost subtree's keys sit below their new lower bound.
	n.keys[len(n.keys)-1] = []byte("key:99999")
	if err := tr.writeNode(root, n); err != nil {
		t.Fatal(err)
	}

	err = CheckInvariants(tr)
	if err == nil {
		t.Fatal("expected an error from a separator key that breaks the child range")
	}
	if !strings.Contains(err.Error(), "bound") {
		t.Fatalf("error %q does not mention a bound", err)
	}
}

// firstLeaf returns the leftmost leaf page by descending child[0] from the root.
func firstLeaf(t *testing.T, tr *Tree) uint32 {
	t.Helper()
	pgno := tr.Root()
	for {
		n, err := tr.readNode(pgno)
		if err != nil {
			t.Fatal(err)
		}
		if n.leaf {
			return pgno
		}
		pgno = n.children[0]
	}
}
