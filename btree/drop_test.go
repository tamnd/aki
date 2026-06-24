package btree

import (
	"fmt"
	"testing"
)

// TestDropTreeFreesAllPages builds a multi-level tree, records the page count,
// drops it, and checks every page it used came back to the freelist so a dropped
// collection sub-tree leaks nothing.
func TestDropTreeFreesAllPages(t *testing.T) {
	tr, p := newTree(t, 4096)

	// in-use pages with just the empty root leaf present.
	inUse := func() int { return int(p.PageCount()) - p.FreeCount() }
	inUseEmpty := inUse()

	// Enough keys with a chunky value to force several leaves and an interior
	// level, so the recursive teardown path is exercised, not just a single leaf.
	val := make([]byte, 200)
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("key-%05d", i))
		if err := tr.Put(k, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	if grew := inUse() - inUseEmpty; grew <= 1 {
		t.Fatalf("expected a multi-page tree, in-use pages grew by %d", grew)
	}

	if err := DropTree(p, tr.Root()); err != nil {
		t.Fatalf("DropTree: %v", err)
	}

	// DropTree frees every page the tree held, including the original root leaf
	// that existed before the puts, so in-use pages drop one below the empty-tree
	// baseline and nothing is leaked.
	if got := inUse(); got != inUseEmpty-1 {
		t.Fatalf("after DropTree in-use pages = %d, want %d (leak)", got, inUseEmpty-1)
	}
}

// TestDropTreeEmpty drops a tree that only has its empty root leaf.
func TestDropTreeEmpty(t *testing.T) {
	tr, p := newTree(t, 0)
	before := p.FreeCount()
	if err := DropTree(p, tr.Root()); err != nil {
		t.Fatalf("DropTree empty: %v", err)
	}
	if got := p.FreeCount(); got != before+1 {
		t.Fatalf("DropTree empty freed %d pages, want 1", got-before)
	}
}
