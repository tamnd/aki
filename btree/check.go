package btree

import (
	"bytes"
	"fmt"
)

// CheckInvariants walks the whole tree and returns the first structural problem
// it finds, or nil when the tree is well formed. It is the on-demand form of the
// debug-build assertions: the integrity checker calls it over a file at rest, and
// debug builds call it after every Put and Delete.
//
// The checks are:
//   - keys within a node are strictly ascending,
//   - an interior node has exactly one more child than it has keys, and a leaf
//     has one value per key and no children,
//   - every key sits inside the (lo, hi] range its parent separators imply, with
//     lo inclusive and hi exclusive,
//   - the keys across the whole tree, visited left to right, are strictly
//     ascending, and
//   - the leaf right-sibling chain visits the leaves in that same left-to-right
//     order.
//
// Minimum occupancy is intentionally not checked. This tree splits by serialized
// byte size, not by a fixed key count, so a "non-root nodes are at least half
// full" rule in keys does not describe it.
func CheckInvariants(t *Tree) error {
	c := &checker{t: t}
	if err := c.walk(t.root, nil, nil, 0); err != nil {
		return err
	}
	return c.verifyLeafChain()
}

type checker struct {
	t        *Tree
	prevKey  []byte   // last key seen in the in-order walk, for global ordering
	leaves   []uint32 // leaf pages in in-order (left to right)
	firstKey [][]byte // first key of each leaf, parallel to leaves
}

func (c *checker) walk(pgno uint32, lo, hi []byte, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("btree: tree deeper than %d levels, likely a cycle", maxDepth)
	}
	n, err := c.t.readNode(pgno)
	if err != nil {
		return fmt.Errorf("btree: read page %d: %w", pgno, err)
	}

	for i := 1; i < len(n.keys); i++ {
		if bytes.Compare(n.keys[i-1], n.keys[i]) >= 0 {
			return fmt.Errorf("btree: page %d keys not ascending: %q >= %q",
				pgno, n.keys[i-1], n.keys[i])
		}
	}

	for _, k := range n.keys {
		if lo != nil && bytes.Compare(k, lo) < 0 {
			return fmt.Errorf("btree: page %d key %q below lower bound %q", pgno, k, lo)
		}
		if hi != nil && bytes.Compare(k, hi) >= 0 {
			return fmt.Errorf("btree: page %d key %q at or above upper bound %q", pgno, k, hi)
		}
	}

	if n.leaf {
		if len(n.vals) != len(n.keys) {
			return fmt.Errorf("btree: leaf %d has %d keys but %d values",
				pgno, len(n.keys), len(n.vals))
		}
		if len(n.children) != 0 {
			return fmt.Errorf("btree: leaf %d has %d children", pgno, len(n.children))
		}
		for _, k := range n.keys {
			if c.prevKey != nil && bytes.Compare(k, c.prevKey) <= 0 {
				return fmt.Errorf("btree: global order broken at %q after %q", k, c.prevKey)
			}
			c.prevKey = bytes.Clone(k)
		}
		c.leaves = append(c.leaves, pgno)
		if len(n.keys) > 0 {
			c.firstKey = append(c.firstKey, bytes.Clone(n.keys[0]))
		} else {
			c.firstKey = append(c.firstKey, nil)
		}
		return nil
	}

	if len(n.children) != len(n.keys)+1 {
		return fmt.Errorf("btree: interior %d has %d keys but %d children",
			pgno, len(n.keys), len(n.children))
	}
	for i, child := range n.children {
		clo := lo
		if i > 0 {
			clo = n.keys[i-1]
		}
		chi := hi
		if i < len(n.keys) {
			chi = n.keys[i]
		}
		if err := c.walk(child, clo, chi, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// verifyLeafChain follows the right-sibling links from the leftmost leaf and
// checks they visit the same leaves, in the same order, as the in-order walk did.
func (c *checker) verifyLeafChain() error {
	if len(c.leaves) == 0 {
		return nil
	}
	pgno := c.leaves[0]
	for i := 0; ; i++ {
		if i >= len(c.leaves) {
			return fmt.Errorf("btree: leaf sibling chain longer than the tree's %d leaves", len(c.leaves))
		}
		if pgno != c.leaves[i] {
			return fmt.Errorf("btree: leaf chain step %d is page %d, walk expected %d",
				i, pgno, c.leaves[i])
		}
		n, err := c.t.readNode(pgno)
		if err != nil {
			return fmt.Errorf("btree: read leaf %d: %w", pgno, err)
		}
		if n.rightSibling == noSibling {
			if i != len(c.leaves)-1 {
				return fmt.Errorf("btree: leaf chain ended at step %d but the tree has %d leaves",
					i, len(c.leaves))
			}
			return nil
		}
		pgno = n.rightSibling
	}
}

// maxDepth bounds the walk so a corrupt child pointer that forms a cycle is
// reported instead of recursing without end.
const maxDepth = 64
