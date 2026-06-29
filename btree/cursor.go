package btree

import "bytes"

// Cursor iterates a tree's keys in sorted order. It walks the leaf level using
// the right-sibling links, so a full scan touches each leaf once. A cursor
// reflects the tree as it was when each leaf was read; it is meant for a single
// scan, not for holding open across concurrent writes.
//
// Forward motion (First/Seek/Next) follows the right-sibling links and ignores
// path. Backward motion (Last/Prev/SeekForPrev) cannot use sibling links because
// leaves carry no left link, so it records the root-to-leaf descent in path and
// climbs it to reach the previous subtree. The two directions do not interleave:
// position with Last or SeekForPrev before walking with Prev.
type Cursor struct {
	t    *Tree
	leaf *node
	idx  int
	path []frame
}

// frame records an interior node and the child index a backward descent took
// through it, so Prev can climb back up and step into the previous subtree.
type frame struct {
	n   *node
	idx int
}

// Cursor returns a new, unpositioned cursor. Call First or Seek before reading.
func (t *Tree) Cursor() *Cursor { return &Cursor{t: t} }

// First positions the cursor at the smallest key.
func (c *Cursor) First() error {
	n, err := c.t.leftmostLeaf()
	if err != nil {
		return err
	}
	c.leaf = n
	c.idx = 0
	return nil
}

// Seek positions the cursor at the first key greater than or equal to key.
func (c *Cursor) Seek(key []byte) error {
	pgno := c.t.root
	for {
		n, err := c.t.readNode(pgno)
		if err != nil {
			return err
		}
		if n.leaf {
			c.leaf = n
			c.idx = 0
			for c.idx < len(n.keys) && bytes.Compare(n.keys[c.idx], key) < 0 {
				c.idx++
			}
			if c.idx >= len(n.keys) {
				return c.advanceLeaf()
			}
			return nil
		}
		pgno = n.children[n.childIndex(key)]
	}
}

// Valid reports whether the cursor points at a live entry.
func (c *Cursor) Valid() bool {
	return c.leaf != nil && c.idx < len(c.leaf.keys)
}

// Key returns the key at the cursor. The bytes are owned by the cursor and are
// valid until the next call that moves it.
func (c *Cursor) Key() []byte { return c.leaf.keys[c.idx] }

// Value returns the value at the cursor.
func (c *Cursor) Value() []byte { return c.leaf.vals[c.idx] }

// Next advances to the following key.
func (c *Cursor) Next() error {
	if c.leaf == nil {
		return nil
	}
	c.idx++
	if c.idx < len(c.leaf.keys) {
		return nil
	}
	return c.advanceLeaf()
}

// advanceLeaf moves to the next non-empty leaf via the sibling links, or marks
// the cursor exhausted when there is none.
func (c *Cursor) advanceLeaf() error {
	for {
		sib := c.leaf.rightSibling
		if sib == noSibling {
			c.leaf = nil
			c.idx = 0
			return nil
		}
		n, err := c.t.readNode(sib)
		if err != nil {
			return err
		}
		c.leaf = n
		c.idx = 0
		if len(n.keys) > 0 {
			return nil
		}
	}
}

// Last positions the cursor at the largest key. It descends the rightmost child
// at every level, recording the path so Prev can climb back up. A deleted-empty
// rightmost leaf (deletes do not merge underfull pages) is handled by climbing to
// the predecessor.
func (c *Cursor) Last() error {
	c.path = c.path[:0]
	return c.descendLast(c.t.root)
}

// SeekForPrev positions the cursor at the largest key less than or equal to key,
// or marks it exhausted when every key is greater. It records the path so Prev
// continues the backward walk, so use it to start a reverse range scan at the
// upper bound.
func (c *Cursor) SeekForPrev(key []byte) error {
	c.path = c.path[:0]
	pgno := c.t.root
	for {
		n, err := c.t.readNode(pgno)
		if err != nil {
			return err
		}
		if n.leaf {
			c.leaf = n
			c.idx = len(n.keys) - 1
			for c.idx >= 0 && bytes.Compare(n.keys[c.idx], key) > 0 {
				c.idx--
			}
			if c.idx >= 0 {
				return nil
			}
			return c.climbPrev() // every key in this leaf exceeds key
		}
		ci := n.childIndex(key)
		c.path = append(c.path, frame{n, ci})
		pgno = n.children[ci]
	}
}

// Prev steps to the previous key in sorted order. Within a leaf it decrements the
// index; at a leaf boundary it climbs the recorded path to the previous subtree
// and descends that subtree's rightmost key. The cursor must have been positioned
// by Last or SeekForPrev (the forward methods do not maintain the path).
func (c *Cursor) Prev() error {
	if c.leaf == nil {
		return nil
	}
	if c.idx > 0 {
		c.idx--
		return nil
	}
	return c.climbPrev()
}

// descendLast walks from pgno down the rightmost children to the largest live key
// at or below it, pushing each interior node onto the path. An empty leaf at the
// bottom (a page emptied by deletes) sends it climbing for the predecessor.
func (c *Cursor) descendLast(pgno uint32) error {
	for {
		n, err := c.t.readNode(pgno)
		if err != nil {
			return err
		}
		if n.leaf {
			c.leaf = n
			c.idx = len(n.keys) - 1
			if c.idx >= 0 {
				return nil
			}
			return c.climbPrev()
		}
		ci := len(n.children) - 1
		c.path = append(c.path, frame{n, ci})
		pgno = n.children[ci]
	}
}

// climbPrev pops the path until it finds an interior node with an unvisited left
// sibling subtree, then descends that subtree's rightmost key, skipping any empty
// leaves it meets. It marks the cursor exhausted when the path runs out, which
// means the current key was the smallest.
func (c *Cursor) climbPrev() error {
	for len(c.path) > 0 {
		top := &c.path[len(c.path)-1]
		if top.idx == 0 {
			c.path = c.path[:len(c.path)-1]
			continue
		}
		top.idx--
		pgno := top.n.children[top.idx]
		empty, err := c.descendRightmost(pgno)
		if err != nil {
			return err
		}
		if !empty {
			return nil
		}
		// The whole subtree was empty leaves; descendRightmost left its frames on
		// the path, so the loop climbs from the deepest of them and keeps going left.
	}
	c.leaf = nil
	c.idx = 0
	return nil
}

// descendRightmost descends pgno's rightmost children to a non-empty leaf, pushing
// interior frames as it goes, and positions the cursor there. empty is true when
// every leaf it reached was empty, in which case the cursor is left for climbPrev
// to continue from the frames just pushed.
func (c *Cursor) descendRightmost(pgno uint32) (empty bool, err error) {
	for {
		n, err := c.t.readNode(pgno)
		if err != nil {
			return false, err
		}
		if n.leaf {
			if len(n.keys) == 0 {
				return true, nil
			}
			c.leaf = n
			c.idx = len(n.keys) - 1
			return false, nil
		}
		ci := len(n.children) - 1
		c.path = append(c.path, frame{n, ci})
		pgno = n.children[ci]
	}
}

// leftmostLeaf descends to the first leaf in key order.
func (t *Tree) leftmostLeaf() (*node, error) {
	pgno := t.root
	for {
		n, err := t.readNode(pgno)
		if err != nil {
			return nil, err
		}
		if n.leaf {
			return n, nil
		}
		pgno = n.children[0]
	}
}
