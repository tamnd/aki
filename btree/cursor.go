package btree

import "bytes"

// Cursor iterates a tree's keys in sorted order. It walks the leaf level using
// the right-sibling links, so a full scan touches each leaf once. A cursor
// reflects the tree as it was when each leaf was read; it is meant for a single
// scan, not for holding open across concurrent writes.
type Cursor struct {
	t    *Tree
	leaf *node
	idx  int
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
