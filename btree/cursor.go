package btree

import (
	"bytes"

	"github.com/tamnd/aki/format"
)

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
	t     *Tree
	leaf  *node
	idx   int
	path  []frame
	arena *nodeArena
}

// frame records an interior node and the child index a backward descent took
// through it, so Prev can climb back up and step into the previous subtree.
type frame struct {
	n   *node
	idx int
}

// Cursor returns a new, unpositioned cursor. Call First or Seek before reading.
func (t *Tree) Cursor() *Cursor { return &Cursor{t: t} }

// UseArena makes the cursor decode each visited node into a single reused scratch
// buffer instead of allocating fresh key and value slices per cell per leaf. It is
// for a single-direction walk and the caller must copy any Key/Value it keeps, as
// those bytes alias the arena.
//
// Forward (First/Seek then Next): each leaf step resets the arena and retires the
// previous leaf, so the buffer stays small no matter how many leaves the scan
// spans. Backward (Last/SeekForPrev then Prev): the live leaf alone lives in the
// arena and is reset on each leaf boundary, while the root-to-leaf path nodes Prev
// keeps live are decoded children-only onto the heap so they survive those resets
// (see readNav and readSeekBack). The arena therefore stays bounded in both
// directions, so a whole-set reverse walk holds one leaf plus the O(height) path,
// never O(visited-leaves). Do not interleave the two directions on one arena-backed
// cursor.
func (c *Cursor) UseArena() {
	if c.arena == nil {
		c.arena = &nodeArena{buf: make([]byte, 0, 8192), tmp: make([]byte, 0, 256)}
	}
}

// readNode reads a node honoring the forward arena when one is set. The arena is
// reset by the caller at each descent/step boundary, not here, so a single
// root-to-leaf descent shares one buffer.
func (c *Cursor) readNode(pgno uint32) (*node, error) {
	if c.arena != nil {
		return c.t.readNodeAr(pgno, c.arena)
	}
	return c.t.readNode(pgno)
}

// readSeekBack reads a node during SeekForPrev's descent, where each interior step
// must pick a child by comparing key against the separators. A leaf is decoded into
// the arena (its keys are needed to position idx). An interior is searched on the
// page with descendOnPage, which finds the descent index without cloning any
// separator, and is decoded children-only so the frame still lets climbPrev step
// left through the path. So a reverse seek positions in O(height) page reads with no
// per-separator allocation, the same on-page descent the point read (Get) uses.
// ci and child are meaningful only for an interior result.
func (c *Cursor) readSeekBack(pgno uint32, key []byte) (n *node, ci int, child uint32, err error) {
	if c.arena == nil {
		n, err = c.t.readNode(pgno)
		if err != nil || n.leaf {
			return n, 0, 0, err
		}
		ci = n.childIndex(key)
		return n, ci, n.children[ci], nil
	}
	pg, err := c.t.pgr.Get(pgno)
	if err != nil {
		return nil, 0, 0, err
	}
	defer c.t.pgr.Unpin(pg, false)
	if pg.Data[0] == format.PageTypeBTreeLeaf {
		c.arena.reset()
		n, err = decodeNodeAr(pg.Data, c.arena)
		return n, 0, 0, err
	}
	ci, child, err = descendOnPage(pg.Data, key)
	if err != nil {
		return nil, 0, 0, err
	}
	n, err = decodeInteriorChildren(pg.Data)
	if err != nil {
		return nil, 0, 0, err
	}
	return n, ci, child, nil
}

// readNav reads a node for the backward navigation steps (descendLast,
// descendRightmost) that need only child pointers, never a separator key. A leaf
// is decoded into the arena after a reset, as readBack does; an interior is
// decoded children-only, so the path frames carry no cloned separator bytes. That
// keeps a whole-set reverse walk allocation-flat in the key count rather than
// O(keys-visited), the clone that otherwise dominates a full reverse dump.
// SeekForPrev keeps readBack because its descent picks the child by key.
func (c *Cursor) readNav(pgno uint32) (*node, error) {
	if c.arena == nil {
		return c.t.readNode(pgno)
	}
	pg, err := c.t.pgr.Get(pgno)
	if err != nil {
		return nil, err
	}
	defer c.t.pgr.Unpin(pg, false)
	if pg.Data[0] == format.PageTypeBTreeLeaf {
		c.arena.reset()
		return decodeNodeAr(pg.Data, c.arena)
	}
	return decodeInteriorChildren(pg.Data)
}

// First positions the cursor at the smallest key.
func (c *Cursor) First() error {
	if c.arena != nil {
		c.arena.reset()
	}
	pgno := c.t.root
	for {
		n, err := c.readNode(pgno)
		if err != nil {
			return err
		}
		if n.leaf {
			c.leaf = n
			c.idx = 0
			return nil
		}
		pgno = n.children[0]
	}
}

// Seek positions the cursor at the first key greater than or equal to key.
func (c *Cursor) Seek(key []byte) error {
	if c.arena != nil {
		c.arena.reset()
	}
	pgno := c.t.root
	for {
		n, err := c.readNode(pgno)
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
		// Forward-only: stepping to the sibling retires the current leaf, so the
		// arena can be reset and reused for it. A caller that needs bytes from the
		// old leaf has already copied them out before calling Next.
		if c.arena != nil {
			c.arena.reset()
		}
		n, err := c.readNode(sib)
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
	if c.arena != nil {
		c.arena.reset()
	}
	return c.descendLast(c.t.root)
}

// SeekForPrev positions the cursor at the largest key less than or equal to key,
// or marks it exhausted when every key is greater. It records the path so Prev
// continues the backward walk, so use it to start a reverse range scan at the
// upper bound.
func (c *Cursor) SeekForPrev(key []byte) error {
	c.path = c.path[:0]
	if c.arena != nil {
		c.arena.reset()
	}
	pgno := c.t.root
	for {
		n, ci, child, err := c.readSeekBack(pgno, key)
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
		c.path = append(c.path, frame{n, ci})
		pgno = child
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
		n, err := c.readNav(pgno)
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
		n, err := c.readNav(pgno)
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
