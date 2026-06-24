package btree

import "github.com/tamnd/aki/pager"

// DropTree frees every page of the tree rooted at root, returning them to the
// pager freelist. The keyspace calls it to tear down a per-key collection
// sub-tree when the collection is deleted or replaced, so the sub-tree's pages
// do not leak (the engine has no compaction filter to reclaim them lazily).
//
// The walk is post-order: a node's children are freed before the node itself, so
// a page is only read while it is still reachable. The frees are recorded in the
// in-memory freelist and become durable with the surrounding commit, the same as
// every other page change made under the engine lock. After DropTree the root is
// invalid and must not be reused.
func DropTree(pgr *pager.Pager, root uint32) error {
	return dropNode(pgr, root)
}

// dropNode frees the page at pgno and, when it is an interior node, every page
// reachable beneath it. Recursion depth is the tree height (a handful of levels
// even for billions of keys), while the fan-out over children is iterative, so
// the stack stays shallow regardless of collection size.
func dropNode(pgr *pager.Pager, pgno uint32) error {
	pg, err := pgr.Get(pgno)
	if err != nil {
		return err
	}
	n, err := decodeNode(pg.Data)
	pgr.Unpin(pg, false)
	if err != nil {
		return err
	}
	if !n.leaf {
		for _, c := range n.children {
			if err := dropNode(pgr, c); err != nil {
				return err
			}
		}
	}
	return pgr.Free(pgno)
}
