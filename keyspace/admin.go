package keyspace

import "github.com/tamnd/aki/format"

// Flush empties the database. It drops the B-tree root and zeroes the counters so
// the next write starts a fresh tree. The old pages are orphaned until the page
// reclamation milestone adds a free list, the same gap UNLINK lives with.
func (db *DB) Flush() {
	db.rootPage = format.NullPage
	db.tree = nil
	db.keyCount = 0
	db.expireCount = 0
	db.avgTTL = 0
}

// Swap exchanges the contents of two databases in place, leaving their indexes
// fixed, so a client on index i sees what was in index j afterward. Swapping a
// database with itself does nothing.
func (ks *Keyspace) Swap(i, j int) error {
	a, err := ks.DB(i)
	if err != nil {
		return err
	}
	b, err := ks.DB(j)
	if err != nil {
		return err
	}
	if a == b {
		return nil
	}
	a.rootPage, b.rootPage = b.rootPage, a.rootPage
	a.tree, b.tree = b.tree, a.tree
	a.keyCount, b.keyCount = b.keyCount, a.keyCount
	a.expireCount, b.expireCount = b.expireCount, a.expireCount
	a.avgTTL, b.avgTTL = b.avgTTL, a.avgTTL
	return nil
}
