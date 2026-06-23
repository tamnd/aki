package keyspace

import (
	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/format"
)

// Flush empties the database. It walks the tree once to free every overflow
// chain and to drop the freed keys from the used-memory estimate, returns the
// tree's own pages to the freelist, then drops the root and zeroes the counters
// so the next write starts a fresh tree. Before page reclamation this path
// orphaned the whole tree, the one place page accounting could not hold; now the
// pages go back on the freelist like an UNLINK frees its overflow chain.
func (db *DB) Flush() error {
	if t := db.loadTree(); t != nil {
		var overflow []uint32
		c := t.Cursor()
		if err := c.First(); err != nil {
			return err
		}
		for c.Valid() {
			if h, _, ok := parseHeader(c.Value()); ok {
				if h.Flags&FlagInlineBody == 0 && h.BodyRef != 0 {
					overflow = append(overflow, uint32(h.BodyRef))
				}
				db.ks.dataBytes -= int64(len(rawKey(c.Key()))) + int64(h.BodyLen) + entryOverhead
			}
			if err := c.Next(); err != nil {
				return err
			}
		}
		for _, head := range overflow {
			if err := db.ks.freeOverflow(head); err != nil {
				return err
			}
		}
		pages, err := btree.Pages(t)
		if err != nil {
			return err
		}
		for _, pgno := range pages {
			if err := db.ks.pgr.Free(pgno); err != nil {
				return err
			}
		}
	}
	db.rootPage = format.NullPage
	db.tree = nil
	db.keyCount = 0
	db.expireCount = 0
	db.avgTTL = 0
	db.hc.cclear()
	return nil
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
	a.hc, b.hc = b.hc, a.hc
	return nil
}
