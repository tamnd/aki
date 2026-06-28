package keyspace

import (
	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/format"
)

// Flush empties the database. For each shard it walks the shard tree to free
// overflow chains and update the used-memory estimate, returns the tree pages
// to the freelist, then zeroes the shard so the next write starts fresh. The
// hot-value cache is cleared after all shards are flushed.
func (db *DB) Flush() error {
	if db.newHL != nil {
		// In hybrid mode the keys live in the per-DB hybrid-log store, not the
		// shard trees, so clearing the store is the whole flush. The hot cache is
		// still dropped for parity with the B-tree path.
		if b := db.hl.Load(); b != nil {
			if err := b.e.Clear(); err != nil {
				return err
			}
		}
		db.hc.Load().cclear()
		return nil
	}
	for s := range NumShards {
		db.shards[s].mu.Lock()
		if err := db.flushShard(s); err != nil {
			db.shards[s].mu.Unlock()
			return err
		}
		db.shards[s].mu.Unlock()
	}
	db.hc.Load().cclear()
	return nil
}

// flushShard drains one shard. Caller holds shards[s].mu.Lock().
func (db *DB) flushShard(s int) error {
	t := db.loadShardTree(s)
	if t != nil {
		var overflow []uint32
		var collTrees []uint32
		c := t.Cursor()
		if err := c.First(); err != nil {
			return err
		}
		for c.Valid() {
			if h, _, ok := parseHeader(c.Value()); ok {
				if h.IsColl() && h.BodyRef != 0 {
					// A btree-backed collection's element sub-tree must be dropped
					// so its pages return to the freelist. The meta keeps
					// FlagInlineBody set, so it never enters the overflow branch.
					collTrees = append(collTrees, uint32(h.BodyRef))
				} else if h.Flags&FlagInlineBody == 0 && h.BodyRef != 0 {
					overflow = append(overflow, uint32(h.BodyRef))
				}
				db.ks.dataBytes.Add(-(int64(len(rawKey(c.Key()))) + int64(h.BodyLen) + entryOverhead))
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
		for _, root := range collTrees {
			if err := btree.DropTree(db.ks.pgr, root); err != nil {
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
	db.shards[s].rootPage = format.NullPage
	db.shards[s].tree = nil
	db.shards[s].keyCount.Store(0)
	db.shards[s].expireCount.Store(0)
	db.shards[s].wbPending = nil
	db.shards[s].pendingUncertain.Store(0)
	// The data is gone, so the resident overlay copies are dropped without a fold;
	// there is no sub-tree left to fold into.
	db.shards[s].live = nil
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
	// In hybrid mode the keys live in each DB's hybrid-log store, so the swap is
	// just an exchange of the two store pointers. Build both stores first so
	// neither hl pointer is left nil after the swap: a nil pointer paired with an
	// already-fired hlOnce would never rebuild, and the next write would panic.
	// Storing the pointers is atomic, so a concurrent point read on either DB sees
	// a consistent store either side of the swap.
	if a.newHL != nil {
		if _, err := a.ensureHL(); err != nil {
			return err
		}
		if _, err := b.ensureHL(); err != nil {
			return err
		}
		sa, sb := a.hl.Load(), b.hl.Load()
		a.hl.Store(sb)
		b.hl.Store(sa)
		aHC, bHC := a.hc.Load(), b.hc.Load()
		a.hc.Store(bHC)
		b.hc.Store(aHC)
		return nil
	}
	// Lock all shards of both DBs in a consistent order (lower DB index first,
	// then shard index) to avoid deadlock when two Swaps race.
	first, second := a, b
	if i > j {
		first, second = b, a
	}
	for s := range NumShards {
		first.shards[s].mu.Lock()
		second.shards[s].mu.Lock()
	}
	defer func() {
		for s := range NumShards {
			second.shards[s].mu.Unlock()
			first.shards[s].mu.Unlock()
		}
	}()

	// Swap shard data fields individually — copying the dbShard struct would
	// copy the embedded sync.RWMutex, which is not allowed. The mutexes stay
	// with their respective DB slots and continue to protect them after the swap.
	for s := range NumShards {
		a.shards[s].rootPage, b.shards[s].rootPage = b.shards[s].rootPage, a.shards[s].rootPage
		a.shards[s].tree, b.shards[s].tree = b.shards[s].tree, a.shards[s].tree
		ak, bk := a.shards[s].keyCount.Load(), b.shards[s].keyCount.Load()
		a.shards[s].keyCount.Store(bk)
		b.shards[s].keyCount.Store(ak)
		ae, be := a.shards[s].expireCount.Load(), b.shards[s].expireCount.Load()
		a.shards[s].expireCount.Store(be)
		b.shards[s].expireCount.Store(ae)
		a.shards[s].wbPending, b.shards[s].wbPending = b.shards[s].wbPending, a.shards[s].wbPending
		a.shards[s].live, b.shards[s].live = b.shards[s].live, a.shards[s].live
		au, bu := a.shards[s].pendingUncertain.Load(), b.shards[s].pendingUncertain.Load()
		a.shards[s].pendingUncertain.Store(bu)
		b.shards[s].pendingUncertain.Store(au)
	}
	a.avgTTL, b.avgTTL = b.avgTTL, a.avgTTL
	// Exchange the hot-cache pointers atomically so lock-free hot-GET readers
	// always see a valid (if transiently stale) cache pointer during the swap.
	aHC := a.hc.Load()
	bHC := b.hc.Load()
	a.hc.Store(bHC)
	b.hc.Store(aHC)
	return nil
}
