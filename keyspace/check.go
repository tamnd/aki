package keyspace

import (
	"bytes"
	"fmt"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/format"
)

// farFutureMs is the cutoff past which a TTL is treated as impossible. A key set
// to expire more than a century from now almost certainly carries a corrupt
// timestamp, so the checker flags it and the fix path clears it.
const farFutureMs int64 = 100 * 365 * 24 * 3600 * 1000

// DBCheck is the integrity result for one database. The counts come from a single
// in-order walk of the B-tree.
type DBCheck struct {
	Index       int
	Entries     int // total tree entries walked
	Live        int // entries that are not expired
	Expires     int // entries carrying a TTL
	StaleTTL    int // entries whose TTL is already in the past
	FutureTTL   int // entries with an impossibly far-future TTL
	BadHeaders  int // entries whose value header failed to parse
	OrderErrors int // entries out of composite-key order
	// StructErr is set when the B-tree itself is malformed (bad child counts,
	// keys outside their range, a broken leaf chain). It is independent of the
	// per-entry counts above, which assume a walkable tree.
	StructErr error
}

// Check walks every database B-tree in order and reports the per-database
// integrity counts. A traversal error (an unreadable page) is returned so the
// caller can treat it as structural corruption.
func (ks *Keyspace) Check() ([]DBCheck, error) {
	now := nowMillis()
	out := make([]DBCheck, 0, len(ks.dbs))
	for _, db := range ks.dbs {
		c, err := db.check(now)
		if err != nil {
			return out, err
		}
		out = append(out, c)
	}
	return out, nil
}

// check walks all shards of one database, verifying key ordering and value
// headers and tallying TTL state. Each shard is checked under its read lock.
func (db *DB) check(now int64) (DBCheck, error) {
	res := DBCheck{Index: db.index}
	for s := range NumShards {
		db.shards[s].mu.RLock()
		t := db.loadShardTree(s)
		if t == nil {
			db.shards[s].mu.RUnlock()
			continue
		}
		if err := btree.CheckInvariants(t); err != nil {
			res.StructErr = err
		}
		c := t.Cursor()
		if err := c.First(); err != nil {
			db.shards[s].mu.RUnlock()
			return res, err
		}
		var prev []byte
		for c.Valid() {
			ck := c.Key()
			if prev != nil && bytes.Compare(ck, prev) <= 0 {
				res.OrderErrors++
			}
			prev = bytes.Clone(ck)
			res.Entries++

			h, _, ok := parseHeader(c.Value())
			if !ok {
				res.BadHeaders++
			} else if h.HasTTL() {
				res.Expires++
				switch {
				case h.TTLms <= now:
					res.StaleTTL++
				case h.TTLms > now+farFutureMs:
					res.FutureTTL++
					res.Live++
				default:
					res.Live++
				}
			} else {
				res.Live++
			}

			if err := c.Next(); err != nil {
				db.shards[s].mu.RUnlock()
				return res, err
			}
		}
		db.shards[s].mu.RUnlock()
	}
	return res, nil
}

// CheckPageAccounting proves every page in the file is accounted for exactly
// once: either live (reachable from the catalog, a database B-tree, or a value's
// overflow chain) or free (on the freelist), never both and never neither. It is
// the page-level form of doc 23 section 9.3, run on demand by the integrity
// checker and after every commit in debug builds.
//
// The three faults it catches are a page that is reachable and also on the
// freelist (a use-after-free waiting to happen), a page that two structures both
// claim as live (a double reference), and a page that is neither reachable nor
// free (a leak). The freelist's own no-duplicate rule is checked here too.
func (ks *Keyspace) CheckPageAccounting() error {
	pageCount := ks.pgr.PageCount()
	live := make(map[uint32]bool, pageCount)

	// Pages 0, 1 and 2 are the header and the two meta slots. They are always in
	// use and never appear in either the tree or the freelist.
	live[0] = true
	live[format.MetaPageA] = true
	live[format.MetaPageB] = true

	mark := func(pgno uint32, what string) error {
		if live[pgno] {
			return fmt.Errorf("page %d claimed live by two structures (%s)", pgno, what)
		}
		live[pgno] = true
		return nil
	}

	if ks.catRoot != format.NullPage {
		if err := mark(ks.catRoot, "catalog"); err != nil {
			return err
		}
	}

	if t := ks.systemTree(); t != nil {
		pages, err := btree.Pages(t)
		if err != nil {
			return fmt.Errorf("system table: %w", err)
		}
		for _, pgno := range pages {
			if err := mark(pgno, "system table"); err != nil {
				return err
			}
		}
	}

	for _, db := range ks.dbs {
		for s := range NumShards {
			db.shards[s].mu.RLock()
			t := db.loadShardTree(s)
			if t == nil {
				db.shards[s].mu.RUnlock()
				continue
			}
			pages, err := btree.Pages(t)
			if err != nil {
				db.shards[s].mu.RUnlock()
				return fmt.Errorf("db%d shard%d: %w", db.index, s, err)
			}
			for _, pgno := range pages {
				if err := mark(pgno, fmt.Sprintf("db%d shard%d tree", db.index, s)); err != nil {
					db.shards[s].mu.RUnlock()
					return err
				}
			}
			c := t.Cursor()
			if err := c.First(); err != nil {
				db.shards[s].mu.RUnlock()
				return fmt.Errorf("db%d shard%d cursor: %w", db.index, s, err)
			}
			for c.Valid() {
				h, _, ok := parseHeader(c.Value())
				if ok && h.Flags&FlagInlineBody == 0 && h.BodyRef != 0 {
					if err := ks.markOverflowChain(uint32(h.BodyRef), pageCount, mark); err != nil {
						db.shards[s].mu.RUnlock()
						return fmt.Errorf("db%d shard%d overflow: %w", db.index, s, err)
					}
				}
				if err := c.Next(); err != nil {
					db.shards[s].mu.RUnlock()
					return fmt.Errorf("db%d shard%d cursor: %w", db.index, s, err)
				}
			}
			db.shards[s].mu.RUnlock()
		}
	}

	free := make(map[uint32]bool, ks.pgr.FreeCount())
	for _, pgno := range ks.pgr.FreePages() {
		if free[pgno] {
			return fmt.Errorf("page %d appears twice on the freelist", pgno)
		}
		free[pgno] = true
		if live[pgno] {
			return fmt.Errorf("page %d is live and on the freelist", pgno)
		}
	}

	for pgno := uint32(0); pgno < pageCount; pgno++ {
		if !live[pgno] && !free[pgno] {
			return fmt.Errorf("page %d is leaked: neither reachable nor free", pgno)
		}
	}
	return nil
}

// markOverflowChain walks a value's overflow chain from head and marks each page
// live. It bounds the walk by the file's page count so a corrupt next-pointer
// that forms a cycle is reported rather than looped on forever.
func (ks *Keyspace) markOverflowChain(head, pageCount uint32, mark func(uint32, string) error) error {
	pgno := head
	for steps := uint32(0); pgno != format.NullPage; steps++ {
		if steps > pageCount {
			return fmt.Errorf("overflow chain from %d is longer than the file, likely a cycle", head)
		}
		if pgno >= pageCount {
			return fmt.Errorf("overflow page %d is out of range (page count %d)", pgno, pageCount)
		}
		if err := mark(pgno, "overflow"); err != nil {
			return err
		}
		pg, err := ks.pgr.Get(pgno)
		if err != nil {
			return err
		}
		next := encoding.U32(pg.Data[ovNextOffset:])
		ks.pgr.Unpin(pg, false)
		pgno = next
	}
	return nil
}

// FixFutureTTLs clears TTLs that are impossibly far in the future, rewriting
// those keys with no expiry, and commits. It returns the number of keys fixed.
// This is the only repair the checker performs on the keyspace; structural
// corruption is left to a dump and reimport.
func (ks *Keyspace) FixFutureTTLs() (int, error) {
	now := nowMillis()
	fixed := 0
	for _, db := range ks.dbs {
		var keys [][]byte
		for s := range NumShards {
			db.shards[s].mu.RLock()
			t := db.loadShardTree(s)
			if t == nil {
				db.shards[s].mu.RUnlock()
				continue
			}
			c := t.Cursor()
			if err := c.First(); err != nil {
				db.shards[s].mu.RUnlock()
				return fixed, err
			}
			for c.Valid() {
				h, _, ok := parseHeader(c.Value())
				if ok && h.HasTTL() && h.TTLms > now+farFutureMs {
					keys = append(keys, copyRaw(c.Key()))
				}
				if err := c.Next(); err != nil {
					db.shards[s].mu.RUnlock()
					return fixed, err
				}
			}
			db.shards[s].mu.RUnlock()
		}
		for _, key := range keys {
			body, h, found, err := db.Peek(key)
			if err != nil {
				return fixed, err
			}
			if !found {
				continue
			}
			if err := db.Set(key, body, h.Type, h.Encoding, -1); err != nil {
				return fixed, err
			}
			fixed++
		}
	}
	if fixed > 0 {
		if err := ks.Commit(); err != nil {
			return fixed, err
		}
	}
	return fixed, nil
}
