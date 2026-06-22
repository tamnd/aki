package keyspace

import "bytes"

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

// check walks one database's B-tree, verifying key ordering and value headers and
// tallying TTL state.
func (db *DB) check(now int64) (DBCheck, error) {
	res := DBCheck{Index: db.index}
	t := db.loadTree()
	if t == nil {
		return res, nil
	}
	c := t.Cursor()
	if err := c.First(); err != nil {
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
			return res, err
		}
	}
	return res, nil
}

// FixFutureTTLs clears TTLs that are impossibly far in the future, rewriting
// those keys with no expiry, and commits. It returns the number of keys fixed.
// This is the only repair the checker performs on the keyspace; structural
// corruption is left to a dump and reimport.
func (ks *Keyspace) FixFutureTTLs() (int, error) {
	now := nowMillis()
	fixed := 0
	for _, db := range ks.dbs {
		t := db.loadTree()
		if t == nil {
			continue
		}
		var keys [][]byte
		c := t.Cursor()
		if err := c.First(); err != nil {
			return fixed, err
		}
		for c.Valid() {
			h, _, ok := parseHeader(c.Value())
			if ok && h.HasTTL() && h.TTLms > now+farFutureMs {
				keys = append(keys, copyRaw(c.Key()))
			}
			if err := c.Next(); err != nil {
				return fixed, err
			}
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
