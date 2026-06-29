package command

import (
	"github.com/tamnd/aki/keyspace"
)

// A large set is stored element-per-row in the key's btree-backed collection
// sub-tree (keyspace.CollUpdate / CollRead): one row per member,
//
//	member -> (empty)
//
// A set carries no per-member value, so the row value is empty and the presence
// of the row is the membership. A small set keeps the single-blob form in
// set_codec.go. A set promotes to the sub-tree exactly when its reported encoding
// becomes hashtable, so OBJECT ENCODING flips at the same threshold as Redis and
// never demotes.
//
// getSet (set_codec.go) is coll-aware, so every read caller (SMEMBERS, SSCAN,
// SORT, the set algebra, DUMP/RDB) works on either form with no change. The write
// commands branch on hdr.IsColl() before getSet so they never rewrite a whole
// blob for a btree-backed set; they do point sub-tree ops and maintain the count.

// setWantsTree reports whether a set with these members should live in the
// btree-backed form. The rule is the encoding rule: a set is tree-backed exactly
// when it reports hashtable, so promotion happens at the listpack threshold and
// the encoding name stays correct for free.
func setWantsTree(lim encLimits, members [][]byte, prevEnc uint8) bool {
	return setEncoding(lim, members, prevEnc) == keyspace.EncHashtable
}

// setPromote moves a set from the blob form to the btree-backed form. It writes
// every member as a sub-tree row through CollUpdate, which creates the fresh
// sub-tree, frees the old blob, and carries over the key's TTL. Callers reach it
// when an applied write pushes the member set past the hashtable threshold.
func setPromote(db *keyspace.DB, key []byte, members [][]byte) error {
	return db.CollUpdate(key, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
		for _, m := range members {
			if _, e := w.Put(m, nil); e != nil {
				return e
			}
		}
		w.SetCount(uint64(len(members)))
		return nil
	})
}

// setHeader probes the value header at key without decoding the body, so a write
// command can route to the blob path or the sub-tree path. found is false for a
// missing key.
func setHeader(db *keyspace.DB, key []byte) (keyspace.ValueHeader, bool, error) {
	return db.CollMetaHeader(key)
}

// collectSetMembers walks a btree-backed set's sub-tree and returns every member
// in tree order. The caller has already confirmed the key is a coll set.
func collectSetMembers(db *keyspace.DB, key []byte) ([][]byte, error) {
	var members [][]byte
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		if e := c.First(); e != nil {
			return e
		}
		for c.Valid() {
			members = append(members, append([]byte(nil), c.Key()...))
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return members, err
}

// setCard returns the member count in whichever form the set is stored. For a
// btree-backed set it reads the metadata count in O(1) rather than walking.
func setCard(db *keyspace.DB, key []byte) (n int64, hdr keyspace.ValueHeader, keyFound bool, err error) {
	hdr, keyFound, err = setHeader(db, key)
	if err != nil || !keyFound {
		return 0, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeSet {
		return 0, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			n = int64(r.Count())
			return nil
		})
		return n, hdr, true, err
	}
	members, _, _, e := getSet(db, key)
	if e != nil {
		return 0, hdr, true, e
	}
	return int64(len(members)), hdr, true, nil
}

// setMemberIn reports whether member is in the set at key. For a btree-backed set
// it is a point sub-tree lookup; for a blob it decodes and scans. The caller has
// already confirmed the key holds a set.
func setMemberIn(db *keyspace.DB, key, member []byte, hdr keyspace.ValueHeader) (bool, error) {
	if hdr.IsColl() {
		present := false
		_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
			_, p, e := r.Get(member)
			present = p
			return e
		})
		return present, err
	}
	members, _, _, err := getSet(db, key)
	if err != nil {
		return false, err
	}
	return setFind(members, member) >= 0, nil
}

// setMembership answers a batch of membership queries against the set at key
// with the cheapest path for the storage form, never materializing the whole
// set. A btree-backed set answers each query with an O(log n) point lookup
// inside one sub-tree reader; a small blob set decodes once and scans. This is
// the difference between O(q) point lookups and an O(n) walk of every member,
// which on a multi-million-member set is the difference between a few page
// reads and dragging the entire set through memory on every SISMEMBER.
//
// present is filled per query (false for an absent key). wrongTyp reports a
// non-set value at key. ok is false only when the underlying view failed.
func setMembership(ctx *Ctx, key []byte, queries [][]byte) (present []bool, wrongTyp bool, ok bool) {
	present = make([]bool, len(queries))
	// A small set may be served straight from the lock-free hot cache; hotGetSet
	// returns a miss for the coll form, so a hit here is always the blob form.
	if ms, hit := hotGetSet(ctx, key); hit {
		for i, q := range queries {
			present[i] = setFind(ms, q) >= 0
		}
		return present, false, true
	}
	ok = ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := setHeader(db, key)
		if err != nil || !found {
			return err
		}
		if hdr.Type != keyspace.TypeSet {
			wrongTyp = true
			return nil
		}
		if hdr.IsColl() {
			// One reader, a point lookup per query: the whole sub-tree is never walked.
			_, e := db.CollRead(key, func(r *keyspace.CollReader) error {
				for i, q := range queries {
					_, p, ge := r.Get(q)
					if ge != nil {
						return ge
					}
					present[i] = p
				}
				return nil
			})
			return e
		}
		members, _, _, e := getSet(db, key)
		if e != nil {
			return e
		}
		for i, q := range queries {
			present[i] = setFind(members, q) >= 0
		}
		return nil
	})
	return present, wrongTyp, ok
}

// setDelOne removes one known-present member from the set at key and reports
// whether that emptied the key. It handles both storage forms and is used by
// SMOVE, which moves a single member across keys.
func setDelOne(lim encLimits, db *keyspace.DB, key, member []byte, hdr keyspace.ValueHeader) (emptied bool, err error) {
	if hdr.IsColl() {
		err = db.CollUpdate(key, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
			existed, e := w.Delete(member)
			if e != nil {
				return e
			}
			if existed {
				w.SetCount(w.Count() - 1)
			}
			emptied = w.Count() == 0
			return nil
		})
		return emptied, err
	}
	members, mhdr, _, err := getSet(db, key)
	if err != nil {
		return false, err
	}
	if idx := setFind(members, member); idx >= 0 {
		members = append(members[:idx], members[idx+1:]...)
	}
	if len(members) == 0 {
		_, err = db.Delete(key)
		return true, err
	}
	return false, db.Set(key, setEncode(members), keyspace.TypeSet,
		setEncoding(lim, members, mhdr.Encoding), keepTTL(mhdr, true))
}

// setAddOne adds member to the set at key when it is not already present,
// promoting a blob that crosses the hashtable threshold. It handles both storage
// forms and a missing key (a fresh set). It is used by SMOVE.
func setAddOne(lim encLimits, db *keyspace.DB, key, member []byte, hdr keyspace.ValueHeader, found bool) error {
	if found && hdr.IsColl() {
		return db.CollUpdate(key, keyspace.TypeSet, keyspace.EncHashtable, func(w *keyspace.CollWriter) error {
			created, e := w.Put(member, nil)
			if e != nil {
				return e
			}
			if created {
				w.SetCount(w.Count() + 1)
			}
			return nil
		})
	}
	members, mhdr, _, err := getSet(db, key)
	if err != nil {
		return err
	}
	if setFind(members, member) >= 0 {
		return nil
	}
	members = append(members, member)
	prev := uint8(keyspace.EncIntset)
	if found {
		prev = mhdr.Encoding
	}
	if setWantsTree(lim, members, prev) {
		if !found {
			if err := db.Set(key, nil, keyspace.TypeSet, keyspace.EncHashtable, -1); err != nil {
				return err
			}
		}
		return setPromote(db, key, members)
	}
	return db.Set(key, setEncode(members), keyspace.TypeSet,
		setEncoding(lim, members, prev), keepTTL(mhdr, found))
}
