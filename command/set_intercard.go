package command

import (
	"bytes"

	"github.com/tamnd/aki/keyspace"
)

// The materialize trap SINTERCARD used to fall into: loadSets cloned every member
// of every input set onto the heap (getSet), then intersectCount walked the first
// set against in-memory membership maps of the rest. Two multi-million-member sets
// dragged both sets through memory in full before a single member was counted, so a
// SINTERCARD with a small LIMIT still OOM-killed under a tight cap.
//
// sinterCardBounded counts the intersection without materializing any whole set. It
// reads each set's cardinality in O(1) for the coll form, drives the smallest set,
// and answers membership in the rest with point lookups. The smallest set is walked
// in copied batches, the driver reader is closed before any probe runs (never a
// nested CollRead on the same shard, which would self-deadlock on the shard's read
// lock), and only one reader is ever open at a time. Memory is bounded by a batch
// and the running count, so the intersection of huge coll sets stays flat.
//
// A blob set is below the hashtable threshold, so it holds few members; those are
// materialized into a membership map once and probed in memory. A coll set is never
// materialized: it is the driver (walked in batches) or a point-probe target.
const interCardBatch = 256

func sinterCardBounded(ctx *Ctx, keys [][]byte, limit int) (n int64, wrongTyp bool, ok bool) {
	ok = ctx.view(func(db *keyspace.DB) error {
		cards := make([]int64, len(keys))
		hdrs := make([]keyspace.ValueHeader, len(keys))
		smallest := -1
		for i, k := range keys {
			c, hdr, found, err := setCard(db, k)
			if err != nil {
				return err
			}
			if found && hdr.Type != keyspace.TypeSet {
				wrongTyp = true
				return nil
			}
			if !found || c == 0 {
				// A missing or empty set makes the whole intersection empty.
				return nil
			}
			cards[i] = c
			hdrs[i] = hdr
			if smallest < 0 || c < cards[smallest] {
				smallest = i
			}
		}
		if smallest < 0 {
			return nil
		}

		// Build membership maps for the small blob sets once (bounded by the listpack
		// threshold); probe the coll sets per batch with point lookups.
		blobMaps := make(map[int]map[string]struct{}, len(keys))
		collOthers := make([]int, 0, len(keys))
		for i := range keys {
			if i == smallest {
				continue
			}
			if hdrs[i].IsColl() {
				collOthers = append(collOthers, i)
				continue
			}
			members, _, _, err := getSet(db, keys[i])
			if err != nil {
				return err
			}
			blobMaps[i] = toMembership(members)
		}

		count := int64(0)
		err := setDriveMembers(db, keys[smallest], hdrs[smallest], func(batch [][]byte) (bool, error) {
			survivors := batch
			// Cheap in-memory blob filters first to shrink the batch before the
			// per-member point probes against the coll sets.
			for i := range keys {
				if m, isBlob := blobMaps[i]; isBlob {
					survivors = keepInMembership(survivors, m)
					if len(survivors) == 0 {
						return false, nil
					}
				}
			}
			for _, j := range collOthers {
				present, e := setProbeColl(db, keys[j], survivors)
				if e != nil {
					return false, e
				}
				survivors = keepPresent(survivors, present)
				if len(survivors) == 0 {
					return false, nil
				}
			}
			count += int64(len(survivors))
			if limit > 0 && count >= int64(limit) {
				count = int64(limit)
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return err
		}
		n = count
		return nil
	})
	return n, wrongTyp, ok
}

// setDriveMembers calls fn with the members of the set at key in batches. A coll set
// is walked through an arena-backed cursor, copying one batch of members at a time
// and closing the reader before fn runs, so fn is free to open readers on other keys
// without nesting a CollRead inside this one. A blob set is small, so its members go
// to fn in a single batch. fn returns stop to end the walk early (the LIMIT hit).
func setDriveMembers(db *keyspace.DB, key []byte, hdr keyspace.ValueHeader, fn func(batch [][]byte) (stop bool, err error)) error {
	if !hdr.IsColl() {
		members, _, _, err := getSet(db, key)
		if err != nil {
			return err
		}
		_, err = fn(members)
		return err
	}
	var resume []byte
	haveResume := false
	for {
		batch := make([][]byte, 0, interCardBatch)
		var last []byte
		_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
			c := r.Cursor()
			c.UseArena()
			if haveResume {
				if e := c.Seek(resume); e != nil {
					return e
				}
				// Seek lands on resume itself when it is still present; skip it so the
				// batch starts at the next member.
				if c.Valid() && bytes.Equal(c.Key(), resume) {
					if e := c.Next(); e != nil {
						return e
					}
				}
			} else {
				if e := c.First(); e != nil {
					return e
				}
			}
			for c.Valid() && len(batch) < interCardBatch {
				m := append([]byte(nil), c.Key()...)
				batch = append(batch, m)
				last = m
				if e := c.Next(); e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		stop, err := fn(batch)
		if err != nil {
			return err
		}
		if stop || len(batch) < interCardBatch {
			return nil
		}
		resume = last
		haveResume = true
	}
}

// setProbeColl reports, for each member, whether it is in the coll set at key. It
// opens one reader and does a point lookup per member, so the whole set is never
// walked. The caller has confirmed key holds a coll-form set.
func setProbeColl(db *keyspace.DB, key []byte, members [][]byte) ([]bool, error) {
	present := make([]bool, len(members))
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		for i, m := range members {
			_, p, e := r.Get(m)
			if e != nil {
				return e
			}
			present[i] = p
		}
		return nil
	})
	return present, err
}

// keepInMembership filters members down to those present in set, filtering in place
// over the backing array (the caller passes a batch it no longer needs intact).
func keepInMembership(members [][]byte, set map[string]struct{}) [][]byte {
	kept := members[:0]
	for _, m := range members {
		if _, ok := set[string(m)]; ok {
			kept = append(kept, m)
		}
	}
	return kept
}

// keepPresent filters members down to those whose present flag is set, in place.
func keepPresent(members [][]byte, present []bool) [][]byte {
	kept := members[:0]
	for i, m := range members {
		if present[i] {
			kept = append(kept, m)
		}
	}
	return kept
}
