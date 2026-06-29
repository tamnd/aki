package command

import (
	"bytes"

	"github.com/tamnd/aki/keyspace"
)

// ZINTERCARD fell into the same materialize trap as SINTERCARD: loadZSets cloned
// every member of every input sorted set (getZSet), then interCardZ walked the
// first against score maps of the rest. zinterCardBounded counts the intersection
// without materializing any whole set, driving the smallest and answering
// membership in the rest with point lookups on the member index.
//
// A coll-form sorted set keeps a member-index row 'm'+member next to the ordered
// score rows, so membership is a point lookup and the member-prefix rows give the
// distinct members for the driver walk. The structure and the bounds match the set
// version: one reader open at a time, the driver reader closed before any probe, a
// batch and the running count the only retained memory.
func zinterCardBounded(ctx *Ctx, keys [][]byte, limit int) (n int64, wrongTyp bool, ok bool) {
	ok = ctx.view(func(db *keyspace.DB) error {
		cards := make([]int64, len(keys))
		hdrs := make([]keyspace.ValueHeader, len(keys))
		smallest := -1
		for i, k := range keys {
			c, hdr, found, err := zsetCard(db, k)
			if err != nil {
				return err
			}
			if found && hdr.Type != keyspace.TypeZSet {
				wrongTyp = true
				return nil
			}
			if !found || c == 0 {
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
			members, _, _, err := getZSet(db, keys[i])
			if err != nil {
				return err
			}
			blobMaps[i] = zmemberSet(members)
		}

		count := int64(0)
		err := zsetDriveMembers(db, keys[smallest], hdrs[smallest], func(batch [][]byte) (bool, error) {
			survivors := batch
			for i := range keys {
				if m, isBlob := blobMaps[i]; isBlob {
					survivors = keepInMembership(survivors, m)
					if len(survivors) == 0 {
						return false, nil
					}
				}
			}
			for _, j := range collOthers {
				present, e := zsetProbeColl(db, keys[j], survivors)
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

// zsetDriveMembers calls fn with the members of the sorted set at key in batches,
// the zset analogue of setDriveMembers. A coll set is walked over its member-index
// rows ('m' prefix) through an arena cursor, one batch at a time, with the reader
// closed before fn runs. A blob set is small and goes to fn in one batch.
func zsetDriveMembers(db *keyspace.DB, key []byte, hdr keyspace.ValueHeader, fn func(batch [][]byte) (stop bool, err error)) error {
	if !hdr.IsColl() {
		members, _, _, err := getZSet(db, key)
		if err != nil {
			return err
		}
		batch := make([][]byte, len(members))
		for i, zm := range members {
			batch[i] = zm.member
		}
		_, err = fn(batch)
		return err
	}
	var resume []byte // the last member row key visited, for Seek-resume
	haveResume := false
	for {
		batch := make([][]byte, 0, interCardBatch)
		var lastRow []byte
		_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
			c := r.Cursor()
			c.UseArena()
			if haveResume {
				if e := c.Seek(resume); e != nil {
					return e
				}
				if c.Valid() && bytes.Equal(c.Key(), resume) {
					if e := c.Next(); e != nil {
						return e
					}
				}
			} else {
				if e := c.Seek([]byte{zRowMember}); e != nil {
					return e
				}
			}
			for c.Valid() && len(batch) < interCardBatch {
				k := c.Key()
				if len(k) == 0 || k[0] != zRowMember {
					// Walked past the member rows into the score rows; the set is done.
					break
				}
				batch = append(batch, append([]byte(nil), k[1:]...))
				lastRow = append([]byte(nil), k...)
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
		resume = lastRow
		haveResume = true
	}
}

// zsetProbeColl reports, for each member, whether it is in the coll sorted set at
// key, via a point lookup on the member-index row. The whole set is never walked.
func zsetProbeColl(db *keyspace.DB, key []byte, members [][]byte) ([]bool, error) {
	present := make([]bool, len(members))
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		for i, m := range members {
			_, p, e := r.Get(zMemberRow(m))
			if e != nil {
				return e
			}
			present[i] = p
		}
		return nil
	})
	return present, err
}

// zmemberSet builds a membership set keyed by member bytes from a blob sorted set's
// decoded members.
func zmemberSet(members []zmember) map[string]struct{} {
	set := make(map[string]struct{}, len(members))
	for _, zm := range members {
		set[string(zm.member)] = struct{}{}
	}
	return set
}
