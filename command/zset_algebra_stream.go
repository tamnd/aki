package command

import (
	"bytes"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// ZUNION, ZINTER and ZDIFF used to call loadZSets, which clones every member and
// score of every source onto the heap, then combine the slices. On a coll-form
// sorted set larger than RAM that is an instant OOM, the same trap SINTER/SUNION/
// SDIFF fell into before note 336. These stream the result without cloning a whole
// source: ZINTER drives the smallest source and answers the rest with point score
// lookups, ZDIFF drives the first and probes the rest for presence, ZUNION streams
// each source through batches into the one accumulator the distinct result needs.
//
// The aggregation modes (SUM, MIN, MAX) are all commutative and associative, so the
// driver order does not have to be the argument order: ZINTER can drive whichever
// source is smallest and fold the others in any order and still match Redis.
//
// A blob source (below the listpack threshold) is small by construction, so it is
// mapped once and probed in memory; only the coll sources are point-probed or
// streamed. One coll reader is open at a time and the driver reader is always closed
// before any probe, the same lock discipline as the set algebra and ZINTERCARD.

// zsetScored carries a member alongside the score read from its member-index row.
type zsetScored struct {
	member []byte
	score  float64
}

// zsetDriveScored calls fn with batches of (member, score) from the sorted set at
// key, the scored analogue of zsetDriveMembers. A coll set is walked over its
// member-index rows ('m' + member -> score bytes) through an arena cursor one batch
// at a time, with the reader closed before fn runs; a blob set goes to fn in one
// batch. The member and score bytes are copied out, so they outlive the page.
func zsetDriveScored(db *keyspace.DB, key []byte, hdr keyspace.ValueHeader, fn func(batch []zsetScored) (stop bool, err error)) error {
	if !hdr.IsColl() {
		members, _, _, err := getZSet(db, key)
		if err != nil {
			return err
		}
		batch := make([]zsetScored, len(members))
		for i, zm := range members {
			batch[i] = zsetScored(zm)
		}
		_, err = fn(batch)
		return err
	}
	var resume []byte // the last member row key visited, for Seek-resume
	haveResume := false
	for {
		batch := make([]zsetScored, 0, interCardBatch)
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
				v := c.Value()
				batch = append(batch, zsetScored{
					member: append([]byte(nil), k[1:]...),
					score:  zScoreUnbits(encoding.U64BE(v)),
				})
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

// zsetProbeScores reports, for each member, whether it is in the coll sorted set at
// key and its score, via a point lookup on the member-index row. The whole set is
// never walked.
func zsetProbeScores(db *keyspace.DB, key []byte, members [][]byte) (scores []float64, present []bool, err error) {
	scores = make([]float64, len(members))
	present = make([]bool, len(members))
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		for i, m := range members {
			v, p, e := r.Get(zMemberRow(m))
			if e != nil {
				return e
			}
			if p {
				scores[i] = zScoreUnbits(encoding.U64BE(v))
				present[i] = true
			}
		}
		return nil
	})
	return scores, present, err
}

// zsetAlgebraSource describes one input to a streamed zset operation after its
// header has been read once: its cardinality, its header (for the coll/blob split),
// and, for a blob, its members mapped to scores so a probe is an in-memory lookup.
type zsetAlgebraSource struct {
	card  int64
	hdr   keyspace.ValueHeader
	blob  map[string]float64 // non-nil only for a blob source
	empty bool               // missing key or zero members
}

// readZSetSources reads each key's header and cardinality and, for the blob form,
// its member-to-score map. wrongTyp is set when any key holds a non-zset. No coll
// source is materialized here: its members are reached later by point probe or by
// the driver walk.
func readZSetSources(db *keyspace.DB, keys [][]byte) (srcs []zsetAlgebraSource, wrongTyp bool, err error) {
	srcs = make([]zsetAlgebraSource, len(keys))
	for i, k := range keys {
		c, hdr, found, e := zsetCard(db, k)
		if e != nil {
			return nil, false, e
		}
		if found && hdr.Type != keyspace.TypeZSet {
			return nil, true, nil
		}
		if !found || c == 0 {
			srcs[i].empty = true
			continue
		}
		srcs[i].card = c
		srcs[i].hdr = hdr
		if !hdr.IsColl() {
			members, _, _, me := getZSet(db, k)
			if me != nil {
				return nil, false, me
			}
			srcs[i].blob = scoreMap(members)
		}
	}
	return srcs, false, nil
}

// zinterStream computes ZINTER without cloning a whole coll source. It drives the
// smallest source in scored batches and folds every other source into each batch
// member's score, blob sources by map lookup and coll sources by a batched point
// probe, dropping any member absent from a source. The result holds only the
// intersection, which is at most the smallest source and is what Redis returns too.
func zinterStream(db *keyspace.DB, keys [][]byte, weights []float64, agg aggMode) (result []zmember, wrongTyp bool, err error) {
	srcs, wt, err := readZSetSources(db, keys)
	if err != nil || wt {
		return nil, wt, err
	}
	smallest := -1
	for i := range srcs {
		if srcs[i].empty {
			return nil, false, nil // an empty source makes the intersection empty
		}
		if smallest < 0 || srcs[i].card < srcs[smallest].card {
			smallest = i
		}
	}

	err = zsetDriveScored(db, keys[smallest], srcs[smallest].hdr, func(batch []zsetScored) (bool, error) {
		members := make([][]byte, len(batch))
		vals := make([]float64, len(batch))
		for i, zs := range batch {
			members[i] = zs.member
			vals[i] = weightedScore(weights[smallest], zs.score)
		}
		for j := range keys {
			if j == smallest || len(members) == 0 {
				continue
			}
			if blob := srcs[j].blob; blob != nil {
				members, vals = keepInScored(members, vals, weights[j], agg, blob)
				continue
			}
			scores, present, pe := zsetProbeScores(db, keys[j], members)
			if pe != nil {
				return false, pe
			}
			members, vals = keepPresentScored(members, vals, weights[j], agg, scores, present)
		}
		for i, m := range members {
			result = append(result, zmember{member: m, score: vals[i]})
		}
		return false, nil
	})
	if err != nil {
		return nil, false, err
	}
	zsetSort(result)
	return result, false, nil
}

// zdiffStream computes ZDIFF without cloning a whole coll source. It drives the
// first source in scored batches and keeps each member absent from every other
// source, blob sources by map lookup and coll sources by a batched point probe. The
// result holds only members of the first source, with their first-source scores.
func zdiffStream(db *keyspace.DB, keys [][]byte) (result []zmember, wrongTyp bool, err error) {
	srcs, wt, err := readZSetSources(db, keys)
	if err != nil || wt {
		return nil, wt, err
	}
	if srcs[0].empty {
		return nil, false, nil
	}

	err = zsetDriveScored(db, keys[0], srcs[0].hdr, func(batch []zsetScored) (bool, error) {
		members := make([][]byte, len(batch))
		vals := make([]float64, len(batch))
		for i, zs := range batch {
			members[i] = zs.member
			vals[i] = zs.score
		}
		for j := 1; j < len(keys); j++ {
			if len(members) == 0 {
				break
			}
			if srcs[j].empty {
				continue
			}
			if blob := srcs[j].blob; blob != nil {
				members, vals = keepAbsentScored(members, vals, blobMembership(blob))
				continue
			}
			_, present, pe := zsetProbeScores(db, keys[j], members)
			if pe != nil {
				return false, pe
			}
			members, vals = keepAbsentScoredByFlag(members, vals, present)
		}
		for i, m := range members {
			result = append(result, zmember{member: m, score: vals[i]})
		}
		return false, nil
	})
	if err != nil {
		return nil, false, err
	}
	zsetSort(result)
	return result, false, nil
}

// zunionStream computes ZUNION without holding every source at once. It streams
// each source through scored batches and folds each member into one accumulator
// map, the distinct union Redis materializes too; no source is cloned whole, only
// one batch is live at a time on top of the result.
func zunionStream(db *keyspace.DB, keys [][]byte, weights []float64, agg aggMode) (result []zmember, wrongTyp bool, err error) {
	srcs, wt, err := readZSetSources(db, keys)
	if err != nil || wt {
		return nil, wt, err
	}
	acc := map[string]float64{}
	for i := range keys {
		if srcs[i].empty {
			continue
		}
		w := weights[i]
		e := zsetDriveScored(db, keys[i], srcs[i].hdr, func(batch []zsetScored) (bool, error) {
			for _, zs := range batch {
				val := weightedScore(w, zs.score)
				if cur, ok := acc[string(zs.member)]; ok {
					acc[string(zs.member)] = aggregate(cur, val, agg)
				} else {
					acc[string(zs.member)] = val
				}
			}
			return false, nil
		})
		if e != nil {
			return nil, false, e
		}
	}
	return mapToSorted(acc), false, nil
}

// keepInScored compacts members and their running scores to those present in the
// blob membership map, folding each survivor's weighted blob score into its running
// value. members and vals stay aligned.
func keepInScored(members [][]byte, vals []float64, w float64, agg aggMode, blob map[string]float64) ([][]byte, []float64) {
	n := 0
	for i, m := range members {
		s, ok := blob[string(m)]
		if !ok {
			continue
		}
		members[n] = m
		vals[n] = aggregate(vals[i], weightedScore(w, s), agg)
		n++
	}
	return members[:n], vals[:n]
}

// keepPresentScored compacts members and their running scores to those a point
// probe found present, folding each survivor's weighted probed score into its
// running value.
func keepPresentScored(members [][]byte, vals []float64, w float64, agg aggMode, scores []float64, present []bool) ([][]byte, []float64) {
	n := 0
	for i, m := range members {
		if !present[i] {
			continue
		}
		members[n] = m
		vals[n] = aggregate(vals[i], weightedScore(w, scores[i]), agg)
		n++
	}
	return members[:n], vals[:n]
}

// keepAbsentScored compacts members and their scores to those absent from the blob
// membership set, the ZDIFF keep rule.
func keepAbsentScored(members [][]byte, vals []float64, set map[string]struct{}) ([][]byte, []float64) {
	n := 0
	for i, m := range members {
		if _, ex := set[string(m)]; ex {
			continue
		}
		members[n] = m
		vals[n] = vals[i]
		n++
	}
	return members[:n], vals[:n]
}

// keepAbsentScoredByFlag compacts members and their scores to those a point probe
// found absent (present[i] false), the coll-source ZDIFF keep rule.
func keepAbsentScoredByFlag(members [][]byte, vals []float64, present []bool) ([][]byte, []float64) {
	n := 0
	for i, m := range members {
		if present[i] {
			continue
		}
		members[n] = m
		vals[n] = vals[i]
		n++
	}
	return members[:n], vals[:n]
}

// blobMembership reduces a blob source's member-to-score map to a membership set
// for the ZDIFF presence test.
func blobMembership(blob map[string]float64) map[string]struct{} {
	set := make(map[string]struct{}, len(blob))
	for m := range blob {
		set[m] = struct{}{}
	}
	return set
}
