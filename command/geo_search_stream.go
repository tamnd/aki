package command

import (
	"encoding/binary"

	"github.com/tamnd/aki/keyspace"
)

// geoSearchHits resolves the center and collects the members inside the query
// shape without materializing the whole source sorted set on the coll form.
//
// GEOSEARCH and the GEORADIUS family used to call getZSet, cloning every member
// of the source set onto the heap (a geo set is stored as a sorted set keyed by
// the geohash score), then scan the clone. A radius query that matches a handful
// of points still dragged a multi-million-member geo set through memory first, an
// OOM under a tight cap for a query whose answer is small. There is no spatial
// index to seek with, so the scan is inherently O(n), but the memory need not be:
// a coll set is streamed through an arena cursor over its member-index rows,
// decoding each member's geohash score and testing the shape in place, so only the
// matches and one decoded member at a time are held. A blob set is below the
// encoding threshold, so it decodes once. The hits are sorted and truncated by the
// caller exactly as the old scan did.
//
// A COUNT ANY query skips the sort, so it can stop the walk the moment it has
// COUNT matches, an early exit that also bounds the time, not just the memory.
func geoSearchHits(db *keyspace.DB, key []byte, q *geoQuery) (hits []geoHit, wrongTyp, noMember bool, err error) {
	hdr, found, err := zsetHeader(db, key)
	if err != nil {
		return nil, false, false, err
	}
	if found && hdr.Type != keyspace.TypeZSet {
		return nil, true, false, nil
	}
	if !found {
		return nil, false, false, nil
	}

	// Resolve a FROMMEMBER center by a point lookup on the member index rather than
	// a scan of the whole set.
	if q.fromMember != nil {
		sc, pr, _, _, e := zsetMemberScores(db, key, [][]byte{q.fromMember})
		if e != nil {
			return nil, false, false, e
		}
		if !pr[0] {
			return nil, false, true, nil
		}
		q.fromLon, q.fromLat = geoDecode(sc[0])
	}

	hits = make([]geoHit, 0)
	earlyExit := geoCountEarlyExit(q)

	if !hdr.IsColl() {
		members, _, _, e := getZSet(db, key)
		if e != nil {
			return nil, false, false, e
		}
		for _, m := range members {
			h, ok := geoTestPoint(q, m.member, m.score)
			if !ok {
				continue
			}
			hits = append(hits, h)
			if earlyExit && len(hits) >= q.count {
				break
			}
		}
		return hits, false, false, nil
	}

	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		c.UseArena()
		if e := c.Seek([]byte{zRowMember}); e != nil {
			return e
		}
		for c.Valid() {
			k := c.Key()
			if len(k) == 0 || k[0] != zRowMember {
				// Walked past the member rows into the score rows; the set is done.
				break
			}
			score := zScoreUnbits(binary.BigEndian.Uint64(c.Value()))
			// k[1:] and the value alias the cursor arena, so copy the member only when
			// the point is a keeper.
			h, ok := geoTestPoint(q, k[1:], score)
			if ok {
				h.member = append([]byte(nil), k[1:]...)
				hits = append(hits, h)
				if earlyExit && len(hits) >= q.count {
					return nil
				}
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, false, err
	}
	return hits, false, false, nil
}
