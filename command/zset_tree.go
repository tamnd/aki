package command

import (
	"math"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// A large sorted set is stored element-per-row in the key's btree-backed
// collection sub-tree (keyspace.CollUpdate / CollRead). A sorted set needs two
// access paths, so it keeps two row families in the one sub-tree:
//
//	'm' + member            -> scoreBits   (point ZSCORE/ZREM/ZADD on a member)
//	's' + scoreBits + member -> (empty)    (ordered ZRANGE/ZRANK walk)
//
// scoreBits is the float64 score encoded so that bytewise order matches numeric
// order (sign bit flipped, negatives inverted, big-endian), which makes the
// score-index rows sort in exactly the (score, member) order Redis uses. The
// member-index value stores the same 8 score bytes so a point member lookup
// returns the score without a scan. The two prefixes 'm' (0x6d) and 's' (0x73)
// keep the families apart, and a walk of the score rows alone reproduces the
// sorted member list.
//
// A small sorted set keeps the single-blob form in zset_codec.go. It promotes to
// the sub-tree exactly when its reported encoding becomes skiplist, so OBJECT
// ENCODING flips at the same threshold as Redis and never demotes.
//
// getZSet (zset_codec.go) is coll-aware, so every read caller (ZRANGE, ZRANK, the
// ZUNION/ZINTER/ZDIFF family, ZSCORE, ZPOP, DUMP/RDB, SORT, GEO) works on either
// form with no change. ZADD, ZINCRBY and ZREM branch on hdr.IsColl() before
// getZSet so they never rewrite a whole blob for a btree-backed set; they do point
// sub-tree ops and maintain the count. The bulk-rewrite and store commands keep
// the unchanged read-then-Set path: they read through the coll-aware getZSet and
// write a blob, which demotes the key, and the next ZADD re-promotes it.

const (
	zRowMember = 'm' // member-index row prefix
	zRowScore  = 's' // score-index row prefix
)

// zScoreBits maps a float64 score to a uint64 whose unsigned big-endian byte
// order matches the score's numeric order. Positive scores get the sign bit set;
// negative scores are bit-inverted so larger magnitudes sort lower.
func zScoreBits(f float64) uint64 {
	b := math.Float64bits(f)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | (1 << 63)
}

// zScoreUnbits is the inverse of zScoreBits.
func zScoreUnbits(u uint64) float64 {
	if u&(1<<63) != 0 {
		u &^= 1 << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

// zMemberRow builds the member-index row key for member.
func zMemberRow(member []byte) []byte {
	k := make([]byte, 1+len(member))
	k[0] = zRowMember
	copy(k[1:], member)
	return k
}

// zScoreRow builds the score-index row key for (score, member).
func zScoreRow(score float64, member []byte) []byte {
	k := make([]byte, 0, 1+8+len(member))
	k = append(k, zRowScore)
	k = encoding.AppendU64BE(k, zScoreBits(score))
	return append(k, member...)
}

// zScoreValue is the 8-byte score payload stored in a member-index row.
func zScoreValue(score float64) []byte {
	return encoding.AppendU64BE(make([]byte, 0, 8), zScoreBits(score))
}

// zsetWantsTree reports whether a sorted set with these members should live in the
// btree-backed form. The rule is the encoding rule: a sorted set is tree-backed
// exactly when it reports skiplist, so promotion happens at the listpack threshold
// and the encoding name stays correct for free.
func zsetWantsTree(lim encLimits, members []zmember, prevEnc uint8) bool {
	return zsetEncoding(lim, members, prevEnc) == keyspace.EncSkiplist
}

// zsetPromote moves a sorted set from the blob form to the btree-backed form. It
// writes both rows for every member through CollUpdate, which creates the fresh
// sub-tree, frees the old blob, and carries over the key's TTL. Callers reach it
// when an applied write pushes the member set past the skiplist threshold.
func zsetPromote(db *keyspace.DB, key []byte, members []zmember) error {
	return db.CollUpdate(key, keyspace.TypeZSet, keyspace.EncSkiplist, func(w *keyspace.CollWriter) error {
		for _, zm := range members {
			if _, e := w.Put(zScoreRow(zm.score, zm.member), nil); e != nil {
				return e
			}
			if _, e := w.Put(zMemberRow(zm.member), zScoreValue(zm.score)); e != nil {
				return e
			}
		}
		w.SetCount(uint64(len(members)))
		return nil
	})
}

// zsetHeader probes the value header at key without decoding the body, so a write
// command can route to the blob path or the sub-tree path. found is false for a
// missing key.
func zsetHeader(db *keyspace.DB, key []byte) (keyspace.ValueHeader, bool, error) {
	return db.CollMetaHeader(key)
}

// collectZSetMembers walks a btree-backed sorted set's score-index rows and returns
// every (member, score) pair in (score, member) order, which the score-index key
// order already gives. The caller has confirmed the key is a coll sorted set.
func collectZSetMembers(db *keyspace.DB, key []byte) ([]zmember, error) {
	var out []zmember
	_, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		out = make([]zmember, 0, r.Count())
		c := r.Cursor()
		if e := c.Seek([]byte{zRowScore}); e != nil {
			return e
		}
		for c.Valid() {
			k := c.Key()
			if len(k) == 0 || k[0] != zRowScore {
				break
			}
			score := zScoreUnbits(encoding.U64BE(k[1:9]))
			member := append([]byte(nil), k[9:]...)
			out = append(out, zmember{member: member, score: score})
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return out, err
}

// zsetScores answers a batch of score lookups (ZSCORE, ZMSCORE) against the sorted
// set at key with the cheapest path for the storage form, never materializing the
// whole set. A btree-backed sorted set answers each query with an O(log n) point
// lookup on its member-index row ('m' + member -> score bytes), reading the score
// straight from the row value; a small blob decodes once and scans. This is the
// difference between O(q) point lookups and an O(n) walk that clones every member on
// every ZSCORE, which on a multi-million-member sorted set is the same allocation
// blow-up that OOM-killed SISMEMBER before it became a point lookup.
//
// scores and present are filled per query (present false for an absent member or an
// absent key). wrongTyp reports a non-zset value at key. ok is false only when the
// underlying view failed.
func zsetScores(ctx *Ctx, key []byte, queries [][]byte) (scores []float64, present []bool, wrongTyp bool, ok bool) {
	scores = make([]float64, len(queries))
	present = make([]bool, len(queries))
	// A small sorted set may be served straight from the lock-free hot cache;
	// hotGetZSet returns a miss for the coll form, so a hit here is the blob form.
	if members, hit := hotGetZSet(ctx, key); hit {
		for i, q := range queries {
			if idx := zsetFind(members, q); idx >= 0 {
				scores[i] = members[idx].score
				present[i] = true
			}
		}
		return scores, present, false, true
	}
	ok = ctx.view(func(db *keyspace.DB) error {
		hdr, found, err := zsetHeader(db, key)
		if err != nil || !found {
			return err
		}
		if hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		if hdr.IsColl() {
			// One reader, a point lookup per query: the score rows are never walked.
			_, e := db.CollRead(key, func(r *keyspace.CollReader) error {
				for i, q := range queries {
					v, p, ge := r.Get(zMemberRow(q))
					if ge != nil {
						return ge
					}
					if p {
						scores[i] = zScoreUnbits(encoding.U64BE(v))
						present[i] = true
					}
				}
				return nil
			})
			return e
		}
		members, _, _, e := getZSet(db, key)
		if e != nil {
			return e
		}
		for i, q := range queries {
			if idx := zsetFind(members, q); idx >= 0 {
				scores[i] = members[idx].score
				present[i] = true
			}
		}
		return nil
	})
	return scores, present, wrongTyp, ok
}

// zsetCard returns the member count in whichever form the sorted set is stored.
// For a btree-backed sorted set it reads the metadata count in O(1).
func zsetCard(db *keyspace.DB, key []byte) (n int64, hdr keyspace.ValueHeader, keyFound bool, err error) {
	hdr, keyFound, err = zsetHeader(db, key)
	if err != nil || !keyFound {
		return 0, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeZSet {
		return 0, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			n = int64(r.Count())
			return nil
		})
		return n, hdr, true, err
	}
	members, _, _, e := getZSet(db, key)
	if e != nil {
		return 0, hdr, true, e
	}
	return int64(len(members)), hdr, true, nil
}

// zsetCollPop removes up to count members from the low end (fromMax false, ZPOPMIN)
// or the high end (fromMax true, ZPOPMAX) of a coll-form sorted set and returns them
// in pop order: lowest score first for ZPOPMIN, highest score first for ZPOPMAX. It
// walks only the popped window of the score index, seeking straight to the end it
// pops from, so taking a handful off a multi-million-member set is O(count log n).
// The materialized path it replaces cloned every member onto the heap and rewrote
// the whole set as a blob (demoting it), which OOM-killed under a tight memory cap.
// The collected members are deleted after the walk so the cursor is never used past
// a mutation. CollUpdate tears the key down when the last member goes.
func zsetCollPop(db *keyspace.DB, key []byte, count int64, fromMax bool) (popped []zmember, err error) {
	if count <= 0 {
		return nil, nil
	}
	err = db.CollUpdate(key, keyspace.TypeZSet, keyspace.EncSkiplist, func(w *keyspace.CollWriter) error {
		c := w.Cursor()
		// Either direction decodes the popped window into the cursor's arena: a forward
		// walk resets it per leaf, a backward walk holds the root-to-leaf path live and
		// grows the buffer by the few nodes the bounded pop touches. Both stay a small
		// constant instead of cloning every member onto the heap.
		c.UseArena()
		var step func() error
		if fromMax {
			// Score rows ('s' 0x73) sort after member rows ('m' 0x6d), so the last key
			// in the sub-tree is the highest-scoring member.
			if e := c.Last(); e != nil {
				return e
			}
			step = c.Prev
		} else {
			if e := c.Seek([]byte{zRowScore}); e != nil {
				return e
			}
			step = c.Next
		}
		for c.Valid() && int64(len(popped)) < count {
			k := c.Key()
			if len(k) == 0 || k[0] != zRowScore {
				break
			}
			score := zScoreUnbits(encoding.U64BE(k[1:9]))
			member := append([]byte(nil), k[9:]...)
			popped = append(popped, zmember{member: member, score: score})
			if e := step(); e != nil {
				return e
			}
		}
		for _, zm := range popped {
			if _, e := zTreeDel(w, zm.member); e != nil {
				return e
			}
		}
		return nil
	})
	return popped, err
}

// zScoreAboveHigh reports whether an ascending walk has passed the high score
// bound and can stop. Rows come back in ascending score order, so once a score is
// above the bound no later row can qualify.
func zScoreAboveHigh(score float64, hi scoreBound) bool {
	if hi.excl {
		return score >= hi.value
	}
	return score > hi.value
}

// zsetCollRangeByScore walks a coll-form sorted set's score-index rows in ascending
// order and returns the members whose score falls in [lo, hi], in score order. It
// seeks straight to the low score, so the walk touches only the matching window plus
// the rows it stops on, never the whole set: a ZRANGEBYSCORE or ZCOUNT over a narrow
// band of a multi-million-member sorted set stays bounded instead of cloning every
// member onto the heap and thrashing under a tight memory cap. When countOnly is set
// it returns the match count without building the slice. limit applies the
// ZRANGEBYSCORE LIMIT offset/count during the walk so a bounded query stops after it
// has the rows it needs. The caller handles the reverse direction and the blob form.
func zsetCollRangeByScore(db *keyspace.DB, key []byte, lo, hi scoreBound, limit bool, offset, count int64, countOnly bool) (out []zmember, n int64, err error) {
	if limit && offset < 0 {
		return nil, 0, nil
	}
	_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
		c := r.Cursor()
		// Forward score-index walk over one band: the arena keeps page decoding to a
		// small constant so a narrow ZRANGEBYSCORE/ZCOUNT over a multi-million-member
		// set stays bounded instead of allocating per cell across every leaf it spans.
		c.UseArena()
		seek := encoding.AppendU64BE([]byte{zRowScore}, zScoreBits(lo.value))
		if e := c.Seek(seek); e != nil {
			return e
		}
		skip := int64(0)
		if limit {
			skip = offset
		}
		for c.Valid() {
			k := c.Key()
			if len(k) == 0 || k[0] != zRowScore {
				break
			}
			score := zScoreUnbits(encoding.U64BE(k[1:9]))
			if zScoreAboveHigh(score, hi) {
				break
			}
			if !scoreInRange(score, lo, hi) { // low-edge exclusive skip
				if e := c.Next(); e != nil {
					return e
				}
				continue
			}
			if countOnly {
				n++
			} else if skip > 0 {
				skip--
			} else {
				member := append([]byte(nil), k[9:]...)
				out = append(out, zmember{member: member, score: score})
				if limit && count >= 0 && int64(len(out)) >= count {
					break
				}
			}
			if e := c.Next(); e != nil {
				return e
			}
		}
		return nil
	})
	return out, n, err
}

// zsetMemberScores reads the scores of a specific handful of members without
// materializing the whole sorted set. For a coll-form sorted set each member is a
// point lookup on its member-index row, so a GEODIST/GEOPOS/GEOHASH against a
// multi-million-member geo set stays O(queries log n) and constant allocation
// instead of cloning every member onto the heap, which under a tight memory cap is
// the difference between serving and an OOM kill. For the blob form it decodes once
// (bounded by the listpack threshold). present[i] reports whether members[i] was
// found; scores[i] holds its score when present. keyFound is false for a missing
// key, and a non-zset value leaves wrongType for the caller to surface via hdr.
func zsetMemberScores(db *keyspace.DB, key []byte, members [][]byte) (scores []float64, present []bool, hdr keyspace.ValueHeader, keyFound bool, err error) {
	scores = make([]float64, len(members))
	present = make([]bool, len(members))
	hdr, keyFound, err = zsetHeader(db, key)
	if err != nil || !keyFound {
		return scores, present, hdr, keyFound, err
	}
	if hdr.Type != keyspace.TypeZSet {
		return scores, present, hdr, true, nil
	}
	if hdr.IsColl() {
		_, err = db.CollRead(key, func(r *keyspace.CollReader) error {
			for i, m := range members {
				v, ok, e := r.Get(zMemberRow(m))
				if e != nil {
					return e
				}
				if ok {
					scores[i] = zScoreUnbits(encoding.U64BE(v))
					present[i] = true
				}
			}
			return nil
		})
		return scores, present, hdr, true, err
	}
	set, _, _, e := getZSet(db, key)
	if e != nil {
		return scores, present, hdr, true, e
	}
	for i, m := range members {
		if idx := zsetFind(set, m); idx >= 0 {
			scores[i] = set[idx].score
			present[i] = true
		}
	}
	return scores, present, hdr, true, nil
}

// zTreeScore reads a member's current score from the member-index row inside a
// write callback. found is false when the member is absent.
func zTreeScore(w *keyspace.CollWriter, member []byte) (score float64, found bool, err error) {
	v, ok, err := w.Get(zMemberRow(member))
	if err != nil || !ok {
		return 0, false, err
	}
	return zScoreUnbits(encoding.U64BE(v)), true, nil
}

// zTreeSet writes member at newScore, keeping both rows and the count in step. When
// the member already exists at oldScore, its stale score-index row is removed first.
func zTreeSet(w *keyspace.CollWriter, member []byte, newScore float64, found bool, oldScore float64) error {
	if found {
		if _, e := w.Delete(zScoreRow(oldScore, member)); e != nil {
			return e
		}
	}
	if _, e := w.Put(zScoreRow(newScore, member), nil); e != nil {
		return e
	}
	if _, e := w.Put(zMemberRow(member), zScoreValue(newScore)); e != nil {
		return e
	}
	if !found {
		w.SetCount(w.Count() + 1)
	}
	return nil
}

// zTreeDel removes a member and both its rows, decrementing the count. existed is
// false when the member was absent.
func zTreeDel(w *keyspace.CollWriter, member []byte) (existed bool, err error) {
	v, ok, err := w.Get(zMemberRow(member))
	if err != nil || !ok {
		return false, err
	}
	score := zScoreUnbits(encoding.U64BE(v))
	if _, e := w.Delete(zMemberRow(member)); e != nil {
		return false, e
	}
	if _, e := w.Delete(zScoreRow(score, member)); e != nil {
		return false, e
	}
	w.SetCount(w.Count() - 1)
	return true, nil
}

// zaddOutcome is the decision for one ZADD pair against the member's current state,
// shared by the blob path and the btree-backed path so the NX/XX/GT/LT/INCR rules
// have one source of truth.
type zaddOutcome struct {
	newScore float64 // score to store when write is true
	write    bool    // the member should be inserted or updated
	add      bool    // write is an insert of a new member
	change   bool    // write changes an existing member's score
	blocked  bool    // a flag (NX/XX/GT/LT) stopped this pair
	nan      bool    // INCR produced NaN
	incrVal  float64 // score to report for INCR
	haveIncr bool    // incrVal is meaningful (the member ends at a known score)
}

// zaddDecide applies the ZADD flags to one pair given the member's current score
// and presence. It does not touch storage; the caller applies the outcome.
func zaddDecide(p zaddPair, cur float64, found, nx, xx, gt, lt, incr bool) zaddOutcome {
	if found {
		ns := p.score
		if incr {
			ns = cur + p.score
			if math.IsNaN(ns) {
				return zaddOutcome{nan: true}
			}
		}
		if nx {
			return zaddOutcome{blocked: true}
		}
		if gt && !(ns > cur) {
			return zaddOutcome{blocked: true}
		}
		if lt && !(ns < cur) {
			return zaddOutcome{blocked: true}
		}
		if ns != cur {
			return zaddOutcome{newScore: ns, write: true, change: true, incrVal: ns, haveIncr: true}
		}
		return zaddOutcome{newScore: ns, incrVal: ns, haveIncr: true}
	}
	if xx {
		return zaddOutcome{blocked: true}
	}
	return zaddOutcome{newScore: p.score, write: true, add: true, incrVal: p.score, haveIncr: true}
}
