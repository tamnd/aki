package command

import (
	"bytes"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// ZUNIONSTORE, ZINTERSTORE and ZDIFFSTORE used to call loadZSets, which clones every
// member and score of every source onto the heap, run computeZSetOp, then write the
// whole result back with one db.Set. The sources lived in RAM in full before a single
// byte reached the destination, so storing the union or intersection of sorted sets
// far larger than RAM OOM-killed under a tight cap even when the destination would
// have spilled to the coll form anyway. This is the zset twin of the set-algebra
// store sink (note 337).
//
// streamZSetOpStore computes the result with the same bounded read-form machinery
// (zinterStreamInto/zdiffStreamInto/zunionStreamInto: one driver walked in batches,
// every other coll source point-probed) and writes each result member into the
// destination as it is produced. A small result buffers up to the listpack threshold
// and writes as one blob, exactly the listpack form Redis would pick, so the common
// case is byte-for-byte what it was. A result that crosses the skiplist threshold
// spills into a fresh coll-form destination and every later member is written straight
// into that dual-index sub-tree, so the destination is built element-by-element and
// never held whole.
//
// For ZUNIONSTORE the spilled sub-tree is also the aggregation structure: a member
// already in the tree is read, its score combined under the AGGREGATE mode, and only
// the changed rows rewritten, so the union of huge sources is computed without an
// in-memory accumulator the size of the result, the sharpest larger-than-memory win
// in this family.
//
// One case keeps the materialize path: when the destination key is also one of the
// sources (ZINTERSTORE dst dst a). Writing into the destination while reading it would
// mutate a source mid-walk, so the aliased case falls back to the buffered compute,
// the same O(result) memory Redis itself uses to build a STORE result.

// zsetStoreSink accumulates a streamed zset-algebra result into a destination key. It
// buffers members while the result still fits a listpack, and the moment the buffer
// crosses the skiplist threshold it deletes the destination, writes the buffered
// members into a fresh dual-index coll sub-tree, and switches to writing each later
// member straight into that sub-tree in batches. Memory is bounded by the listpack
// threshold plus one flush batch, never the result.
type zsetStoreSink struct {
	db    *keyspace.DB
	dst   []byte
	lim   encLimits
	dedup bool    // ZUNIONSTORE: aggregate a member seen in more than one source
	agg   aggMode // the AGGREGATE combiner used when dedup folds a repeat

	// Pre-spill listpack buffer and the running max member length that, with the
	// buffer length, decides when the set must promote to the coll form (the
	// zsetEncoding rule, evaluated in O(1) per member).
	buf     []zmember
	seen    map[string]int // member -> index in buf, the dedup/aggregate map (union only)
	maxLen  int
	spilled bool

	// Post-spill flush batch written into the coll sub-tree once it fills.
	batch []zmember

	count int64 // distinct members stored so far (set once at spill, then incremented)
}

func newZSetStoreSink(db *keyspace.DB, dst []byte, lim encLimits, dedup bool, agg aggMode) *zsetStoreSink {
	s := &zsetStoreSink{db: db, dst: dst, lim: lim, dedup: dedup, agg: agg}
	if dedup {
		s.seen = map[string]int{}
	}
	return s
}

// add records one result member. m aliases a driver batch the caller still owns, so
// add copies it before keeping any reference. For ZUNIONSTORE a member already seen
// folds its score under the AGGREGATE mode rather than appending a duplicate.
func (s *zsetStoreSink) add(m []byte, score float64) error {
	if s.spilled {
		s.batch = append(s.batch, zmember{member: append([]byte(nil), m...), score: score})
		if len(s.batch) >= storeFlushBatch {
			return s.flushBatch()
		}
		return nil
	}
	if s.dedup {
		if idx, ok := s.seen[string(m)]; ok {
			s.buf[idx].score = aggregate(s.buf[idx].score, score, s.agg)
			return nil
		}
		s.seen[string(m)] = len(s.buf)
	}
	s.buf = append(s.buf, zmember{member: append([]byte(nil), m...), score: score})
	if len(m) > s.maxLen {
		s.maxLen = len(m)
	}
	if s.treeWanted() {
		return s.spill()
	}
	return nil
}

// treeWanted reports whether the buffered members have crossed the threshold where
// the sorted set reports skiplist, which is exactly where it must become coll form.
// It mirrors zsetEncoding from the running length and max member length so the check
// stays O(1) per member instead of rescanning the buffer.
func (s *zsetStoreSink) treeWanted() bool {
	if int64(len(s.buf)) > s.lim.zsetEntries {
		return true
	}
	return int64(s.maxLen) > s.lim.zsetValue
}

// spill deletes the destination and writes the buffered members into a fresh
// coll-form sub-tree, both the score-index and member-index row per member, then
// drops the buffer so later members write straight into the tree.
func (s *zsetStoreSink) spill() error {
	if _, err := s.db.Delete(s.dst); err != nil {
		return err
	}
	buffered := s.buf
	err := s.db.CollUpdate(s.dst, keyspace.TypeZSet, keyspace.EncSkiplist, func(w *keyspace.CollWriter) error {
		for _, zm := range buffered {
			if _, e := w.Put(zScoreRow(zm.score, zm.member), nil); e != nil {
				return e
			}
			if _, e := w.Put(zMemberRow(zm.member), zScoreValue(zm.score)); e != nil {
				return e
			}
		}
		w.SetCount(uint64(len(buffered)))
		return nil
	})
	if err != nil {
		return err
	}
	s.spilled = true
	s.count = int64(len(buffered))
	s.buf = nil
	s.seen = nil
	return nil
}

// flushBatch writes the pending post-spill members into the coll sub-tree. A new
// member writes both rows and bumps the count. For ZUNIONSTORE a member already in
// the tree (from a buffered source or an earlier source in this batch) reads its
// stored score, combines it under the AGGREGATE mode, and rewrites only the changed
// rows, so the union aggregates against the tree with no result-sized RAM structure.
func (s *zsetStoreSink) flushBatch() error {
	pending := s.batch
	if len(pending) == 0 {
		return nil
	}
	err := s.db.CollUpdate(s.dst, keyspace.TypeZSet, keyspace.EncSkiplist, func(w *keyspace.CollWriter) error {
		for _, zm := range pending {
			if s.dedup {
				cur, ok, e := w.Get(zMemberRow(zm.member))
				if e != nil {
					return e
				}
				if ok {
					old := zScoreUnbits(encoding.U64BE(cur))
					ns := aggregate(old, zm.score, s.agg)
					if ns != old {
						// The score-index key embeds the score, so a change moves the
						// row: drop the old score row, write the new one, update the
						// member row's score payload.
						if _, e := w.Delete(zScoreRow(old, zm.member)); e != nil {
							return e
						}
						if _, e := w.Put(zScoreRow(ns, zm.member), nil); e != nil {
							return e
						}
						if _, e := w.Put(zMemberRow(zm.member), zScoreValue(ns)); e != nil {
							return e
						}
					}
					continue
				}
			}
			if _, e := w.Put(zScoreRow(zm.score, zm.member), nil); e != nil {
				return e
			}
			if _, e := w.Put(zMemberRow(zm.member), zScoreValue(zm.score)); e != nil {
				return e
			}
			w.SetCount(w.Count() + 1)
			s.count++
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.batch = s.batch[:0]
	return nil
}

// finish writes the result that is still buffered (the small, common case) and
// reports how many distinct members the destination holds and whether a delete
// removed a prior value (so the caller can fire the right keyspace event). A spilled
// result is already in the destination; finish only flushes its tail.
func (s *zsetStoreSink) finish() (n int64, deletedExisting bool, err error) {
	if s.spilled {
		if e := s.flushBatch(); e != nil {
			return 0, false, e
		}
		return s.count, false, nil
	}
	if len(s.buf) == 0 {
		existed, e := s.db.Delete(s.dst)
		return 0, existed, e
	}
	zsetSort(s.buf)
	enc := zsetEncoding(s.lim, s.buf, keyspace.EncListpack)
	if e := s.db.Set(s.dst, zsetEncode(s.buf), keyspace.TypeZSet, enc, -1); e != nil {
		return 0, false, e
	}
	return int64(len(s.buf)), false, nil
}

// handleZSetOpStore implements ZUNIONSTORE, ZINTERSTORE and ZDIFFSTORE. When the
// destination is independent of the sources it streams the result into the
// destination without materializing any whole source (streamZSetOpStore). When the
// destination aliases a source it falls back to the buffered compute, which reads
// every source before touching the destination (storeZSetOpMaterialize).
func handleZSetOpStore(ctx *Ctx, op zsetOp, dst []byte, keys [][]byte, weights []float64, agg aggMode) {
	for _, k := range keys {
		if bytes.Equal(dst, k) {
			storeZSetOpMaterialize(ctx, op, dst, keys, weights, agg)
			return
		}
	}
	streamZSetOpStore(ctx, op, dst, keys, weights, agg)
}

// streamZSetOpStore computes the operation with the bounded read-form machinery and
// writes the result into dst through a zsetStoreSink, so neither the sources nor the
// result are ever held whole.
func streamZSetOpStore(ctx *Ctx, op zsetOp, dst []byte, keys [][]byte, weights []float64, agg aggMode) {
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		sink := newZSetStoreSink(db, dst, ctx.encLimits(), op == zopUnion, agg)
		var wt bool
		var err error
		switch op {
		case zopInter:
			wt, err = zinterStreamInto(db, keys, weights, agg, sink.add)
		case zopDiff:
			wt, err = zdiffStreamInto(db, keys, sink.add)
		default:
			wt, err = zunionStreamInto(db, keys, weights, sink.add)
		}
		if err != nil {
			return err
		}
		if wt {
			// A wrong-type source is rejected before any member is emitted, so the
			// destination is left untouched, matching Redis.
			wrongTyp = true
			return nil
		}
		nn, del, e := sink.finish()
		n, dstDeleted = nn, del
		return e
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if n > 0 {
		ctx.notify(notifyZset, zSetStoreEvent(op), dst)
		ctx.signalReady(dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}

// storeZSetOpMaterialize is the fallback for ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE when
// the destination aliases a source. It reads every source in full before writing the
// destination, so the destination read stays consistent while it is rewritten. This
// is the same O(result) memory Redis uses to build any STORE result.
func storeZSetOpMaterialize(ctx *Ctx, op zsetOp, dst []byte, keys [][]byte, weights []float64, agg aggMode) {
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
	)
	done := ctx.update(func(db *keyspace.DB) error {
		sets, wt, err := loadZSets(db, keys)
		if err != nil {
			return err
		}
		if wt {
			wrongTyp = true
			return nil
		}
		result := computeZSetOp(op, sets, weights, agg)
		n = int64(len(result))
		if len(result) == 0 {
			existed, err := db.Delete(dst)
			dstDeleted = existed
			return err
		}
		return db.Set(dst, zsetEncode(result), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), result, keyspace.EncListpack), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if n > 0 {
		ctx.notify(notifyZset, zSetStoreEvent(op), dst)
		ctx.signalReady(dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}
