package command

import (
	"bytes"

	"github.com/tamnd/aki/encoding"
	"github.com/tamnd/aki/keyspace"
)

// ZRANGESTORE used to call getZSet on the source, which clones every member and score
// of a coll-form sorted set onto the heap, run computeRange over the clone, then write
// the slice back with one db.Set. The whole source lived in RAM before a single byte
// reached the destination, so ZRANGESTORE dst src 0 -1 (or -inf +inf BYSCORE, or - +
// BYLEX) over a sorted set far larger than RAM OOM-killed under a tight cap even when
// the destination would have spilled to the coll form anyway. This is the range twin of
// the zset-algebra store sink (note 342).
//
// streamZRangeStore walks only the matched window of the coll source through the same
// bounded read-form cursors the read commands use, in resumable batches with the reader
// closed between them, and feeds each member into a zsetStoreSink that buffers a small
// result as one listpack blob and spills a large one into a fresh dual-index sub-tree.
// Neither the source nor the result is ever held whole; peak memory is one batch plus
// the sink's own listpack-threshold bound.
//
// The stored sorted set is re-sorted by the sink (and by the score-index key order once
// spilled), so only the window's membership matters, not the walk order. That lets every
// form use a forward cursor walk: a REV range stores the same members as its forward
// twin, and REV with LIMIT is turned into a forward offset/count by first counting the
// band (zrangeStoreWindow), so the reverse cursor is never needed here.
//
// When the destination is also the source (ZRANGESTORE dst dst ...) writing into the
// source mid-walk would mutate it, so the aliased case falls back to the buffered
// materialize, the same O(result) memory Redis itself uses to build a STORE result.

// handleZRangeStore implements ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV]
// [LIMIT offset count]. It computes the range over the source and stores it at the
// destination, preserving scores.
func handleZRangeStore(ctx *Ctx) {
	spec, errStr := parseZRangeArgs(ctx.Argv[5:])
	if errStr != "" {
		ctx.enc().WriteError(errStr)
		return
	}
	dst, src, minArg, maxArg := ctx.Argv[1], ctx.Argv[2], ctx.Argv[3], ctx.Argv[4]
	if bytes.Equal(dst, src) {
		zrangeStoreMaterialize(ctx, spec, dst, src, minArg, maxArg)
		return
	}
	streamZRangeStore(ctx, spec, dst, src, minArg, maxArg)
}

// streamZRangeStore stores the range of an independent source into the destination
// without materializing the whole source. A blob source is small by construction and
// keeps the buffered compute; a coll source streams its matched window into a sink.
func streamZRangeStore(ctx *Ctx, spec rangeSpec, dst, src, minArg, maxArg []byte) {
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
		rangeErr   string
	)
	done := ctx.update(func(db *keyspace.DB) error {
		// Only the source key is type-checked. The destination is overwritten whatever
		// it held, so a string or list at the destination is replaced, matching Redis.
		hdr, found, err := zsetHeader(db, src)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		if !found {
			existed, e := db.Delete(dst)
			dstDeleted = existed
			return e
		}
		if !hdr.IsColl() {
			// A blob source fits a listpack, so loading it is bounded; reuse the
			// buffered compute, which also serves as the test oracle for the streamed
			// coll path.
			members, _, _, e := getZSet(db, src)
			if e != nil {
				return e
			}
			var result []zmember
			result, rangeErr = computeRange(members, minArg, maxArg, spec)
			if rangeErr != "" {
				return nil
			}
			n = int64(len(result))
			if len(result) == 0 {
				existed, e := db.Delete(dst)
				dstDeleted = existed
				return e
			}
			stored := make([]zmember, len(result))
			copy(stored, result)
			zsetSort(stored)
			return db.Set(dst, zsetEncode(stored), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), stored, keyspace.EncListpack), -1)
		}
		// A coll source streams its matched window straight into the sink.
		sink := newZSetStoreSink(db, dst, ctx.encLimits(), false, aggSum)
		rangeErr, err = zrangeStoreWalk(db, src, minArg, maxArg, spec, sink)
		if err != nil {
			return err
		}
		if rangeErr != "" {
			// A bound parse error leaves the destination untouched: nothing was emitted.
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
	if rangeErr != "" {
		ctx.enc().WriteError(rangeErr)
		return
	}
	if n > 0 {
		ctx.notify(notifyZset, "zrangestore", dst)
		ctx.signalReady(dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}

// zrangeStoreWalk dispatches a coll-source range to the matching index walker, feeding
// each matched member into the sink. It returns a non-empty range error string when a
// bound fails to parse (before any member is emitted), mirroring computeRange.
func zrangeStoreWalk(db *keyspace.DB, src, minArg, maxArg []byte, spec rangeSpec, sink *zsetStoreSink) (rangeErr string, err error) {
	switch {
	case spec.byScore:
		// In a REV command the argument order is (max, min); resolve to (lo, hi).
		loArg, hiArg := minArg, maxArg
		if spec.rev {
			loArg, hiArg = maxArg, minArg
		}
		lo, ok := parseScoreBound(loArg)
		if !ok {
			return "ERR min or max is not a float", nil
		}
		hi, ok := parseScoreBound(hiArg)
		if !ok {
			return "ERR min or max is not a float", nil
		}
		skip, take, err := zrangeStoreLimit(db, spec, func() (int64, error) {
			_, n, e := zsetCollRangeByScore(db, src, lo, hi, false, 0, 0, true)
			return n, e
		})
		if err != nil {
			return "", err
		}
		_, err = zsetStoreScoreBand(db, src, lo, hi, skip, take, sink)
		return "", err
	case spec.byLex:
		loArg, hiArg := minArg, maxArg
		if spec.rev {
			loArg, hiArg = maxArg, minArg
		}
		lo, ok := parseLexBound(loArg)
		if !ok {
			return "ERR min or max not valid string range item", nil
		}
		hi, ok := parseLexBound(hiArg)
		if !ok {
			return "ERR min or max not valid string range item", nil
		}
		skip, take, err := zrangeStoreLimit(db, spec, func() (int64, error) {
			_, n, e := zsetCollRangeByLex(db, src, lo, hi, false, 0, 0, true)
			return n, e
		})
		if err != nil {
			return "", err
		}
		_, err = zsetStoreLexBand(db, src, lo, hi, skip, take, sink)
		return "", err
	default:
		start, ok := parseInteger(minArg)
		if !ok {
			return "ERR value is not an integer or out of range", nil
		}
		stop, ok := parseInteger(maxArg)
		if !ok {
			return "ERR value is not an integer or out of range", nil
		}
		_, err = zsetStoreRankWindow(db, src, start, stop, spec.rev, sink)
		return "", err
	}
}

// zrangeStoreLimit resolves a BYSCORE/BYLEX LIMIT into a forward (skip, take) pair.
// take < 0 means take every match from skip onward. A REV window selects from the high
// end, so it is turned into a forward offset by counting the band first (countBand),
// the only case that needs the extra pass. Without LIMIT the whole band is taken.
func zrangeStoreLimit(db *keyspace.DB, spec rangeSpec, countBand func() (int64, error)) (skip, take int64, err error) {
	if !spec.limit {
		return 0, -1, nil
	}
	if spec.offset < 0 {
		return 0, 0, nil // a negative offset yields nothing, matching applyLimit
	}
	if !spec.rev {
		return spec.offset, spec.count, nil // count < 0 keeps everything from the offset
	}
	total, err := countBand()
	if err != nil {
		return 0, 0, err
	}
	if spec.offset >= total {
		return 0, 0, nil
	}
	hiD := total // descending-order exclusive end of the selected window
	if spec.count >= 0 && spec.offset+spec.count < total {
		hiD = spec.offset + spec.count
	}
	// Descending positions [offset, hiD) are ascending positions [total-hiD, total-offset).
	return total - hiD, hiD - spec.offset, nil
}

// zsetStoreScoreBand forward-walks the score-index band [lo, hi] of a coll source in
// resumable batches, skips the first skip matches, feeds up to take matches (take < 0
// for all) into the sink, and returns how many it fed. The reader is closed between
// batches and reopened with a Seek-resume, so peak memory is one batch.
func zsetStoreScoreBand(db *keyspace.DB, key []byte, lo, hi scoreBound, skip, take int64, sink *zsetStoreSink) (int64, error) {
	if take == 0 {
		return 0, nil
	}
	var (
		resume     []byte
		haveResume bool
		fed        int64
	)
	for {
		batch := make([]zmember, 0, interCardBatch)
		var lastRow []byte
		done := false
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
				seek := encoding.AppendU64BE([]byte{zRowScore}, zScoreBits(lo.value))
				if e := c.Seek(seek); e != nil {
					return e
				}
			}
			for c.Valid() && len(batch) < interCardBatch {
				k := c.Key()
				if len(k) == 0 || k[0] != zRowScore {
					done = true
					break
				}
				score := zScoreUnbits(encoding.U64BE(k[1:9]))
				if zScoreAboveHigh(score, hi) {
					done = true
					break
				}
				lastRow = append(lastRow[:0], k...)
				if !scoreInRange(score, lo, hi) { // low-edge exclusive skip
					if e := c.Next(); e != nil {
						return e
					}
					continue
				}
				if skip > 0 {
					skip--
					if e := c.Next(); e != nil {
						return e
					}
					continue
				}
				batch = append(batch, zmember{member: append([]byte(nil), k[9:]...), score: score})
				if take >= 0 && fed+int64(len(batch)) >= take {
					done = true
					break
				}
				if e := c.Next(); e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return fed, err
		}
		for i := range batch {
			if e := sink.add(batch[i].member, batch[i].score); e != nil {
				return fed, e
			}
		}
		fed += int64(len(batch))
		if done || len(batch) == 0 {
			return fed, nil
		}
		resume = append([]byte(nil), lastRow...)
		haveResume = true
	}
}

// zsetStoreLexBand is the member-index twin of zsetStoreScoreBand: it forward-walks the
// lex band [lo, hi] of a coll source in resumable batches, applies skip/take, and feeds
// each member (with the score read from its member-index row) into the sink.
func zsetStoreLexBand(db *keyspace.DB, key []byte, lo, hi lexBound, skip, take int64, sink *zsetStoreSink) (int64, error) {
	if take == 0 || lo.inf == 1 || hi.inf == -1 { // low +inf or high -inf: empty band
		return 0, nil
	}
	var (
		resume     []byte
		haveResume bool
		fed        int64
	)
	for {
		batch := make([]zmember, 0, interCardBatch)
		var lastRow []byte
		done := false
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
				seek := []byte{zRowMember}
				if lo.inf != -1 {
					seek = zMemberRow(lo.value)
				}
				if e := c.Seek(seek); e != nil {
					return e
				}
			}
			for c.Valid() && len(batch) < interCardBatch {
				k := c.Key()
				if len(k) == 0 || k[0] != zRowMember {
					done = true
					break
				}
				member := k[1:]
				if !lexAfterLow(member, lo) { // low-edge exclusive skip
					if e := c.Next(); e != nil {
						return e
					}
					continue
				}
				if !lexBeforeHigh(member, hi) {
					done = true
					break
				}
				lastRow = append(lastRow[:0], k...)
				if skip > 0 {
					skip--
					if e := c.Next(); e != nil {
						return e
					}
					continue
				}
				batch = append(batch, zmember{
					member: append([]byte(nil), member...),
					score:  zScoreUnbits(encoding.U64BE(c.Value())),
				})
				if take >= 0 && fed+int64(len(batch)) >= take {
					done = true
					break
				}
				if e := c.Next(); e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return fed, err
		}
		for i := range batch {
			if e := sink.add(batch[i].member, batch[i].score); e != nil {
				return fed, e
			}
		}
		fed += int64(len(batch))
		if done || len(batch) == 0 {
			return fed, nil
		}
		resume = append([]byte(nil), lastRow...)
		haveResume = true
	}
}

// zsetStoreRankWindow stores the [start, stop] rank slice of a coll source. start and
// stop are the raw (possibly negative) rank arguments; rev selects the by-rank reverse
// window. The window resolves to an ascending score-index position range [aLo, aHi]
// (the same mapping streamZRangeByRank uses), which is walked forward in resumable
// batches so the order is irrelevant to the re-sorted destination.
func zsetStoreRankWindow(db *keyspace.DB, key []byte, start, stop int64, rev bool, sink *zsetStoreSink) (int64, error) {
	var card int64
	if _, err := db.CollRead(key, func(r *keyspace.CollReader) error {
		card = int64(r.Count())
		return nil
	}); err != nil {
		return 0, err
	}
	s, e := start, stop
	if s < 0 {
		s += card
	}
	if e < 0 {
		e += card
	}
	if s < 0 {
		s = 0
	}
	if e >= card {
		e = card - 1
	}
	if s > e || s >= card {
		return 0, nil
	}
	aLo, aHi := s, e
	if rev {
		aLo, aHi = card-1-e, card-1-s
	}
	take := aHi - aLo + 1

	var (
		resume     []byte
		haveResume bool
		fed        int64
	)
	for {
		batch := make([]zmember, 0, interCardBatch)
		var lastRow []byte
		done := false
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
			} else if e := seekScoreIndex(r, c, aLo, card, true); e != nil {
				return e
			}
			for c.Valid() && len(batch) < interCardBatch {
				k := c.Key()
				if len(k) == 0 || k[0] != zRowScore {
					done = true
					break
				}
				lastRow = append(lastRow[:0], k...)
				batch = append(batch, zmember{
					member: append([]byte(nil), k[9:]...),
					score:  zScoreUnbits(encoding.U64BE(k[1:9])),
				})
				if fed+int64(len(batch)) >= take {
					done = true
					break
				}
				if e := c.Next(); e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return fed, err
		}
		for i := range batch {
			if e := sink.add(batch[i].member, batch[i].score); e != nil {
				return fed, e
			}
		}
		fed += int64(len(batch))
		if done || len(batch) == 0 {
			return fed, nil
		}
		resume = append([]byte(nil), lastRow...)
		haveResume = true
	}
}

// zrangeStoreMaterialize is the fallback for ZRANGESTORE when the destination is also
// the source (ZRANGESTORE dst dst ...). It reads the source in full before writing the
// destination, so the source read stays consistent while it is rewritten, the same
// O(result) memory Redis uses to build any STORE result.
func zrangeStoreMaterialize(ctx *Ctx, spec rangeSpec, dst, src, minArg, maxArg []byte) {
	var (
		wrongTyp   bool
		dstDeleted bool
		n          int64
		rangeErr   string
	)
	done := ctx.update(func(db *keyspace.DB) error {
		members, hdr, found, err := getZSet(db, src)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeZSet {
			wrongTyp = true
			return nil
		}
		var result []zmember
		result, rangeErr = computeRange(members, minArg, maxArg, spec)
		if rangeErr != "" {
			return nil
		}
		n = int64(len(result))
		if len(result) == 0 {
			existed, err := db.Delete(dst)
			dstDeleted = existed
			return err
		}
		stored := make([]zmember, len(result))
		copy(stored, result)
		zsetSort(stored)
		return db.Set(dst, zsetEncode(stored), keyspace.TypeZSet, zsetEncoding(ctx.encLimits(), stored, keyspace.EncListpack), -1)
	})
	if !done {
		return
	}
	if wrongTyp {
		ctx.enc().WriteError(wrongTypeError)
		return
	}
	if rangeErr != "" {
		ctx.enc().WriteError(rangeErr)
		return
	}
	if n > 0 {
		ctx.notify(notifyZset, "zrangestore", dst)
		ctx.signalReady(dst)
	} else if dstDeleted {
		ctx.notify(notifyGeneric, "del", dst)
	}
	ctx.enc().WriteInteger(n)
}
