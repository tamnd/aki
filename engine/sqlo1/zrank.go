package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// Rank math, doc 09 section 2: a member's rank is a prefix sum over
// the score fence counts plus its index inside the covering run, so
// the whole ask is one member-segment read for the score, bounded
// fence arithmetic, and one run read (Z-I3), at any zset size. The
// flat fence keeps the counts in the root; the paged fence keeps
// per-page totals so the sum stays bounded scans (the zrank lab's
// two-level 250/250 verdict, #1014), and this surface reads through
// z.zfence either way once that slice lands.

// ZScore answers member's score, the ZSCORE surface: root plus at
// most one member segment, hot or cold.
func (z *ZSet) ZScore(ctx context.Context, key, member []byte) (float64, bool, error) {
	v, _, ok, err := z.h.getEntry(ctx, key, member)
	if err != nil || !ok {
		return 0, false, err
	}
	if len(v) != zmemScoreLen {
		return 0, false, fmt.Errorf("sqlo1: zset member score of %d bytes, want %d", len(v), zmemScoreLen)
	}
	return zScoreFromSortable(binary.BigEndian.Uint64(v)), true, nil
}

// ZMScore answers every member's score in argument order through the
// member side's batched cold path, one IO round for all the misses
// (doc 09's ZMSCORE row). ok false is the nil slot; emitted values
// are plain floats and outlive the call.
func (z *ZSet) ZMScore(ctx context.Context, key []byte, members [][]byte, emit func(score float64, ok bool)) error {
	return z.h.HMGet(ctx, key, members, func(v []byte, ok bool) {
		if !ok || len(v) != zmemScoreLen {
			emit(0, false)
			return
		}
		emit(zScoreFromSortable(binary.BigEndian.Uint64(v)), true)
	})
}

// ZRank answers member's 0-based ascending rank and its score (the
// WITHSCORE half rides free, the walk holds the sortable anyway).
func (z *ZSet) ZRank(ctx context.Context, key, member []byte) (int64, float64, bool, error) {
	rank, _, s, ok, err := z.zrankOf(ctx, key, member)
	if err != nil || !ok {
		return 0, 0, false, err
	}
	return rank, zScoreFromSortable(s), true, nil
}

// ZRevRank is ZRank from the high end: card-1-rank, one extra
// subtraction over the same walk.
func (z *ZSet) ZRevRank(ctx context.Context, key, member []byte) (int64, float64, bool, error) {
	rank, total, s, ok, err := z.zrankOf(ctx, key, member)
	if err != nil || !ok {
		return 0, 0, false, err
	}
	return total - 1 - rank, zScoreFromSortable(s), true, nil
}

// zrankOf walks member to its ascending rank: the member count total
// beside it (ZREVRANK's other operand) and the sortable score the
// walk resolved on the way.
func (z *ZSet) zrankOf(ctx context.Context, key, member []byte) (rank, total int64, s uint64, ok bool, err error) {
	h := z.h
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil || st == hashAbsent {
		return 0, 0, 0, false, err
	}

	if st == hashInlineState {
		// The inline region is kept in (score, member) order, so the
		// entry index is the rank.
		it := hashEntryIter{p: hi.entries, enc: h.enc}
		for {
			m, v, _, entOK, err := it.next()
			if err != nil {
				return 0, 0, 0, false, err
			}
			if !entOK {
				return 0, 0, 0, false, nil
			}
			if bytes.Equal(m, member) {
				if len(v) != zmemScoreLen {
					return 0, 0, 0, false, fmt.Errorf("sqlo1: zset member score of %d bytes, want %d", len(v), zmemScoreLen)
				}
				return rank, int64(hi.count), binary.BigEndian.Uint64(v), true, nil
			}
			rank++
		}
	}

	// Segmented: the member segment answers the score, the fence
	// routes to the covering run, the run scan lands the offset.
	fh := hashFH(member)
	i, err := h.fenceIdx(ctx, fh)
	if err != nil {
		return 0, 0, 0, false, err
	}
	seg, err := h.readSeg(ctx, h.segRoot.fence[i].segid)
	if err != nil {
		return 0, 0, 0, false, err
	}
	v, _, entOK, err := hashSegGet(seg, fh, member)
	if err != nil || !entOK {
		return 0, 0, 0, false, err
	}
	if len(v) != zmemScoreLen {
		return 0, 0, 0, false, fmt.Errorf("sqlo1: zset member score of %d bytes, want %d", len(v), zmemScoreLen)
	}
	s = binary.BigEndian.Uint64(v)
	total = int64(h.segRoot.count)

	z.zfence, err = decodeZTail(h.segRoot.tail, z.zfence)
	if err != nil {
		return 0, 0, 0, false, err
	}
	ri, err := z.zrunRoute(ctx, s, member)
	if err != nil {
		return 0, 0, 0, false, err
	}
	for k := range ri {
		rank += int64(z.zfence[k].count)
	}
	e := z.zfence[ri]
	img, err := z.readRun(ctx, e.segid)
	if err != nil {
		return 0, 0, 0, false, err
	}
	idx, found, err := zrunIdx(img, e.count, s, member)
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !found {
		// Z-I2 broken: the member side holds a pair the score side
		// does not.
		return 0, 0, 0, false, fmt.Errorf("sqlo1: zset member %q of rooth %#x is missing from its score run", member, h.segRoot.rooth)
	}
	return rank + int64(idx), total, s, true, nil
}
