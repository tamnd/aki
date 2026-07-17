package sqlo1

// The zset range family's storage half, doc 09 section 4: every BY
// form reduces to two primitives. zseekRank answers the number of
// entries ordered below a (sortable, member) pair, which is the
// insertion rank; a score bound is the pair (s, "") because no member
// orders below the empty one, an exclusive score bound is (s+1, ""),
// and a lex bound is the pair against the zset's one shared score.
// zwalkRank streams a window of forward ranks in order, seeking the
// start run by fence-count descent and never touching a run before
// the window, so a range on a 100M-member cold zset is the root, the
// covering pages, and one run read before output streaming starts.
// The command layer resolves Redis's bound grammar, REV mirroring,
// and LIMIT into rank arithmetic over these two, and ZRANGESTORE
// walks the same window into the bulk build (zstore.go).

import (
	"context"
	"encoding/binary"
)

// zseekRank answers the insertion rank of (s, member): how many
// entries order strictly below the pair, alongside the cardinality.
// An absent key answers 0, 0.
func (z *ZSet) zseekRank(ctx context.Context, key []byte, s uint64, member []byte) (rank, card int64, err error) {
	h := z.h
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil || st == hashAbsent {
		return 0, 0, err
	}
	if st == hashInlineState {
		it := hashEntryIter{p: hi.entries, enc: h.enc}
		for {
			m, v, _, ok, err := it.next()
			if err != nil {
				return 0, 0, err
			}
			if !ok {
				return rank, int64(hi.count), nil
			}
			es := binary.BigEndian.Uint64(v)
			if es > s || (es == s && string(m) >= string(member)) {
				return rank, int64(hi.count), nil
			}
			rank++
		}
	}
	if err := z.zloadTail(); err != nil {
		return 0, 0, err
	}
	card = int64(h.segRoot.count)
	ri, err := z.zrunRoute(ctx, s, member)
	if err != nil {
		return 0, 0, err
	}
	rank = z.zprefixLoaded(ri)
	e := z.zfence[ri]
	if e.count == 0 {
		return rank, card, nil
	}
	img, err := z.readRun(ctx, e.segid)
	if err != nil {
		return 0, 0, err
	}
	idx, err := zrunLowerIdx(img, e.count, s, member)
	if err != nil {
		return 0, 0, err
	}
	return rank + int64(idx), card, nil
}

// zrunLowerIdx answers the index of the first live entry at or above
// (s, member), or count when the whole run orders below it: zrunIdx
// for an insertion point instead of a hit.
func zrunLowerIdx(img []byte, count uint32, s uint64, member []byte) (int, error) {
	off := zRunHdrLen
	for i := uint32(0); i < count; i++ {
		es, em, next, err := zRunEntAt(img, off)
		if err != nil {
			return 0, err
		}
		if es > s || (es == s && string(em) >= string(member)) {
			return int(i), nil
		}
		off = next
	}
	return int(count), nil
}

// zfirstSortable answers the smallest sortable score in the zset, the
// shared score a lex bound pairs with. ok is false on an absent or
// (impossible today, defensively) empty key.
func (z *ZSet) zfirstSortable(ctx context.Context, key []byte) (uint64, bool, error) {
	var s uint64
	ok := false
	err := z.zwalkRank(ctx, key, 0, 1, func(es uint64, m []byte) bool {
		s, ok = es, true
		return false
	})
	return s, ok, err
}

// zwalkRank streams the entries with forward rank in [lo, hi) in
// (score, member) order. Emitted bytes alias the run read and die on
// the next Tiered call; emit answers false to stop early. The walk
// reads no run before the window: the start seeks by fence-count
// descent, so cost is the covering pages plus the runs the window
// spans.
func (z *ZSet) zwalkRank(ctx context.Context, key []byte, lo, hi int64, emit func(s uint64, member []byte) bool) error {
	if hi <= lo {
		return nil
	}
	h := z.h
	st, hin, _, err := h.stateOf(ctx, key)
	if err != nil || st == hashAbsent {
		return err
	}
	if st == hashInlineState {
		it := hashEntryIter{p: hin.entries, enc: h.enc}
		for rank := int64(0); rank < hi; rank++ {
			m, v, _, ok, err := it.next()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			if rank >= lo && !emit(binary.BigEndian.Uint64(v), m) {
				return nil
			}
		}
		return nil
	}
	if err := z.zloadTail(); err != nil {
		return err
	}
	n := hi - lo

	// Descend the count totals to the start run: subtrees wholly
	// below lo skip unread.
	ui, li := 0, 0
	if z.zpaged {
		for ; ui < len(z.zridx); ui++ {
			if c := int64(z.zridx[ui].count); lo < c {
				break
			} else {
				lo -= c
			}
		}
		if ui == len(z.zridx) {
			return nil
		}
		if err := z.loadZUpper(ctx, ui); err != nil {
			return err
		}
		for ; li < len(z.zupper); li++ {
			if c := int64(z.zupper[li].count); lo < c {
				break
			} else {
				lo -= c
			}
		}
		if err := z.loadZLeaf(ctx, ui, li); err != nil {
			return err
		}
	}
	ei := 0
	for ; ei < len(z.zfence); ei++ {
		if c := int64(z.zfence[ei].count); lo < c {
			break
		} else {
			lo -= c
		}
	}

	// Stream runs forward from (ui, li, ei), skipping lo entries
	// inside the first, until n entries emitted or the fence ends.
	for {
		for ; ei < len(z.zfence); ei++ {
			e := z.zfence[ei]
			if e.count == 0 {
				continue
			}
			img, err := z.readRun(ctx, e.segid)
			if err != nil {
				return err
			}
			off := zRunHdrLen
			for j := uint32(0); j < e.count; j++ {
				s, m, next, err := zRunEntAt(img, off)
				if err != nil {
					return err
				}
				off = next
				if lo > 0 {
					lo--
					continue
				}
				if !emit(s, m) {
					return nil
				}
				if n--; n == 0 {
					return nil
				}
			}
		}
		if !z.zpaged {
			return nil
		}
		if li++; li == len(z.zupper) {
			if ui++; ui == len(z.zridx) {
				return nil
			}
			if err := z.loadZUpper(ctx, ui); err != nil {
				return err
			}
			li = 0
		}
		if err := z.loadZLeaf(ctx, ui, li); err != nil {
			return err
		}
		ei = 0
	}
}
