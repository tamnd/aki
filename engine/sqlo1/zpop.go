package sqlo1

// The zset pop family's storage half, doc 09 section 4: ZPOPMIN,
// ZPOPMAX, and ZMPOP pop from the rank window's end of the collection,
// and ZRANDMEMBER samples uniform ranks, both over the same rank
// primitives the range family runs on. A pop collects its window
// first (the popped pairs are the reply, so the arena costs what the
// reply already costs), then removes the pairs under one deferred
// root, so however many member segments, runs, merges, or page edits
// the removals touch, the command lands exactly one full root frame,
// last, and a crash prefix replays to the whole pop or none of it
// (Z-I4). The blocking forms are the server layer's; storage only
// ever sees the pop.

import (
	"context"
	"encoding/binary"
	"fmt"
)

// ZPopCount pops min(count, ZCard) pairs from the low end (maxSide
// false, ZPOPMIN's ascending order) or the high end (maxSide true,
// ZPOPMAX's descending order). begin runs exactly once, before any
// emit, with the exact number popped; emitted member bytes live in
// the pop arena and stay valid through the emit sequence. An absent
// key or a non-positive count is begin(0) with nothing removed;
// popping the last pair deletes the key, Redis's empty-key rule.
func (z *ZSet) ZPopCount(ctx context.Context, key []byte, count int64, maxSide bool, begin func(n int64), emit func(score float64, member []byte)) error {
	h := z.h
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return err
	}
	if st == hashAbsent || count <= 0 {
		begin(0)
		return nil
	}
	if st == hashInlineState {
		return z.zpopInline(ctx, key, hi, expMs, count, maxSide, begin, emit)
	}

	r := &h.segRoot
	card := int64(r.count)
	k := min(count, card)
	lo, hiRank := int64(0), k
	if maxSide {
		lo, hiRank = card-k, card
	}
	z.zparena, z.zppairs = z.zparena[:0], z.zppairs[:0]
	err = z.zwalkRank(ctx, key, lo, hiRank, func(su uint64, m []byte) bool {
		off := len(z.zparena)
		z.zparena = append(z.zparena, m...)
		z.zppairs = append(z.zppairs, zbuildPair{s: su, off: off, end: len(z.zparena)})
		return true
	})
	if err != nil {
		return err
	}
	if int64(len(z.zppairs)) != k {
		return fmt.Errorf("sqlo1: segmented zset root claims %d members, rank window held %d", card, len(z.zppairs))
	}

	if k == card {
		// Full pop: the key dies and the plane retires whole behind a
		// genbump, runs and fence pages included, nothing edited.
		h.t.Bump(key, r.rooth, r.rootgen+1)
		if _, err := h.t.Del(ctx, key); err != nil {
			return err
		}
		z.zpopEmit(k, maxSide, begin, emit)
		return nil
	}

	// One frame group for every removal: each pair drops from its
	// member segment and its score run through the ZRem machinery
	// (merges, run deaths, and page edits included), and the root
	// lands once, after all of them, as the group's commit point. The
	// window never holds the last member, so no removal can retire the
	// plane mid-group.
	h.deferRoot = true
	h.t.ht.pinRoot(key)
	defer func() { h.deferRoot, h.rootPend = false, false; h.t.ht.unpinRoot() }()
	for _, p := range z.zppairs {
		member := z.zparena[p.off:p.end]
		removed, err := h.hdelSeg(ctx, key, member, expMs)
		if err != nil {
			return err
		}
		if !removed {
			return fmt.Errorf("sqlo1: zset member %q of rooth %#x vanished mid-pop", member, h.segRoot.rooth)
		}
		ok, err := z.zrunDelSeg(ctx, key, p.s, member)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("sqlo1: zset member %q of rooth %#x is missing from its score run", member, h.segRoot.rooth)
		}
	}
	if err := z.zflushRoot(ctx, key, expMs); err != nil {
		return err
	}
	z.zpopEmit(k, maxSide, begin, emit)
	return nil
}

// zpopInline pops from an inline root. The inline region sits in
// (score, member) order, so the popped pairs are a contiguous span at
// the region's low or high end: the span copies to the pop arena, the
// survivors rebuild the root in one write, and popping everything
// deletes the key, with no plane to retire.
func (z *ZSet) zpopInline(ctx context.Context, key []byte, hi hashInline, expMs int64, count int64, maxSide bool, begin func(n int64), emit func(score float64, member []byte)) error {
	h := z.h
	n := hi.count
	k := min(count, int64(n))
	skip := int(k)
	if maxSide {
		skip = n - int(k)
	}
	it := hashEntryIter{p: hi.entries, enc: h.enc}
	for i := 0; i < skip; i++ {
		if _, _, _, _, err := it.next(); err != nil {
			return err
		}
	}
	split := len(hi.entries) - len(it.p)
	pop, keep := hi.entries[:split], hi.entries[split:]
	if maxSide {
		pop, keep = hi.entries[split:], hi.entries[:split]
	}

	z.zparena, z.zppairs = z.zparena[:0], z.zppairs[:0]
	it = hashEntryIter{p: pop, enc: h.enc}
	for {
		m, v, _, ok, err := it.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		off := len(z.zparena)
		z.zparena = append(z.zparena, m...)
		z.zppairs = append(z.zppairs, zbuildPair{s: binary.BigEndian.Uint64(v), off: off, end: len(z.zparena)})
	}

	if int(k) == n {
		if _, err := h.t.Del(ctx, key); err != nil {
			return err
		}
		z.zpopEmit(k, maxSide, begin, emit)
		return nil
	}
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	h.rootBuf = append(h.rootBuf, keep...)
	putHashInlineHdr(h.rootBuf, h.subInline, n-int(k), 0, false)
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return err
	}
	if err := h.restamp(ctx, key, expMs); err != nil {
		return err
	}
	z.zpopEmit(k, maxSide, begin, emit)
	return nil
}

// zpopEmit replays the collected window to the caller: forward for a
// min pop, backward for a max pop, so the emission order is always
// nearest-end-first, Redis's reply order.
func (z *ZSet) zpopEmit(k int64, maxSide bool, begin func(n int64), emit func(score float64, member []byte)) {
	begin(k)
	if maxSide {
		for i := len(z.zppairs) - 1; i >= 0; i-- {
			p := z.zppairs[i]
			emit(zScoreFromSortable(p.s), z.zparena[p.off:p.end])
		}
		return
	}
	for _, p := range z.zppairs {
		emit(zScoreFromSortable(p.s), z.zparena[p.off:p.end])
	}
}

// ZRandMemberCount samples count pairs: distinct uniform ranks when
// withReplacement is false (capped at the cardinality), independent
// uniform ranks when true (exactly count draws). Every draw is a rank
// descent, so cost is O(draws) run reads regardless of size. begin
// runs exactly once, before any emit, with the exact emit count;
// emitted member bytes alias a run read and are only valid inside the
// emit call. An absent key or non-positive count is begin(0).
func (z *ZSet) ZRandMemberCount(ctx context.Context, key []byte, count int64, withReplacement bool, begin func(n int64), emit func(score float64, member []byte)) error {
	card, err := z.ZCard(ctx, key)
	if err != nil {
		return err
	}
	if card == 0 || count <= 0 {
		begin(0)
		return nil
	}
	fetch := func(rank int64) error {
		return z.zwalkRank(ctx, key, rank, rank+1, func(su uint64, m []byte) bool {
			emit(zScoreFromSortable(su), m)
			return false
		})
	}
	if withReplacement {
		begin(count)
		for i := int64(0); i < count; i++ {
			if err := fetch(int64(z.h.rand64() % uint64(card))); err != nil {
				return err
			}
		}
		return nil
	}
	k := min(count, card)
	begin(k)
	if k == card {
		return z.zwalkRank(ctx, key, 0, card, func(su uint64, m []byte) bool {
			emit(zScoreFromSortable(su), m)
			return true
		})
	}
	// A sparse partial Fisher-Yates over the exact rank space: swap
	// holds only the touched positions, so drawing k of a 100M-member
	// zset costs k map entries and k descents, and the picks are
	// exactly uniform and land in random order.
	swap := make(map[int64]int64, 2*k)
	at := func(i int64) int64 {
		if v, ok := swap[i]; ok {
			return v
		}
		return i
	}
	for j := int64(0); j < k; j++ {
		r := j + int64(z.h.rand64()%uint64(card-j))
		vr, vj := at(r), at(j)
		swap[r], swap[j] = vj, vr
		if err := fetch(vr); err != nil {
			return err
		}
	}
	return nil
}
