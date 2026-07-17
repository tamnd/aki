package sqlo1

// The ZREMRANGE family's storage half, doc 09 section 4: every form
// resolves to a forward rank window [lo, hi) before storage is
// touched, so removal is rank arithmetic on the score fence. Runs
// wholly inside the window die whole, one small tombstone frame each
// and never a rewrite; the window's edge runs rewrite once; and the
// member side batches its removals by fence segment, so a window of
// thousands touches each member segment once however many members it
// loses there. Both sides run under one deferred root, Z-I4's
// discipline: the command lands exactly one full root frame, last,
// and a crash prefix replays to the whole trim or none of it.

import (
	"context"
	"fmt"
)

// ZRemRange removes the members with forward rank in [lo, hi),
// answering how many went. The window clamps to the cardinality, and
// the trim that empties the zset deletes the key (Redis's empty-key
// rule), on the segmented rung the O(1) plane retire.
func (z *ZSet) ZRemRange(ctx context.Context, key []byte, lo, hi int64) (int64, error) {
	h := z.h
	st, hin, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	if st == hashAbsent {
		return 0, nil
	}
	if lo < 0 {
		lo = 0
	}
	if st == hashInlineState {
		return z.zremRangeInline(ctx, key, hin, expMs, lo, hi)
	}

	r := &h.segRoot
	card := int64(r.count)
	if hi > card {
		hi = card
	}
	if hi <= lo {
		return 0, nil
	}
	k := hi - lo
	if k == card {
		// Full-window trim: the key dies and the plane retires whole
		// behind a genbump, runs and fence pages included, nothing
		// edited.
		h.t.Bump(key, r.rooth, r.rootgen+1)
		if _, err := h.t.Del(ctx, key); err != nil {
			return 0, err
		}
		return k, nil
	}

	// The member side needs the window's members and the score side
	// only its ranks: one walk collects the members into the pop
	// arena before either side edits anything.
	z.zparena, z.zppairs = z.zparena[:0], z.zppairs[:0]
	err = z.zwalkRank(ctx, key, lo, hi, func(su uint64, m []byte) bool {
		off := len(z.zparena)
		z.zparena = append(z.zparena, m...)
		z.zppairs = append(z.zppairs, zbuildPair{s: su, off: off, end: len(z.zparena)})
		return true
	})
	if err != nil {
		return 0, err
	}
	if int64(len(z.zppairs)) != k {
		return 0, fmt.Errorf("sqlo1: segmented zset root claims %d members, rank window held %d", card, len(z.zppairs))
	}

	// One frame group for both sides: the member segments each
	// rewrite once for their share of the window, the score fence
	// kills and trims its runs, and the root lands once, after all of
	// it, as the group's commit point. The window never holds the
	// last member, so nothing here can retire the plane mid-group.
	h.deferRoot = true
	h.t.ht.pinRoot(key)
	defer func() { h.deferRoot, h.rootPend = false, false; h.t.ht.unpinRoot() }()
	members := make([][]byte, len(z.zppairs))
	for i, p := range z.zppairs {
		members[i] = z.zparena[p.off:p.end]
	}
	if err := h.hdelSegBatch(ctx, key, members); err != nil {
		return 0, err
	}
	if err := z.zrunDelRange(ctx, key, lo, k); err != nil {
		return 0, err
	}
	if err := z.zflushRoot(ctx, key, expMs); err != nil {
		return 0, err
	}
	return k, nil
}

// zremRangeInline trims an inline root: the ordered region holds the
// window as one contiguous byte span, so the survivors are head plus
// tail and the root rebuilds in one write. Trimming everything
// deletes the key, with no plane to retire.
func (z *ZSet) zremRangeInline(ctx context.Context, key []byte, hin hashInline, expMs int64, lo, hi int64) (int64, error) {
	h := z.h
	n := int64(hin.count)
	if hi > n {
		hi = n
	}
	if hi <= lo {
		return 0, nil
	}
	k := hi - lo
	if k == n {
		_, err := h.t.Del(ctx, key)
		return k, err
	}
	it := hashEntryIter{p: hin.entries, enc: h.enc}
	for i := int64(0); i < lo; i++ {
		if _, _, _, _, err := it.next(); err != nil {
			return 0, err
		}
	}
	cutStart := len(hin.entries) - len(it.p)
	for i := int64(0); i < k; i++ {
		if _, _, _, _, err := it.next(); err != nil {
			return 0, err
		}
	}
	cutEnd := len(hin.entries) - len(it.p)
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	h.rootBuf = append(h.rootBuf, hin.entries[:cutStart]...)
	h.rootBuf = append(h.rootBuf, hin.entries[cutEnd:]...)
	putHashInlineHdr(h.rootBuf, h.subInline, int(n-k), 0, false)
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return 0, err
	}
	return k, h.restamp(ctx, key, expMs)
}

// zrunDelRange removes the score-side entries with forward rank in
// [lo, lo+k). Each pass descends the fence's count totals to the run
// holding rank lo, kills it whole when the window covers it, or
// rewrites the edge once; killed entries shift their successors down
// to rank lo and every descent reads post-edit counts, so lo never
// moves while remaining drains. The caller holds the deferred root.
func (z *ZSet) zrunDelRange(ctx context.Context, key []byte, lo, k int64) error {
	if err := z.zloadTail(); err != nil {
		return err
	}
	for remaining := k; remaining > 0; {
		// The descent is zwalkRank's: subtrees wholly below lo skip
		// unread, and the loaded pages are exactly the run's covers.
		off := lo
		if z.zpaged {
			ui := 0
			for ; ui < len(z.zridx); ui++ {
				if c := int64(z.zridx[ui].count); off < c {
					break
				} else {
					off -= c
				}
			}
			if ui == len(z.zridx) {
				return fmt.Errorf("sqlo1: zset trim of rooth %#x ran past the root index with %d left", z.h.segRoot.rooth, remaining)
			}
			if err := z.loadZUpper(ctx, ui); err != nil {
				return err
			}
			li := 0
			for ; li < len(z.zupper); li++ {
				if c := int64(z.zupper[li].count); off < c {
					break
				} else {
					off -= c
				}
			}
			if err := z.loadZLeaf(ctx, ui, li); err != nil {
				return err
			}
		}
		ei := 0
		for ; ei < len(z.zfence); ei++ {
			if c := int64(z.zfence[ei].count); off < c {
				break
			} else {
				off -= c
			}
		}
		if ei == len(z.zfence) {
			return fmt.Errorf("sqlo1: zset trim of rooth %#x ran past the fence with %d left", z.h.segRoot.rooth, remaining)
		}
		e := &z.zfence[ei]
		span := min(int64(e.count)-off, remaining)
		if off == 0 && span == int64(e.count) {
			// The window covers the run: it dies whole, one small
			// tombstone frame, never a rewrite (doc 09's
			// interior-run rule).
			z.zbumpLoaded(-span)
			if err := z.zrunDie(ctx, key, ei); err != nil {
				return err
			}
		} else {
			img, err := z.readRun(ctx, e.segid)
			if err != nil {
				return err
			}
			cutStart := zRunHdrLen
			for j := int64(0); j < off; j++ {
				_, _, next, err := zRunEntAt(img, cutStart)
				if err != nil {
					return err
				}
				cutStart = next
			}
			cutEnd := cutStart
			for j := int64(0); j < span; j++ {
				_, _, next, err := zRunEntAt(img, cutEnd)
				if err != nil {
					return err
				}
				cutEnd = next
			}
			_, _, liveEnd, err := zrunPos(img, e.count, 0, nil)
			if err != nil {
				return err
			}
			n := int(int64(e.count) - span)
			z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
			putZRunHdr(z.zrbuf, n)
			z.zrbuf = append(z.zrbuf, img[zRunHdrLen:cutStart]...)
			z.zrbuf = append(z.zrbuf, img[cutEnd:liveEnd]...)
			z.zbumpLoaded(-span)
			merged, err := z.tryMergeRun(ctx, key, ei, n)
			if err != nil {
				return err
			}
			if !merged {
				if err := z.writeRun(ctx, e.segid, z.zrbuf); err != nil {
					return err
				}
				e.count = uint32(n)
				if err := z.zfenceCommit(ctx, key); err != nil {
					return err
				}
			}
		}
		remaining -= span
	}
	return nil
}
