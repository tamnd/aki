package sqlo1

// XDEL, doc 10's arbitrary-deletion slice. A delete flips the entry's
// tomb bit in its run, so the cost is one boundary-style run rewrite
// per touched run and the root; a run whose last live entry falls
// drops whole, dead survivors included, exactly like a trim boundary,
// and a fence page emptied that way drops after the root that stopped
// referencing it. The max-deleted-ID root field advances to the
// largest ID actually deleted (X-I2 keeps it across trims), and the
// last generated ID and entries-added never move, so a stream deleted
// to empty keeps generating above its history.

import (
	"context"
	"slices"
)

// Del is XDEL: tombstone every id that currently exists live and
// report how many fell. ids may arrive unsorted with duplicates; the
// slice is sorted and deduped in place, so a duplicate counts once,
// the pinned Redis shape. A missing key deletes nothing.
func (x *Stream) Del(ctx context.Context, key []byte, ids []streamID) (int64, error) {
	exists, expMs, err := x.stateOf(ctx, key)
	if err != nil || !exists {
		return 0, err
	}
	slices.SortFunc(ids, func(a, b streamID) int {
		if a.less(b) {
			return -1
		}
		if b.less(a) {
			return 1
		}
		return 0
	})
	ids = slices.Compact(ids)
	r := &x.root
	var removed int64
	var lastHit streamID
	var hit bool
	x.deadPages = x.deadPages[:0]
	if r.paged {
		for lo := 0; lo < len(ids); {
			pi := streamSeekLE(r.pidx, ids[lo])
			if pi < 0 {
				lo++
				continue
			}
			hi := lo + 1
			for hi < len(ids) && streamSeekLE(r.pidx, ids[hi]) == pi {
				hi++
			}
			if err := x.loadPage(ctx, pi); err != nil {
				return 0, err
			}
			n, last, ok, err := x.delInFence(ctx, ids[lo:hi])
			if err != nil {
				return 0, err
			}
			if ok {
				lastHit, hit = last, true
			}
			if n > 0 {
				removed += n
				if len(x.fence) == 0 {
					x.deadPages = append(x.deadPages, r.pidx[pi].segid)
					r.pidx[pi].count = 0
				} else if err := x.writeFencePage(ctx); err != nil {
					return 0, err
				}
			}
			lo = hi
		}
		if removed == 0 {
			return 0, nil
		}
		live := r.pidx[:0]
		for i := range r.pidx {
			if r.pidx[i].count > 0 {
				live = append(live, r.pidx[i])
			}
		}
		r.pidx = live
		x.pi = -1
	} else {
		n, last, ok, err := x.delInFence(ctx, ids)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, nil
		}
		removed, lastHit, hit = n, last, ok
	}
	r.count -= uint64(removed)
	if hit && r.maxDel.less(lastHit) {
		r.maxDel = lastHit
	}
	if err := x.writeRoot(ctx, key); err != nil {
		return 0, err
	}
	for _, pid := range x.deadPages {
		if err := x.delFencePage(ctx, pid); err != nil {
			return 0, err
		}
	}
	x.deadPages = x.deadPages[:0]
	return removed, x.restamp(ctx, key, expMs)
}

// delInFence tombs ids inside the fence view in scratch (the flat
// fence or one loaded page), rewriting each touched run once and
// deleting the runs whose live count reaches zero, then compacts the
// emptied entries out of x.fence. ids are sorted and deduped. It
// reports the live entries removed and the largest ID that fell.
func (x *Stream) delInFence(ctx context.Context, ids []streamID) (removed int64, lastHit streamID, hit bool, err error) {
	for lo := 0; lo < len(ids); {
		fi := streamSeekLE(x.fence, ids[lo])
		if fi < 0 {
			lo++
			continue
		}
		hi := lo + 1
		for hi < len(ids) && streamSeekLE(x.fence, ids[hi]) == fi {
			hi++
		}
		n, last, ok, err := x.delInRun(ctx, fi, ids[lo:hi])
		if err != nil {
			return 0, streamID{}, false, err
		}
		if ok {
			removed += n
			lastHit, hit = last, true
		}
		lo = hi
	}
	if removed > 0 {
		live := x.fence[:0]
		for i := range x.fence {
			if x.fence[i].count > 0 {
				live = append(live, x.fence[i])
			}
		}
		x.fence = live
	}
	return removed, lastHit, hit, nil
}

// delInRun rewrites the run at fence index fi with the tomb bits of
// every matching live entry set, trimBoundary's decode-and-reencode
// shape. A run left with no live entry deletes whole and zeroes its
// fence count for the caller's compaction.
func (x *Stream) delInRun(ctx context.Context, fi int, ids []streamID) (removed int64, lastHit streamID, hit bool, err error) {
	fe := &x.fence[fi]
	v, err := x.readRun(ctx, fe.segid)
	if err != nil {
		return 0, streamID{}, false, err
	}
	x.ents, x.fvPool, x.fvOffs = x.ents[:0], x.fvPool[:0], x.fvOffs[:0]
	live := uint32(0)
	_, err = walkStreamRun(v, func(_ int, e streamEntry) error {
		if !e.dead {
			if _, ok := slices.BinarySearchFunc(ids, e.id, func(a, b streamID) int {
				if a.less(b) {
					return -1
				}
				if b.less(a) {
					return 1
				}
				return 0
			}); ok {
				e.dead = true
				removed++
				lastHit, hit = e.id, true
			} else {
				live++
			}
		}
		x.fvOffs = append(x.fvOffs, len(x.fvPool))
		x.fvPool = append(x.fvPool, e.fv...)
		x.ents = append(x.ents, streamEntry{id: e.id, dead: e.dead})
		return nil
	})
	if err != nil {
		return 0, streamID{}, false, err
	}
	if removed == 0 {
		return 0, streamID{}, false, nil
	}
	if live == 0 {
		if err := x.delRun(ctx, fe.segid); err != nil {
			return 0, streamID{}, false, err
		}
		fe.count = 0
		return removed, lastHit, hit, nil
	}
	x.fvOffs = append(x.fvOffs, len(x.fvPool))
	for i := range x.ents {
		x.ents[i].fv = x.fvPool[x.fvOffs[i]:x.fvOffs[i+1]]
	}
	x.runBuf = appendStreamRun(x.runBuf[:0], x.ents)
	if err := x.writeRun(ctx, fe.segid, x.runBuf); err != nil {
		return 0, streamID{}, false, err
	}
	fe.count -= uint32(removed)
	return removed, lastHit, hit, nil
}
