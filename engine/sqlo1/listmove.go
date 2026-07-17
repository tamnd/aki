package sqlo1

import (
	"bytes"
	"context"
)

// Move pops one element from src's chosen end and pushes it onto dst's
// chosen end, LMOVE's storage half (RPOPLPUSH is the fixed
// right-to-left call). ok is false when src is missing; the returned
// element stays valid until the next call on this List.
//
// The crash story is SMove's frame group, doc 07's two-root row. Both
// keys type-gate before any write, so a wrong-typed dst never leaves
// src half-moved. The push to dst goes first and the pop from src
// second, so the element is in at least one list at every drain batch
// boundary, and the guard keeps the pair's frames contiguous: a record
// the pop will coalesce into (src root or its edge node) that is
// already dirty holds a drain-queue position ahead of the push's fresh
// entries, and a batch cut there would commit the pop before the push,
// so the tier flushes first. dst-side dirt only moves the push
// earlier, the safe direction, but it is checked too so the pair's
// frames stay contiguous in the WAL and a torn tail replays the move
// all-or-nothing.
//
// A same-key move stays on the single-root path: the two writes
// coalesce into one root image in the hot tier, so the rotation runs
// pop first (no transient growth to trip the inline thresholds) and a
// same-end move answers the element without writing at all.
func (l *List) Move(ctx context.Context, src, dst []byte, srcLeft, dstLeft bool) ([]byte, bool, error) {
	st, li, _, err := l.stateOf(ctx, src)
	if err != nil {
		return nil, false, err
	}
	if st == listAbsent {
		return nil, false, nil
	}

	// Copy the moving element out while the src view is live, and
	// collect the pop side of the flush guard from the same state: the
	// records the pop coalesces into are the src root and, noded, the
	// edge node it rewrites or drops, plus the edge page a paged fence
	// rewrites in place.
	dirty := l.t.ht.dirtyKey(src)
	if st == listNodedState {
		if l.nodeRoot.paged {
			pj := 0
			if !srcLeft {
				pj = len(l.nodeRoot.pidx) - 1
			}
			if err := l.loadPage(ctx, pj); err != nil {
				return nil, false, err
			}
			putHashFenceKey(l.kbuf[:], l.nodeRoot.rooth, l.nodeRoot.pidx[pj].segid)
			dirty = dirty || l.t.ht.dirtyKey(l.kbuf[:])
		}
		ei := 0
		if !srcLeft {
			ei = len(l.fence) - 1
		}
		ent := l.fence[ei]
		putHashSegKey(l.kbuf[:], l.nodeRoot.rooth, ent.segid)
		dirty = dirty || l.t.ht.dirtyKey(l.kbuf[:])
		node, err := l.readNode(ctx, ent.segid)
		if err != nil {
			return nil, false, err
		}
		l.moveBuf = append(l.moveBuf[:0], edgeElem(node.elems, srcLeft)...)
	} else {
		l.moveBuf = append(l.moveBuf[:0], edgeElem(li.elems, srcLeft)...)
	}

	if bytes.Equal(src, dst) {
		if srcLeft == dstLeft {
			// Pop then push at the same end is the identity.
			return l.moveBuf, true, nil
		}
		if _, _, err := l.Pop(ctx, src, srcLeft, 1); err != nil {
			return nil, false, err
		}
		if _, err := l.Push(ctx, dst, dstLeft, false, l.moveBuf); err != nil {
			return nil, false, err
		}
		return l.moveBuf, true, nil
	}

	// The dst type gate, before any write. Its state read clobbers the
	// src views, which is why the element copied out above.
	dstSt, _, _, err := l.stateOf(ctx, dst)
	if err != nil {
		return nil, false, err
	}
	dirty = dirty || l.t.ht.dirtyKey(dst)
	if dstSt == listNodedState {
		if l.nodeRoot.paged {
			pj := 0
			if !dstLeft {
				pj = len(l.nodeRoot.pidx) - 1
			}
			if err := l.loadPage(ctx, pj); err != nil {
				return nil, false, err
			}
			putHashFenceKey(l.kbuf[:], l.nodeRoot.rooth, l.nodeRoot.pidx[pj].segid)
			dirty = dirty || l.t.ht.dirtyKey(l.kbuf[:])
		}
		ei := 0
		if !dstLeft {
			ei = len(l.fence) - 1
		}
		putHashSegKey(l.kbuf[:], l.nodeRoot.rooth, l.fence[ei].segid)
		dirty = dirty || l.t.ht.dirtyKey(l.kbuf[:])
	}
	if dirty {
		if err := l.t.Flush(ctx); err != nil {
			return nil, false, err
		}
	}
	if _, err := l.Push(ctx, dst, dstLeft, false, l.moveBuf); err != nil {
		return nil, false, err
	}
	if _, _, err := l.Pop(ctx, src, srcLeft, 1); err != nil {
		return nil, false, err
	}
	return l.moveBuf, true, nil
}

// edgeElem returns the first or last element span of a decoded element
// region. The span aliases the region and dies with it.
func edgeElem(region []byte, first bool) []byte {
	it := listElemIter{p: region}
	e, _ := it.next()
	if first {
		return e
	}
	for {
		x, ok := it.next()
		if !ok {
			return e
		}
		e = x
	}
}
