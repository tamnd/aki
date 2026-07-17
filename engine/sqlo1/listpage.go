package sqlo1

// Fence paging, doc 07's second fence rung and the zset score fence's
// discipline transplanted to positional entries. A fence past
// listFenceMaxNodes moves into page records, subkey kind 3 under the
// list's plane, and the root keeps a page index whose entries carry
// per-page element totals, so a positional seek prefix-sums two
// levels and lands in 3 records at any length.
//
// The crash story is zfencepage.go's, with one list-specific twist.
// An in-place page rewrite rides the same drain batch as the root
// that owns it, atomic at the store seam. Fresh pageids (the
// transition, spills, page splits) land and flush BEFORE the root
// that references them: a crash prefix reads the old root and the
// fresh pages are orphans the plane retire cleans. Replaced page
// records die after the root lands, a bounded orphan. The twist: the
// list has no W3 reconciliation and no clip tolerance, so an amended
// edge NODE must never be separated from its root by that flush
// either. Every paged writer therefore stages the edge amendment in
// its own buffer first, writes fresh nodes and fresh pages, flushes,
// and only then lands the amended node, the in-place page, and the
// root in one batch. Page images always carry exact counts, and every
// load cross-checks its parent's total.

import (
	"context"
	"fmt"
)

// fenceElems sums an entry array's element counts.
func fenceElems(ents []listFenceEnt) uint64 {
	sum := uint64(0)
	for _, e := range ents {
		sum += uint64(e.count)
	}
	return sum
}

// loadPage reads page j of the index into l.fence, replacing whatever
// page was loaded. The entries are copied out on decode, so they
// survive the node reads that follow; the one-page cache means the
// deque paths reload only when they cross a page boundary.
func (l *List) loadPage(ctx context.Context, j int) error {
	if l.pi == j {
		return nil
	}
	r := &l.nodeRoot
	pe := r.pidx[j]
	putHashFenceKey(l.pkbuf[:], r.rooth, pe.segid)
	v, ok, err := l.t.Get(ctx, l.pkbuf[:])
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("sqlo1: list fence page %d of rooth %#x is missing", pe.segid, r.rooth)
	}
	ents, sum, err := decodeListFencePage(v, r.nextSegid, l.fence[:0])
	if err != nil {
		return err
	}
	if sum != uint64(pe.count) {
		return fmt.Errorf("sqlo1: list fence page %d sums to %d elements, index says %d", pe.segid, sum, pe.count)
	}
	l.fence = ents
	l.pi = j
	return nil
}

// pageForPos prefix-sums the page index to the page holding absolute
// position pos and the position local to it. The caller bounds pos
// below the root count.
func (l *List) pageForPos(pos int64) (j int, local int64) {
	for i := range l.nodeRoot.pidx {
		c := int64(l.nodeRoot.pidx[i].count)
		if pos < c {
			return i, pos
		}
		pos -= c
	}
	return len(l.nodeRoot.pidx) - 1, pos
}

// fenceSeekAt is fenceSeek across both fence rungs: paged, it loads
// the covering page first, and the returned entry index is local to
// l.fence either way.
func (l *List) fenceSeekAt(ctx context.Context, pos int64) (ni int, off int, err error) {
	if l.nodeRoot.paged {
		j, local := l.pageForPos(pos)
		if err := l.loadPage(ctx, j); err != nil {
			return 0, 0, err
		}
		pos = local
	}
	ni, off = l.fenceSeek(pos)
	return ni, off, nil
}

// writePageRaw lands a page payload under pageid.
func (l *List) writePageRaw(ctx context.Context, pageid uint64, ents []listFenceEnt) error {
	l.pageBuf = appendListFencePage(l.pageBuf[:0], ents)
	putHashFenceKey(l.pkbuf[:], l.nodeRoot.rooth, pageid)
	return l.t.SetGen(ctx, l.pkbuf[:], l.pageBuf, TagList|TagFence, l.nodeRoot.rootgen)
}

// writeFencePage rewrites the loaded page in place from l.fence and
// refreshes its index total. In-place rewrites ride the same batch as
// the root, so they need no flush barrier.
func (l *List) writeFencePage(ctx context.Context) error {
	if err := l.writePageRaw(ctx, l.nodeRoot.pidx[l.pi].segid, l.fence); err != nil {
		return err
	}
	l.nodeRoot.pidx[l.pi].count = uint32(fenceElems(l.fence))
	return nil
}

// writeFreshPage mints a pageid from the shared segid counter and
// lands ents under it, returning the index entry. The caller owns the
// flush-before-root barrier.
func (l *List) writeFreshPage(ctx context.Context, ents []listFenceEnt) (listFenceEnt, error) {
	r := &l.nodeRoot
	pageid := r.nextSegid
	if err := l.writePageRaw(ctx, pageid, ents); err != nil {
		return listFenceEnt{}, err
	}
	r.nextSegid++
	return listFenceEnt{segid: pageid, count: uint32(fenceElems(ents))}, nil
}

// delPage drops a page record, always after the root that stopped
// referencing it.
func (l *List) delPage(ctx context.Context, pageid uint64) error {
	putHashFenceKey(l.pkbuf[:], l.nodeRoot.rooth, pageid)
	_, err := l.t.Del(ctx, l.pkbuf[:])
	return err
}

// pageChunks bounds how many pages an entry array chunks into, and
// chunkEnts cuts chunk i of it: full pages with the partial chunk
// last, or first when partialFirst, which leaves growth room at the
// pushed end after a transition.
func pageChunks(n int) int {
	return (n + listFencePageMax - 1) / listFencePageMax
}

func chunkEnts(ents []listFenceEnt, i int, partialFirst bool) []listFenceEnt {
	if !partialFirst {
		lo := i * listFencePageMax
		return ents[lo:min(lo+listFencePageMax, len(ents))]
	}
	first := len(ents) - (pageChunks(len(ents))-1)*listFencePageMax
	if i == 0 {
		return ents[:first]
	}
	lo := first + (i-1)*listFencePageMax
	return ents[lo : lo+listFencePageMax]
}

// pageFence moves a whole fence into fresh pages: the flat-to-paged
// transition and the upgrade path that overshoots the flat cap. It
// writes every page and builds the index in place of r.pidx, leaving
// the caller to flush before the root that flips the paged bit; on a
// crash short of that root the old root still reads the flat fence
// and the pages are orphans. The capacity check runs first, so a
// refused transition is side-effect free.
func (l *List) pageFence(ctx context.Context, ents []listFenceEnt, partialFirst bool) error {
	r := &l.nodeRoot
	n := pageChunks(len(ents))
	if n > listFencePageIdxMax {
		return errListFenceThirdLevel
	}
	r.pidx = l.pidxBuf[:0]
	for i := range n {
		pe, err := l.writeFreshPage(ctx, chunkEnts(ents, i, partialFirst))
		if err != nil {
			return err
		}
		r.pidx = append(r.pidx, pe)
	}
	l.pidxBuf = r.pidx
	r.paged = true
	r.fence = nil
	l.pi = -1
	return nil
}
