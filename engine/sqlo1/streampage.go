package sqlo1

// Stream fence paging, doc 10's second fence rung: listpage.go's
// discipline at the stream entry width, simpler because a stream only
// grows at its tail. A fence past streamFenceMaxRuns moves into page
// records, subkey kind 3 under the stream's plane, and the root keeps a
// page index whose entries carry per-page base IDs and live totals, so
// an ID seek binary-searches two levels and a range decodes only
// boundary runs in boundary pages.
//
// The crash story is the T5 one unchanged. An in-place rewrite of the
// tail page rides the same drain batch as the root that owns it, atomic
// at the store seam, and so does the tail run amendment it accompanies.
// Fresh pageids (the transition, a full tail page's successor) land and
// flush BEFORE the root that references them: a crash prefix reads the
// old root and the fresh pages are orphans the plane retire cleans.

import (
	"context"
	"fmt"
)

// streamFenceElems sums an entry array's live counts.
func streamFenceElems(ents []streamFenceEnt) uint64 {
	sum := uint64(0)
	for _, e := range ents {
		sum += uint64(e.count)
	}
	return sum
}

// streamSeekLE finds the last entry whose base is at or below id, -1
// when every base is above it. Bases are strictly increasing, so a
// binary search lands it; the same helper serves the page index and a
// loaded page.
func streamSeekLE(ents []streamFenceEnt, id streamID) int {
	lo, hi := 0, len(ents)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if id.less(ents[mid].base) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo - 1
}

// streamFenceSeekIn finds the interval of entries whose runs (or pages)
// can hold IDs in [start, end]: lo is the last entry whose base is at
// or below start (clamped to 0), hi the last whose base is at or below
// end. last is the stream's last generated ID, the upper wall. ok is
// false when nothing can overlap.
func streamFenceSeekIn(ents []streamFenceEnt, last, start, end streamID) (lo, hi int, ok bool) {
	if len(ents) == 0 || end.less(ents[0].base) || last.less(start) {
		return 0, 0, false
	}
	hi = streamSeekLE(ents, end)
	lo = max(streamSeekLE(ents, start), 0)
	return lo, hi, true
}

// loadPage reads page j of the index into x.fence, replacing whatever
// page was loaded, and cross-checks the two-level invariant: the page's
// first base is the index base and its counts sum to the index total.
func (x *Stream) loadPage(ctx context.Context, j int) error {
	if x.pi == j {
		return nil
	}
	pe := x.root.pidx[j]
	putHashFenceKey(x.pkbuf[:], x.root.rooth, pe.segid)
	v, ok, err := x.t.Get(ctx, x.pkbuf[:])
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("sqlo1: stream fence page %d of rooth %#x is missing", pe.segid, x.root.rooth)
	}
	ents, sum, err := decodeStreamFencePage(v, x.root.nextSegid, x.fence[:0])
	if err != nil {
		return err
	}
	if sum != uint64(pe.count) {
		return fmt.Errorf("sqlo1: stream fence page %d sums to %d entries, index says %d", pe.segid, sum, pe.count)
	}
	if ents[0].base != pe.base {
		return fmt.Errorf("sqlo1: stream fence page %d starts at %d-%d, index says %d-%d", pe.segid, ents[0].base.ms, ents[0].base.seq, pe.base.ms, pe.base.seq)
	}
	x.fence = ents
	x.pi = j
	return nil
}

// writePageRaw lands a page payload under pageid.
func (x *Stream) writePageRaw(ctx context.Context, pageid uint64, ents []streamFenceEnt) error {
	x.pageBuf = appendStreamFencePage(x.pageBuf[:0], ents)
	putHashFenceKey(x.pkbuf[:], x.root.rooth, pageid)
	return x.t.SetGen(ctx, x.pkbuf[:], x.pageBuf, TagStream|TagFence, x.root.rootgen)
}

// writeFencePage rewrites the loaded page in place from x.fence and
// refreshes its index total. In-place rewrites ride the same batch as
// the root, so they need no flush barrier.
func (x *Stream) writeFencePage(ctx context.Context) error {
	if err := x.writePageRaw(ctx, x.root.pidx[x.pi].segid, x.fence); err != nil {
		return err
	}
	x.root.pidx[x.pi].base = x.fence[0].base
	x.root.pidx[x.pi].count = uint32(streamFenceElems(x.fence))
	return nil
}

// writeFreshPage mints a pageid from the shared segid counter and lands
// ents under it, returning the index entry. The caller owns the
// flush-before-root barrier.
func (x *Stream) writeFreshPage(ctx context.Context, ents []streamFenceEnt) (streamFenceEnt, error) {
	r := &x.root
	pageid := r.nextSegid
	if err := x.writePageRaw(ctx, pageid, ents); err != nil {
		return streamFenceEnt{}, err
	}
	r.nextSegid++
	return streamFenceEnt{base: ents[0].base, segid: pageid, count: uint32(streamFenceElems(ents))}, nil
}

// streamPageChunks bounds how many pages an entry array chunks into.
func streamPageChunks(n int) int {
	return (n + streamFencePageMax - 1) / streamFencePageMax
}

// pageStreamFence moves a whole fence into fresh pages, the
// flat-to-paged transition. Full pages cut from the front with the
// partial chunk last, leaving growth room at the tail, the only end a
// stream grows at. It writes every page and builds the index in
// x.pidxBuf, leaving the caller to flush before the root that flips the
// paged bit; on a crash short of that root the old root still reads the
// flat fence and the pages are orphans. The capacity check runs first,
// so a refused transition is side-effect free.
func (x *Stream) pageStreamFence(ctx context.Context, ents []streamFenceEnt) error {
	r := &x.root
	n := streamPageChunks(len(ents))
	if n > streamFencePageIdxMax {
		return errStreamFenceThirdLevel
	}
	r.pidx = x.pidxBuf[:0]
	for i := range n {
		lo := i * streamFencePageMax
		pe, err := x.writeFreshPage(ctx, ents[lo:min(lo+streamFencePageMax, len(ents))])
		if err != nil {
			return err
		}
		r.pidx = append(r.pidx, pe)
	}
	x.pidxBuf = r.pidx
	r.paged = true
	r.fence = nil
	x.pi = -1
	return nil
}
