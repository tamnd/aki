package sqlo1

// SPOP with a count, doc 08 section 5: destructive uniform sampling
// with batched removal. The strategy ladder is the spop lab's verdict
// (labs/sqlo1/t3/02_spop): count >= cardinality is the trivial full
// delete; count >= popRebuildFactor x fence length emits the popped
// members and bulk-rebuilds the survivors into a fresh packed plane;
// anything below edits every touched segment in place, exactly once,
// with a take that covers a segment's whole live count writing the
// bare 12-byte empty image instead of rebuilding entries.

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
)

// popRebuildFactor is the edit-vs-rebuild switch: a pop of count >=
// popRebuildFactor x fence length rebuilds. The lab put the bytes
// crossover between 8 and 16 takes per segment at every size, with
// the arms within one percent of each other near the boundary; 8 is
// the low end because rebuild is already ahead on write frames there
// and the repack heals churn occupancy for free.
const popRebuildFactor = 8

// SPopCount is SPOP key count: it pops min(count, SCARD) distinct
// uniform members. begin runs exactly once, before any emit, with the
// exact number popped; emitted bytes are only valid inside the emit
// call. An absent key or a zero count is begin(0) with nothing
// removed; popping the last member deletes the key, Redis's empty-set
// rule.
func (s *Set) SPopCount(ctx context.Context, key []byte, count int64, begin func(n int64), emit func(member []byte)) error {
	h := s.h
	st, hi, expMs, err := h.stateOfLive(ctx, key)
	if err != nil {
		return err
	}
	if st == hashAbsent || count <= 0 {
		begin(0)
		return nil
	}
	if st == hashInlineState {
		return s.popInline(ctx, key, hi, expMs, count, begin, emit)
	}
	r := &h.segRoot
	card := int64(r.count)
	if count >= card {
		// Full pop: emit everything, then the key dies and the plane
		// retires whole, hdelSeg's last-field path.
		begin(card)
		emitted, err := h.iterSegEntries(ctx, func(f, _ []byte) { emit(f) })
		if err != nil {
			return err
		}
		if emitted != r.count {
			return fmt.Errorf("sqlo1: segmented set root claims %d members, segments hold %d", r.count, emitted)
		}
		h.t.Bump(key, r.rooth, r.rootgen+1)
		_, err = h.t.Del(ctx, key)
		return err
	}
	// Fence length in O(1) from the root: exact on a flat root, pages
	// times page capacity on a paged one. The paged bound overstates
	// the true segment count by at most 2x (a split page holds at
	// least half capacity), which moves the effective switch inside
	// the lab's insensitive 8..16 window.
	fenceLen := int64(len(r.fence))
	if r.paged {
		fenceLen = int64(len(r.pidx)) * hashFencePageMax
	}
	if count >= popRebuildFactor*fenceLen {
		return s.popRebuild(ctx, key, expMs, count, begin, emit)
	}
	return s.popEdit(ctx, key, expMs, count, begin, emit)
}

// popInline pops count distinct members of an inline set: a partial
// Fisher-Yates over the entry indexes picks the subset (randInline's
// stack arrays), then one region walk emits the picked spans and
// rebuilds the root without them. count >= the live count deletes the
// key; the inline root is planeless, so there is nothing to retire.
func (s *Set) popInline(ctx context.Context, key []byte, hi hashInline, expMs int64, count int64, begin func(n int64), emit func(member []byte)) error {
	h := s.h
	n := hi.count
	if count >= int64(n) {
		begin(int64(n))
		it := hashEntryIter{p: hi.entries, enc: h.enc}
		for {
			f, _, _, ok, err := it.next()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			emit(f)
		}
		_, err := h.t.Del(ctx, key)
		return err
	}
	c := int(count)
	var idx [hashInlineMaxCount]int
	for i := range n {
		idx[i] = i
	}
	for k := range c {
		j := k + int(h.rand64()%uint64(n-k))
		idx[k], idx[j] = idx[j], idx[k]
	}
	var sel [hashInlineMaxCount]bool
	for _, i := range idx[:c] {
		sel[i] = true
	}
	begin(count)
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	it := hashEntryIter{p: hi.entries, enc: h.enc}
	for k := 0; ; k++ {
		before := it.p
		f, _, _, ok, err := it.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if sel[k] {
			emit(f)
			continue
		}
		h.rootBuf = append(h.rootBuf, before[:len(before)-len(it.p)]...)
	}
	// Removals never move the intset bit in either direction, the
	// one-way rule SREM already follows.
	putHashInlineHdr(h.rootBuf, h.subInline, n-c, 0, hi.allInt)
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return err
	}
	return h.restamp(ctx, key, expMs)
}

// popPick is one drawn entry of the edit arm: the member copy lives in
// the reservoir arena, and the (segid, entry index) pair keys both the
// dedupe and the removal grouping. The index is stable between the
// draw and the removal because nothing writes the segment in between;
// both reads decode the same record image.
type popPick struct {
	segid uint64
	fh    uint64
	ei    int
	slot  rvSlot
}

// popEdit is the below-threshold arm. The draw phase is the
// HRANDFIELD distinct rejection sampler (near-uniform under the
// fill-class approximation the hrand lab guards) with popFill as its
// deterministic valve, recording (segid, entry index) instead of
// emitting. The removal phase rewrites every touched segment exactly
// once; a take that covers a segment's whole live count writes the
// bare header image with no entry walk (the whole-segment fast path).
// The emptied segment's fence entry stays for the lazy merge to fold
// on a later removal, doc 06's empty-segment stance, so the fence
// shape never changes under a pop and the root write stays a
// count-only delta (rule W2).
func (s *Set) popEdit(ctx context.Context, key []byte, expMs int64, count int64, begin func(n int64), emit func(member []byte)) error {
	h := s.h
	r := &h.segRoot
	total := h.buildDrawWeights()
	if h.picked == nil {
		h.picked = make(map[uint64]struct{}, count)
	}
	clear(h.picked)
	h.rvArena = h.rvArena[:0]
	picks := make([]popPick, 0, count)
	remaining := count
	for tries := 20*count + 100; remaining > 0 && tries > 0; tries-- {
		fi, ei, f, _, err := h.drawSegEntry(ctx, total)
		if err != nil {
			return err
		}
		segid := r.fence[fi].segid
		k := segid*4096 + uint64(ei)
		if _, dup := h.picked[k]; dup {
			continue
		}
		h.picked[k] = struct{}{}
		picks = append(picks, popPick{segid: segid, fh: hashFH(f), ei: ei, slot: h.rvPut(f, nil)})
		remaining--
	}
	if remaining > 0 {
		var err error
		picks, remaining, err = s.popFill(ctx, picks, remaining)
		if err != nil {
			return err
		}
		if remaining > 0 {
			return fmt.Errorf("sqlo1: segmented set root claims %d members, pop sample ran dry %d short", r.count, remaining)
		}
	}
	// Removal: picks group by segid, and a group's fence entry is
	// re-found through any member's fh (fenceIdx reloads the covering
	// page on a paged root). Segments write before the root, hdelSeg's
	// order.
	sort.Slice(picks, func(a, b int) bool { return picks[a].segid < picks[b].segid })
	for start := 0; start < len(picks); {
		end := start + 1
		for end < len(picks) && picks[end].segid == picks[start].segid {
			end++
		}
		if err := s.popSeg(ctx, picks[start:end]); err != nil {
			return err
		}
		start = end
	}
	r.count -= uint64(count)
	if err := h.writeSegRoot(ctx, key, true); err != nil {
		return err
	}
	begin(count)
	for _, p := range picks {
		f, _ := h.rvGet(p.slot)
		emit(f)
	}
	return h.restamp(ctx, key, expMs)
}

// popFill is the draw phase's valve, fenceFill with picks: it finishes
// the distinct quota deterministically in fence order, page by page on
// a paged root. Returns what it could not fill, which is zero unless
// the root count lied.
func (s *Set) popFill(ctx context.Context, picks []popPick, remaining int64) ([]popPick, int64, error) {
	h := s.h
	r := &h.segRoot
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := 0; p < pages && remaining > 0; p++ {
		if err := h.loadPage(ctx, p); err != nil {
			return picks, remaining, err
		}
		for i := 0; i < len(r.fence) && remaining > 0; i++ {
			seg, err := h.readSeg(ctx, r.fence[i].segid)
			if err != nil {
				return picks, remaining, err
			}
			it := hashEntryIter{p: seg.entries, enc: seg.enc}
			for j := 0; remaining > 0; j++ {
				f, _, _, ok, err := it.next()
				if err != nil {
					return picks, remaining, err
				}
				if !ok {
					break
				}
				k := r.fence[i].segid*4096 + uint64(j)
				if _, dup := h.picked[k]; dup {
					continue
				}
				h.picked[k] = struct{}{}
				picks = append(picks, popPick{segid: r.fence[i].segid, fh: hashFH(f), ei: j, slot: h.rvPut(f, nil)})
				remaining--
			}
		}
	}
	return picks, remaining, nil
}

// popSeg rewrites one touched segment without its picked entries. The
// picks all share the segment; a take that covers the whole live count
// writes the bare header image with no entry walk.
func (s *Set) popSeg(ctx context.Context, picks []popPick) error {
	h := s.h
	r := &h.segRoot
	i, err := h.fenceIdx(ctx, picks[0].fh)
	if err != nil {
		return err
	}
	if r.fence[i].segid != picks[0].segid {
		return fmt.Errorf("sqlo1: pop pick fh %#x routes to segment %d, drawn from %d", picks[0].fh, r.fence[i].segid, picks[0].segid)
	}
	seg, err := h.readSeg(ctx, picks[0].segid)
	if err != nil {
		return err
	}
	kept := seg.n - len(picks)
	if kept < 0 {
		return fmt.Errorf("sqlo1: %d pop picks in segment %d of %d entries", len(picks), picks[0].segid, seg.n)
	}
	h.segBuf = grow(h.segBuf, hashSegHdrLen)
	if kept > 0 {
		drop := make(map[int]bool, len(picks))
		for _, p := range picks {
			drop[p.ei] = true
		}
		it := hashEntryIter{p: seg.entries, enc: seg.enc}
		for k := 0; ; k++ {
			before := it.p
			_, _, _, ok, err := it.next()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if drop[k] {
				continue
			}
			h.segBuf = append(h.segBuf, before[:len(before)-len(it.p)]...)
		}
	}
	putHashSegHdr(h.segBuf, kept, 0)
	if err := h.writeSeg(ctx, picks[0].segid, h.segBuf); err != nil {
		return err
	}
	return h.setFenceMeta(ctx, i, hashSegMeta(kept, 0))
}

// popRebuild is the at-or-above-threshold arm: the pop touches
// essentially every segment, so instead of editing them all the walk
// emits the popped members and packs the survivors into a fresh
// plane, old plane retired by one genbump, the doc 09 section 6 bulk
// pattern. The popped subset is count distinct walk positions drawn
// by a sparse partial Fisher-Yates over the exact root count, so this
// arm is exactly uniform (the lab's position allocator).
//
// The packed images accumulate in memory and land only after the walk
// (a store write inside the walk would recycle the IO round the
// walk's aliases live in), so the arm's high-water mark is the packed
// survivor bytes plus their fence. The plane lands and flushes before
// the root that references it, upgrade's crash rule: every crash
// prefix reads the old root over the old plane, and the new plane is
// bounded orphan garbage until the root lands.
func (s *Set) popRebuild(ctx context.Context, key []byte, expMs int64, count int64, begin func(n int64), emit func(member []byte)) error {
	h := s.h
	r := &h.segRoot
	card := r.count

	// Draw min(count, card-count) distinct positions and flip the
	// test when the popped side is the bigger one; the complement of
	// a uniform distinct subset is uniform.
	sel := uint64(count)
	invert := false
	if kept := card - uint64(count); kept < sel {
		sel, invert = kept, true
	}
	pos := make(map[uint64]struct{}, sel)
	swap := make(map[uint64]uint64, 2*sel)
	at := func(i uint64) uint64 {
		if v, ok := swap[i]; ok {
			return v
		}
		return i
	}
	for k := uint64(0); k < sel; k++ {
		j := k + h.rand64()%(card-k)
		vj, vk := at(j), at(k)
		swap[j], swap[k] = vk, vj
		pos[vj] = struct{}{}
	}

	// One walk in fence order, which is global (fh, member) order, so
	// the packed segments come out sorted for free. Cuts land at
	// seg_max and never between equal fh values, the fence rule.
	begin(count)
	var (
		pack   []byte   // concatenated finished segment images
		cuts   []int    // end offset of each image in pack
		lows   []uint64 // first fh of each image
		segN   int      // entries in the open segment
		segOff int      // pack offset of the open segment's header
		segLo  uint64   // first fh of the open segment
		prevFH uint64
		g      uint64 // global walk position
	)
	openSeg := func(lo uint64) {
		segOff = len(pack)
		pack = append(pack, make([]byte, hashSegHdrLen)...)
		segN, segLo = 0, lo
	}
	closeSeg := func() {
		putHashSegHdr(pack[segOff:], segN, 0)
		cuts = append(cuts, len(pack))
		lows = append(lows, segLo)
	}
	openSeg(0)
	popped := uint64(0)
	walked, err := h.iterSegEntries(ctx, func(f, _ []byte) {
		_, in := pos[g]
		g++
		if in != invert {
			emit(f)
			popped++
			return
		}
		fh := hashFH(f)
		if segN > 0 && len(pack)-segOff+hashEntrySize(len(f), 0, 0, encSet) > hashSegMax && fh != prevFH {
			closeSeg()
			openSeg(fh)
		}
		pack = appendHashEntry(pack, f, nil, 0, encSet)
		segN++
		prevFH = fh
	})
	if err != nil {
		return err
	}
	if walked != card {
		return fmt.Errorf("sqlo1: segmented set root claims %d members, segments hold %d", card, walked)
	}
	if popped != uint64(count) {
		return fmt.Errorf("sqlo1: pop of %d drew %d positions", count, popped)
	}
	closeSeg()

	// Land the plane under a fresh rooth at rootgen 1: segments, then
	// fence pages when the packed fence outgrows the flat root, all
	// fenced from the root by one flush.
	nsegs := len(cuts)
	rooth, err := h.nextRooth(ctx)
	if err != nil {
		return err
	}
	fence := make([]hashFenceEnt, 0, nsegs)
	off := 0
	for i, end := range cuts {
		img := pack[off:end]
		putHashSegKey(h.kbuf2[:], rooth, uint64(i))
		if err := h.t.SetGen(ctx, h.kbuf2[:], img, h.tag, 1); err != nil {
			return err
		}
		lo := lows[i]
		if i == 0 {
			lo = 0
		}
		fence = append(fence, hashFenceEnt{
			lo:    lo,
			segid: uint64(i),
			meta:  hashSegMeta(int(binary.LittleEndian.Uint16(img)), 0),
		})
		off = end
	}
	nextSegid := uint64(nsegs)
	nr := hashSegRoot{sub: h.subSeg, rootgen: 1, rooth: rooth, count: card - uint64(count), pi: -1}
	if nsegs > hashFenceMaxSegs {
		// Paged from birth, pages packed full like the segments.
		if (nsegs+hashFencePageMax-1)/hashFencePageMax > hashFencePageIdxMax {
			return errHashFenceThirdLevel
		}
		for base := 0; base < nsegs; base += hashFencePageMax {
			pe := fence[base:min(base+hashFencePageMax, nsegs)]
			pageid := nextSegid
			nextSegid++
			h.pageBuf = appendHashFencePage(h.pageBuf[:0], pe)
			putHashFenceKey(h.kbuf2[:], rooth, pageid)
			if err := h.t.SetGen(ctx, h.kbuf2[:], h.pageBuf, h.tag|TagFence, 1); err != nil {
				return err
			}
			nr.pidx = append(nr.pidx, hashPageEnt{lo: pe[0].lo, pageid: pageid, weight: hashPageWeight(pe)})
		}
		nr.paged = true
	} else {
		nr.fence = fence
	}
	nr.nextSegid = nextSegid
	if err := h.t.Flush(ctx); err != nil {
		return err
	}
	h.t.Bump(key, r.rooth, r.rootgen+1)
	h.segRoot = nr
	h.fence = nr.fence
	h.pidx = nr.pidx
	if err := h.writeSegRoot(ctx, key, false); err != nil {
		return err
	}
	return h.restamp(ctx, key, expMs)
}
