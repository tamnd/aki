package sqlo1

// HRANDFIELD, doc 06 section 3: random draws over the fence's fill
// classes. A segmented draw picks a fence entry with probability
// proportional to 2*fillclass+1 (the entry-count midpoint of the
// class's quantization bucket), then a uniform entry inside the
// segment, so one draw costs one segment read instead of a full walk.
// The class is a 4-bit approximation, so per-entry probability is
// near-uniform, not exact; the hrand lab's chi-square run guards how
// near. Distinct sampling climbs a ladder by count: emit-all, then
// reservoir, then rejection over the weighted primitive.

import (
	"context"
	"fmt"
	"sort"
)

// rand64 steps the hash layer's splitmix64 generator. Draw quality
// only has to satisfy the chi-square lab; there is no adversary,
// which is the same stance Redis takes with random().
func (h *Hash) rand64() uint64 {
	h.rngState += 0x9e3779b97f4a7c15
	z := h.rngState
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// rvSlot locates one reservoir entry in rvArena: the field is
// rvArena[off:off+flen], the value the vlen bytes after it.
type rvSlot struct {
	off  int
	flen int
	vlen int
}

// rvPut copies an entry into the reservoir arena. The copies are what
// let the reservoir emit after the pass: emitted segment bytes alias
// their IO round and are long dead by then.
func (h *Hash) rvPut(f, v []byte) rvSlot {
	s := rvSlot{off: len(h.rvArena), flen: len(f), vlen: len(v)}
	h.rvArena = append(h.rvArena, f...)
	h.rvArena = append(h.rvArena, v...)
	return s
}

func (h *Hash) rvGet(s rvSlot) (f, v []byte) {
	f = h.rvArena[s.off : s.off+s.flen]
	v = h.rvArena[s.off+s.flen : s.off+s.flen+s.vlen]
	return f, v
}

// buildDrawWeights fills h.wsum with the cumulative draw weights one
// weighted draw searches: per fence entry flat (2*fillclass+1), per
// page paged (the index's stored per-page sums of the same weights).
// Returns the total. Weight zero cannot happen (an empty segment
// still weighs 1 and decode rejects a zero page weight), so the total
// is always positive when a fence exists.
func (h *Hash) buildDrawWeights() uint64 {
	r := &h.segRoot
	h.wsum = h.wsum[:0]
	total := uint64(0)
	if r.paged {
		for _, e := range r.pidx {
			total += uint64(e.weight)
			h.wsum = append(h.wsum, total)
		}
		return total
	}
	for _, e := range r.fence {
		total += 2*uint64(e.meta&hashMetaFillMask) + 1
		h.wsum = append(h.wsum, total)
	}
	return total
}

// drawFenceIdx picks a fence index with probability proportional to
// its fill-class weight; buildDrawWeights has run. Flat is one search
// over the prebuilt sums. Paged draws twice: a page proportional to
// its stored weight sum, then an entry within the loaded page by a
// linear weighted walk over its live metas (at most hashFencePageMax
// of them, recomputed per draw because the loaded page changes under
// the draws). The stored page weight can trail the live metas by the
// skipped advisory writes, which skews the draw no further than the
// fill classes already do. The returned index is into r.fence, which
// holds the drawn page's entries until the next page load.
func (h *Hash) drawFenceIdx(ctx context.Context, total uint64) (int, error) {
	r := &h.segRoot
	x := h.rand64() % total
	j := sort.Search(len(h.wsum), func(i int) bool { return h.wsum[i] > x })
	if !r.paged {
		return j, nil
	}
	if err := h.loadPage(ctx, j); err != nil {
		return 0, err
	}
	pt := uint64(0)
	for _, e := range r.fence {
		pt += 2*uint64(e.meta&hashMetaFillMask) + 1
	}
	y := h.rand64() % pt
	acc := uint64(0)
	for i, e := range r.fence {
		acc += 2*uint64(e.meta&hashMetaFillMask) + 1
		if acc > y {
			return i, nil
		}
	}
	return len(r.fence) - 1, nil
}

// hashEntryAt walks region to entry idx. The caller has bounded idx
// by the entry count, so running out is a corruption error.
func hashEntryAt(region []byte, idx int, valless bool) (f, v []byte, err error) {
	it := hashEntryIter{p: region, valless: valless}
	for k := 0; ; k++ {
		f, v, _, ok, err := it.next()
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("sqlo1: hash entry index %d beyond region end", idx)
		}
		if k == idx {
			return f, v, nil
		}
	}
}

// drawSegEntry is the weighted primitive: one uniform-ish entry of
// the segmented hash under the current root. Empty segments (weight 1
// until the lazy merge folds them) redraw; a run of empty draws falls
// back to the first non-empty segment in fence order, page by page on
// a paged root. The fallback terminates because a segmented root's
// count is positive. Returns the fence index and entry index
// alongside the bytes so distinct sampling can key its dedupe; on
// return r.fence still holds the page containing fi, so the caller
// can read r.fence[fi].segid before the next draw. The bytes alias
// the segment read and die at the next Tiered call.
func (h *Hash) drawSegEntry(ctx context.Context, total uint64) (fi, ei int, f, v []byte, err error) {
	r := &h.segRoot
	for range 32 {
		i, err := h.drawFenceIdx(ctx, total)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		seg, err := h.readSeg(ctx, r.fence[i].segid)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		if seg.n == 0 {
			continue
		}
		j := int(h.rand64() % uint64(seg.n))
		f, v, err := hashEntryAt(seg.entries, j, seg.valless)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return i, j, f, v, nil
	}
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := range pages {
		if err := h.loadPage(ctx, p); err != nil {
			return 0, 0, nil, nil, err
		}
		for i := range r.fence {
			seg, err := h.readSeg(ctx, r.fence[i].segid)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			if seg.n == 0 {
				continue
			}
			j := int(h.rand64() % uint64(seg.n))
			f, v, err := hashEntryAt(seg.entries, j, seg.valless)
			if err != nil {
				return 0, 0, nil, nil, err
			}
			return i, j, f, v, nil
		}
	}
	return 0, 0, nil, nil, fmt.Errorf("sqlo1: segmented hash rooth %#x claims %d fields, all segments empty", r.rooth, r.count)
}

// HRandField is the no-count form: one uniform-ish draw, ok false on
// an absent key. The returned bytes alias internal buffers until the
// next call on this layer, the HGet rule. Draws read through
// stateOfLive (a due root reaps first), so a dead entry can never be
// drawn and the count the draws trust is exact live.
func (h *Hash) HRandField(ctx context.Context, key []byte) (field, val []byte, ok bool, err error) {
	st, hi, _, err := h.stateOfLive(ctx, key)
	if err != nil {
		return nil, nil, false, err
	}
	switch st {
	case hashAbsent:
		return nil, nil, false, nil
	case hashInlineState:
		idx := int(h.rand64() % uint64(hi.count))
		f, v, err := hashEntryAt(hi.entries, idx, h.valless)
		if err != nil {
			return nil, nil, false, err
		}
		return f, v, true, nil
	}
	total := h.buildDrawWeights()
	_, _, f, v, err := h.drawSegEntry(ctx, total)
	if err != nil {
		return nil, nil, false, err
	}
	return f, v, true, nil
}

// HRandFieldCount is the count form. count is the magnitude of the
// wire argument and withReplacement its sign: Redis's negative count
// draws count times with replacement, positive count returns
// min(count, HLEN) distinct fields. begin runs exactly once, before
// any emit, with the exact number of entries that will be emitted, so
// a RESP writer can put the array header down first. Emitted bytes
// are only valid inside the emit call. Like HRandField, the read goes
// through stateOfLive, so the begin(n) arithmetic and every draw path
// below run over live entries only.
func (h *Hash) HRandFieldCount(ctx context.Context, key []byte, count int64, withReplacement bool, begin func(n int64), emit func(field, val []byte)) error {
	st, hi, _, err := h.stateOfLive(ctx, key)
	if err != nil {
		return err
	}
	if st == hashAbsent || count <= 0 {
		begin(0)
		return nil
	}
	if st == hashInlineState {
		return h.randInline(hi, count, withReplacement, begin, emit)
	}
	if withReplacement {
		return h.randSegReplace(ctx, count, begin, emit)
	}
	return h.randSegDistinct(ctx, count, begin, emit)
}

// randInline serves both count forms off the single root read: no
// Tiered calls happen inside, so the region aliases stay live for the
// whole pass.
func (h *Hash) randInline(hi hashInline, count int64, withReplacement bool, begin func(n int64), emit func(field, val []byte)) error {
	n := hi.count
	if withReplacement {
		begin(count)
		for range count {
			f, v, err := hashEntryAt(hi.entries, int(h.rand64()%uint64(n)), h.valless)
			if err != nil {
				return err
			}
			emit(f, v)
		}
		return nil
	}
	if count >= int64(n) {
		begin(int64(n))
		it := hashEntryIter{p: hi.entries, valless: h.valless}
		for {
			f, v, _, ok, err := it.next()
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			emit(f, v)
		}
	}
	// A partial Fisher-Yates over the entry indexes picks the distinct
	// subset, then one region walk emits it in entry order. n is at
	// most hashInlineMaxCount, so the arrays sit on the stack.
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
	it := hashEntryIter{p: hi.entries, valless: h.valless}
	for k := 0; ; k++ {
		f, v, _, ok, err := it.next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if sel[k] {
			emit(f, v)
		}
	}
}

// randSegReplace draws count times with replacement over the weighted
// primitive; each draw is one segment read and the emit consumes the
// aliases before the next one.
func (h *Hash) randSegReplace(ctx context.Context, count int64, begin func(n int64), emit func(field, val []byte)) error {
	total := h.buildDrawWeights()
	begin(count)
	for range count {
		_, _, f, v, err := h.drawSegEntry(ctx, total)
		if err != nil {
			return err
		}
		emit(f, v)
	}
	return nil
}

// randSegDistinct climbs the distinct ladder. count >= HLEN emits
// everything, the HGETALL walk. count within a third of HLEN runs a
// reservoir over the same walk, because rejection would churn near
// the end; Redis draws the equivalent line at SUB_STRATEGY_MUL = 3.
// Below that, rejection sampling over the weighted primitive emits
// accepted draws immediately, with a dedupe keyed by fence position
// and a valve that finishes the quota in fence order if the draws
// stop landing (pathological weight skew, not an expected path).
func (h *Hash) randSegDistinct(ctx context.Context, count int64, begin func(n int64), emit func(field, val []byte)) error {
	r := &h.segRoot
	hlen := int64(r.count)
	if count >= hlen {
		begin(hlen)
		emitted, err := h.iterSegEntries(ctx, emit)
		if err != nil {
			return err
		}
		if emitted != r.count {
			return fmt.Errorf("sqlo1: segmented hash root claims %d fields, segments hold %d", r.count, emitted)
		}
		return nil
	}
	if count*3 >= hlen {
		return h.randSegReservoir(ctx, count, begin, emit)
	}

	total := h.buildDrawWeights()
	if h.picked == nil {
		h.picked = make(map[uint64]struct{}, count)
	}
	clear(h.picked)
	begin(count)
	remaining := count
	// Entry indexes stay under 4096 (a 4032-byte segment holds at
	// most 576 entries), so segid*4096+idx is collision-free and fits:
	// segids cap at 2^48.
	for tries := 20*count + 100; remaining > 0 && tries > 0; tries-- {
		fi, ei, f, v, err := h.drawSegEntry(ctx, total)
		if err != nil {
			return err
		}
		k := r.fence[fi].segid*4096 + uint64(ei)
		if _, dup := h.picked[k]; dup {
			continue
		}
		h.picked[k] = struct{}{}
		emit(f, v)
		remaining--
	}
	if remaining == 0 {
		return nil
	}
	remaining, err := h.fenceFill(ctx, remaining, emit)
	if err != nil {
		return err
	}
	if remaining > 0 {
		return fmt.Errorf("sqlo1: segmented hash root claims %d fields, distinct sample ran dry %d short", r.count, remaining)
	}
	return nil
}

// fenceFill is the rejection sampler's valve: it finishes a distinct
// quota deterministically, emitting the first entries in fence order
// whose dedupe key is not already in picked, page by page on a paged
// root. Returns what it could not fill, which is zero unless the root
// count lied.
func (h *Hash) fenceFill(ctx context.Context, remaining int64, emit func(field, val []byte)) (int64, error) {
	r := &h.segRoot
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := 0; p < pages && remaining > 0; p++ {
		if err := h.loadPage(ctx, p); err != nil {
			return remaining, err
		}
		for i := 0; i < len(r.fence) && remaining > 0; i++ {
			seg, err := h.readSeg(ctx, r.fence[i].segid)
			if err != nil {
				return remaining, err
			}
			it := hashEntryIter{p: seg.entries, valless: seg.valless}
			for j := 0; remaining > 0; j++ {
				f, v, _, ok, err := it.next()
				if err != nil {
					return remaining, err
				}
				if !ok {
					break
				}
				k := r.fence[i].segid*4096 + uint64(j)
				if _, dup := h.picked[k]; dup {
					continue
				}
				h.picked[k] = struct{}{}
				emit(f, v)
				remaining--
			}
		}
	}
	return remaining, nil
}

// randSegReservoir is Algorithm R over the full walk, with entry
// copies in the reservoir arena because the walk's aliases die round
// by round. Expected copies stay under count*(1+ln 3), so the arena
// is bounded by the sample, not the hash.
func (h *Hash) randSegReservoir(ctx context.Context, count int64, begin func(n int64), emit func(field, val []byte)) error {
	h.rvSlots = h.rvSlots[:0]
	h.rvArena = h.rvArena[:0]
	seen := uint64(0)
	_, err := h.iterSegEntries(ctx, func(f, v []byte) {
		seen++
		if int64(len(h.rvSlots)) < count {
			h.rvSlots = append(h.rvSlots, h.rvPut(f, v))
			return
		}
		if j := h.rand64() % seen; int64(j) < count {
			h.rvSlots[j] = h.rvPut(f, v)
		}
	})
	if err != nil {
		return err
	}
	begin(int64(len(h.rvSlots)))
	for _, s := range h.rvSlots {
		f, v := h.rvGet(s)
		emit(f, v)
	}
	return nil
}
