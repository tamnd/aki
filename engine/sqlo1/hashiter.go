package sqlo1

// The hash iteration surface, doc 06 section 3: HGETALL, HKEYS, and
// HVALS stream every segment in fence order with cold segments read
// in IO batches ahead of the RESP writer, and HSCAN walks the same
// order behind an fh cursor. Full iteration is the one operator
// family that is O(segments) by definition of its output size.

import (
	"context"
	"fmt"
)

// hashIterBatchSegs is the segment prefetch width of a full
// iteration: how many segments one LookupBatch round pulls before the
// emits drain them. 16 segments bound the round at about 64 KiB while
// amortizing the cold index path across the batch.
const hashIterBatchSegs = 16

// HIterate streams every field of key in segment order (inline
// hashes: insertion order). begin runs exactly once, before any emit,
// with the exact field count, so a RESP writer can put the array
// header down and stream the rest. Emitted bytes alias the current IO
// round and die at the next Tiered call, so emit must consume them
// before returning. An absent key is begin(0); another type is
// ErrWrongType.
func (h *Hash) HIterate(ctx context.Context, key []byte, begin func(count int), emit func(field, val []byte)) error {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return err
	}
	switch st {
	case hashAbsent:
		begin(0)
		return nil
	case hashInlineState:
		begin(hi.count)
		it := hashEntryIter{p: hi.entries}
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

	// Segmented: the root count is exact under rule W1, so the header
	// goes down before the first segment read.
	r := &h.segRoot
	begin(int(r.count))
	emitted, err := h.iterSegEntries(ctx, emit)
	if err != nil {
		return err
	}
	if emitted != r.count {
		return fmt.Errorf("sqlo1: segmented hash root claims %d fields, segments hold %d", r.count, emitted)
	}
	return nil
}

// iterSegEntries streams every entry of the current segRoot in fence
// order, hashIterBatchSegs per LookupBatch round; each round is
// fetched only after the previous round's entries were emitted, which
// keeps the aliases legal and the RAM bound at one round. A paged
// root walks its pages in index order, one loaded at a time, so the
// RAM bound gains one page, not the whole fence. The caller has run
// stateOf to hashSegState. Returns the entries emitted.
func (h *Hash) iterSegEntries(ctx context.Context, emit func(field, val []byte)) (uint64, error) {
	r := &h.segRoot
	emitted := uint64(0)
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := range pages {
		if err := h.loadPage(ctx, p); err != nil {
			return emitted, err
		}
		for base := 0; base < len(r.fence); base += hashIterBatchSegs {
			n := min(hashIterBatchSegs, len(r.fence)-base)
			h.mgKeyBuf = grow(h.mgKeyBuf, n*SubkeySize)
			h.mgKeys = h.mgKeys[:0]
			for j := range n {
				k := h.mgKeyBuf[j*SubkeySize : (j+1)*SubkeySize]
				putHashSegKey(k, r.rooth, r.fence[base+j].segid)
				h.mgKeys = append(h.mgKeys, k)
			}
			var err error
			h.mgVals, h.mgRoots, h.mgExps, err = h.t.LookupBatch(ctx, h.mgKeys, h.mgVals, h.mgRoots, h.mgExps)
			if err != nil {
				return emitted, err
			}
			for j := range n {
				if h.mgVals[j] == nil {
					return emitted, fmt.Errorf("sqlo1: hash segment %d of rooth %#x is missing", r.fence[base+j].segid, r.rooth)
				}
				seg, err := decodeHashSeg(h.mgVals[j])
				if err != nil {
					return emitted, err
				}
				it := hashEntryIter{p: seg.entries}
				for {
					f, v, _, ok, err := it.next()
					if err != nil {
						return emitted, err
					}
					if !ok {
						break
					}
					emit(f, v)
					emitted++
				}
			}
		}
	}
	return emitted, nil
}

// HScan is one cursor step: it emits fields from cursor upward in fh
// order and returns the next cursor, zero when the scan is complete.
// The cursor is the last returned fh plus one, so it survives splits
// and merges (an entry's fh never changes and range order is
// preserved), which is what upholds the Redis contract that a field
// present for the whole scan is returned at least once. count is the
// scanned-elements hint; the step always finishes the segment it is
// in, so the cut lands between segments and the resume point cannot
// bisect a run of equal fh values. Inline hashes answer any cursor
// with the whole set and a zero next cursor, the listpack behavior.
// Emitted bytes die at the next Tiered call, like HIterate's.
func (h *Hash) HScan(ctx context.Context, key []byte, cursor uint64, count int64, emit func(field, val []byte)) (uint64, error) {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	switch st {
	case hashAbsent:
		return 0, nil
	case hashInlineState:
		it := hashEntryIter{p: hi.entries}
		for {
			f, v, _, ok, err := it.next()
			if err != nil {
				return 0, err
			}
			if !ok {
				return 0, nil
			}
			emit(f, v)
		}
	}

	r := &h.segRoot
	scanned := int64(0)
	var lastFH uint64
	p, pages := 0, 1
	if r.paged {
		p = hashPageFind(r.pidx, cursor)
		pages = len(r.pidx)
	}
	for ; p < pages; p++ {
		if err := h.loadPage(ctx, p); err != nil {
			return 0, err
		}
		// Only the cursor's own page can start mid-page; later pages'
		// entries all sit above the cursor, where hashFenceFind says
		// -1 and the walk starts at their first entry.
		for i := max(hashFenceFind(r.fence, cursor), 0); i < len(r.fence); i++ {
			if scanned >= count {
				// lastFH sits below this fence entry's lo, so the resume
				// cursor cannot wrap and cannot skip anything: the next
				// step lands back on segment i at its first entry.
				return lastFH + 1, nil
			}
			seg, err := h.readSeg(ctx, r.fence[i].segid)
			if err != nil {
				return 0, err
			}
			it := hashEntryIter{p: seg.entries}
			for {
				f, v, _, ok, err := it.next()
				if err != nil {
					return 0, err
				}
				if !ok {
					break
				}
				fh := hashFH(f)
				if fh < cursor {
					continue
				}
				emit(f, v)
				scanned++
				lastFH = fh
			}
		}
	}
	return 0, nil
}
