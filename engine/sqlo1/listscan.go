package sqlo1

// The list scan surface, doc 07 slice 6: LINSERT, LREM, and LPOS, the
// middle ops that walk the list by value instead of by position. All
// three walk directionally and exit early, so a match near the scanned
// end touches a handful of nodes no matter how long the list is. LREM
// wields the lmid half-merge counterweight: nodes it shrinks coalesce
// with their walk neighbors while the combined payload fits
// listMergeMax, which is what bounds fence growth under the decimation
// adversary.

import (
	"bytes"
	"context"
	"math"
	"slices"
)

// Pos scans for elem and emits the absolute index of each match, LPOS:
// a positive rank scans head to tail skipping rank-1 matches first, a
// negative rank mirrors that from the tail (indexes still count from
// the head), num caps the emits, and maxlen caps the comparisons from
// the scan's starting end (0 is unlimited). The caller guarantees rank
// is nonzero. A missing key emits nothing.
func (l *List) Pos(ctx context.Context, key, elem []byte, rank, num, maxlen int64, emit func(idx int64)) error {
	st, li, _, err := l.stateOf(ctx, key)
	if err != nil || st == listAbsent {
		return err
	}
	reverse := rank < 0
	skip := rank - 1
	if reverse {
		skip = -rank - 1
	}
	if maxlen == 0 {
		maxlen = math.MaxInt64
	}
	emitted, compared := int64(0), int64(0)
	// step compares one element at absolute index i and reports whether
	// the walk goes on.
	step := func(e []byte, i int64) bool {
		if compared >= maxlen {
			return false
		}
		compared++
		if !bytes.Equal(e, elem) {
			return true
		}
		if skip > 0 {
			skip--
			return true
		}
		emit(i)
		emitted++
		return emitted < num
	}

	if st == listInlineState {
		l.spans = l.spans[:0]
		it := listElemIter{p: li.elems}
		for {
			e, ok := it.next()
			if !ok {
				break
			}
			l.spans = append(l.spans, e)
		}
		if !reverse {
			for i, e := range l.spans {
				if !step(e, int64(i)) {
					return nil
				}
			}
			return nil
		}
		for i := len(l.spans) - 1; i >= 0; i-- {
			if !step(l.spans[i], int64(i)) {
				return nil
			}
		}
		return nil
	}

	// Noded: nodes read one at a time in walk order, each scanned to
	// completion before the next read recycles its view; the early exit
	// is what makes a near-end match O(nodes touched), not O(list). A
	// paged fence walks its pages in the same order one level up, and
	// the early exit keeps far pages unread too.
	r := &l.nodeRoot
	npages := 1
	if r.paged {
		npages = len(r.pidx)
	}
	if !reverse {
		base := int64(0)
		for pj := range npages {
			if r.paged {
				if err := l.loadPage(ctx, pj); err != nil {
					return err
				}
			}
			for fi := range l.fence {
				node, err := l.readNode(ctx, l.fence[fi].segid)
				if err != nil {
					return err
				}
				it := listElemIter{p: node.elems}
				for j := 0; ; j++ {
					e, ok := it.next()
					if !ok {
						break
					}
					if !step(e, base+int64(j)) {
						return nil
					}
				}
				base += int64(l.fence[fi].count)
			}
		}
		return nil
	}
	base := int64(r.count)
	for pj := npages - 1; pj >= 0; pj-- {
		if r.paged {
			if err := l.loadPage(ctx, pj); err != nil {
				return err
			}
		}
		for fi := len(l.fence) - 1; fi >= 0; fi-- {
			base -= int64(l.fence[fi].count)
			node, err := l.readNode(ctx, l.fence[fi].segid)
			if err != nil {
				return err
			}
			l.spans = l.spans[:0]
			it := listElemIter{p: node.elems}
			for {
				e, ok := it.next()
				if !ok {
					break
				}
				l.spans = append(l.spans, e)
			}
			for j := len(l.spans) - 1; j >= 0; j-- {
				if !step(l.spans[j], base+int64(j)) {
					return nil
				}
			}
		}
	}
	return nil
}

// Insert places elem before or after the first occurrence of pivot,
// LINSERT: the new length, -1 when the pivot is not in the list, 0 on
// a missing key. The head-to-tail pivot scan exits at the first match,
// the touched node amends in place, and only a node grown past the cut
// thresholds splits at the element boundary nearest half its bytes. A
// split the fence cannot take is refused side-effect free, and an
// inline root grown past its caps takes the push upgrade.
func (l *List) Insert(ctx context.Context, key []byte, before bool, pivot, elem []byte) (int64, error) {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	if st == listAbsent {
		return 0, nil
	}

	if st == listInlineState {
		at := -1
		it := listElemIter{p: li.elems}
		for len(it.p) > 0 {
			a := len(li.elems) - len(it.p)
			e, _ := it.next()
			if bytes.Equal(e, pivot) {
				if before {
					at = a
				} else {
					at = len(li.elems) - len(it.p)
				}
				break
			}
		}
		if at < 0 {
			return -1, nil
		}
		count := li.count + 1
		l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
		l.rootBuf = append(l.rootBuf, li.elems[:at]...)
		l.rootBuf = appendListElem(l.rootBuf, elem)
		l.rootBuf = append(l.rootBuf, li.elems[at:]...)
		if count > listInlineMaxCount || len(l.rootBuf) > listInlineMax {
			return l.upgrade(ctx, key, l.rootBuf[listInlineHdrLen:], count, expMs)
		}
		putListInlineHdr(l.rootBuf, count)
		if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
			return 0, err
		}
		return int64(count), l.restamp(ctx, key, expMs)
	}

	// Noded: scan for the pivot's node page by page, then build the
	// amended payload out of the read before the next Tiered call
	// recycles it.
	r := &l.nodeRoot
	npages := 1
	if r.paged {
		npages = len(r.pidx)
	}
	ni, oldN, pj := -1, 0, 0
scan:
	for p := range npages {
		if r.paged {
			if err := l.loadPage(ctx, p); err != nil {
				return 0, err
			}
		}
		for fi := range l.fence {
			node, err := l.readNode(ctx, l.fence[fi].segid)
			if err != nil {
				return 0, err
			}
			at := -1
			it := listElemIter{p: node.elems}
			for len(it.p) > 0 {
				a := len(node.elems) - len(it.p)
				e, _ := it.next()
				if bytes.Equal(e, pivot) {
					if before {
						at = a
					} else {
						at = len(node.elems) - len(it.p)
					}
					break
				}
			}
			if at < 0 {
				continue
			}
			l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
			l.nodeBuf = append(l.nodeBuf, node.elems[:at]...)
			l.nodeBuf = appendListElem(l.nodeBuf, elem)
			l.nodeBuf = append(l.nodeBuf, node.elems[at:]...)
			ni, oldN, pj = fi, node.n, p
			break scan
		}
	}
	if ni < 0 {
		return -1, nil
	}

	newN := oldN + 1
	if newN <= listNodeMaxElems && len(l.nodeBuf) <= listNodeMax {
		putListNodeHdr(l.nodeBuf, newN)
		if err := l.writeNode(ctx, l.fence[ni].segid, l.nodeBuf); err != nil {
			return 0, err
		}
		l.fence[ni].count++
		if r.paged {
			if err := l.writeFencePage(ctx); err != nil {
				return 0, err
			}
		} else {
			r.fence = l.fence
		}
		r.count++
		if err := l.writeNodeRoot(ctx, key); err != nil {
			return 0, err
		}
		return int64(r.count), l.restamp(ctx, key, expMs)
	}

	// The grown node splits at the element boundary nearest half its
	// bytes; both halves are nonempty because the node holds at least
	// two elements past any cut threshold. A fence with no room for the
	// second entry transitions to pages or splits its page instead of
	// refusing; only a full page index refuses, before anything writes.
	pageSplit, transition := false, false
	if r.paged {
		if len(l.fence)+1 > listFencePageMax {
			if len(r.pidx)+1 > listFencePageIdxMax {
				return 0, errListFenceThirdLevel
			}
			pageSplit = true
		}
	} else if len(l.fence)+1 > listFenceMaxNodes {
		if pageChunks(len(l.fence)+1) > listFencePageIdxMax {
			return 0, errListFenceThirdLevel
		}
		transition = true
	}
	region := l.nodeBuf[listNodeHdrLen:]
	half := len(region) / 2
	cut, prev, firstN := 0, 0, 0
	it := listElemIter{p: region}
	for cut < half {
		e, _ := it.next()
		prev = cut
		cut += listElemHdrLen + len(e)
		firstN++
	}
	if firstN == newN {
		// One oversize tail element swallowed the walk; it gets the
		// second node to itself.
		cut, firstN = prev, firstN-1
	}
	l.segBuf = grow(l.segBuf, listNodeHdrLen)
	l.segBuf = append(l.segBuf, region[cut:]...)
	putListNodeHdr(l.segBuf, newN-firstN)
	newSegid := r.nextSegid
	if err := l.writeNode(ctx, newSegid, l.segBuf); err != nil {
		return 0, err
	}
	r.nextSegid++
	oldSegid := l.fence[ni].segid
	l.nodeBuf = l.nodeBuf[:listNodeHdrLen+cut]
	putListNodeHdr(l.nodeBuf, firstN)
	l.fence2 = append(l.fence2[:0], l.fence[:ni+1]...)
	l.fence2[ni].count = uint32(firstN)
	l.fence2 = append(l.fence2, listFenceEnt{segid: newSegid, count: uint32(newN - firstN)})
	l.fence2 = append(l.fence2, l.fence[ni+1:]...)

	switch {
	case transition:
		// The fresh split node and the whole paged fence land and flush
		// first, orphans until a root references them; the trimmed
		// node, an in-place rewrite the old root still reads, stays
		// behind the barrier in the root's own batch.
		if err := l.pageFence(ctx, l.fence2, false); err != nil {
			return 0, err
		}
		if err := l.t.Flush(ctx); err != nil {
			return 0, err
		}
	case pageSplit:
		// The full page cuts in half: the high half is a fresh page
		// behind the flush, the low half rewrites the loaded page in
		// place beside the root, and the index takes the new entry
		// after the page write so l.pi still names the loaded slot.
		h := len(l.fence2) / 2
		highEnt, err := l.writeFreshPage(ctx, l.fence2[h:])
		if err != nil {
			return 0, err
		}
		if err := l.t.Flush(ctx); err != nil {
			return 0, err
		}
		l.fence = append(l.fence[:0], l.fence2[:h]...)
		if err := l.writeFencePage(ctx); err != nil {
			return 0, err
		}
		r.pidx = slices.Insert(r.pidx, pj+1, highEnt)
		l.pidxBuf = r.pidx
	case r.paged:
		l.fence = append(l.fence[:0], l.fence2...)
		if err := l.writeFencePage(ctx); err != nil {
			return 0, err
		}
	default:
		l.fence, l.fence2 = l.fence2, l.fence
		r.fence = l.fence
	}
	if err := l.writeNode(ctx, oldSegid, l.nodeBuf); err != nil {
		return 0, err
	}
	r.count++
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return 0, err
	}
	return int64(r.count), l.restamp(ctx, key, expMs)
}

// Rem removes matches of elem under LREM's count grammar: positive
// removes up to count head to tail, negative up to -count tail to
// head, zero removes all. Removing the last element deletes the key.
// On the noded tier the walk exits as soon as the budget is spent, and
// shrunk nodes coalesce with their walk neighbors while the combined
// payload fits listMergeMax, the lmid counterweight; a walk that
// removes nothing writes nothing.
func (l *List) Rem(ctx context.Context, key []byte, count int64, elem []byte) (int64, error) {
	st, li, expMs, err := l.stateOf(ctx, key)
	if err != nil || st == listAbsent {
		return 0, err
	}
	budget := int64(math.MaxInt64)
	reverse := false
	if count > 0 {
		budget = count
	} else if count < 0 {
		budget, reverse = -count, true
	}

	if st == listInlineState {
		l.spans = l.spans[:0]
		it := listElemIter{p: li.elems}
		for {
			e, ok := it.next()
			if !ok {
				break
			}
			l.spans = append(l.spans, e)
		}
		// Two passes: count the matches, then drop match ordinals in
		// [lo, hi), which expresses both directions without a marker
		// array.
		m := int64(0)
		for _, e := range l.spans {
			if bytes.Equal(e, elem) {
				m++
			}
		}
		k := min(budget, m)
		if k == 0 {
			return 0, nil
		}
		lo, hi := int64(0), k
		if reverse {
			lo, hi = m-k, m
		}
		l.rootBuf = grow(l.rootBuf, listInlineHdrLen)
		ord := int64(0)
		kept := 0
		for _, e := range l.spans {
			if bytes.Equal(e, elem) {
				o := ord
				ord++
				if o >= lo && o < hi {
					continue
				}
			}
			l.rootBuf = appendListElem(l.rootBuf, e)
			kept++
		}
		if kept == 0 {
			if _, err := l.t.Del(ctx, key); err != nil {
				return 0, err
			}
			return k, nil
		}
		putListInlineHdr(l.rootBuf, kept)
		if err := l.t.Set(ctx, key, l.rootBuf, TagList|TagRoot); err != nil {
			return 0, err
		}
		return k, l.restamp(ctx, key, expMs)
	}

	return l.remNoded(ctx, key, elem, budget, reverse, expMs)
}

// remNoded is the directional noded walk with the carry merge. The
// carry is the last surviving node the walk produced, its bytes staged
// in segBuf: the next survivor merges into it while the combined
// payload fits listMergeMax and the counts fit a node, one of the pair
// is dirty, and the pair is adjacent among survivors (emptied nodes
// between them are gone). Everything the walk drops is a delete, never
// a rewrite, and fence slots die in place (count 0) until the one
// compaction at the end. A paged fence walks page by page with the
// carry confined to its page: each touched page compacts and rewrites
// in place as the walk leaves it, a page emptied whole leaves a
// count-0 sentinel the index compaction drops, and the dead page
// records land after the root.
func (l *List) remNoded(ctx context.Context, key, elem []byte, budget int64, reverse bool, expMs int64) (int64, error) {
	r := &l.nodeRoot
	removed := int64(0)
	mutated := false
	npages := 1
	if r.paged {
		npages = len(r.pidx)
	}
	l.deadPages = l.deadPages[:0]

	for p := 0; p < npages && removed < budget; p++ {
		pj := p
		if reverse {
			pj = npages - 1 - p
		}
		if r.paged {
			if err := l.loadPage(ctx, pj); err != nil {
				return 0, err
			}
		}

		carry := false // segBuf holds the carry node's payload when set
		carryFi, carryN := 0, 0
		carryDirty := false
		flushCarry := func() error {
			if !carry {
				return nil
			}
			carry = false
			if !carryDirty {
				return nil
			}
			putListNodeHdr(l.segBuf, carryN)
			if err := l.writeNode(ctx, l.fence[carryFi].segid, l.segBuf); err != nil {
				return err
			}
			l.fence[carryFi].count = uint32(carryN)
			return nil
		}
		pageChanged := false

		n := len(l.fence)
		for i := 0; i < n && removed < budget; i++ {
			fi := i
			if reverse {
				fi = n - 1 - i
			}
			ent := l.fence[fi]
			node, err := l.readNode(ctx, ent.segid)
			if err != nil {
				return 0, err
			}

			// Filter the node into nodeBuf, copying the survivors out
			// of the read. The reverse direction drops its matches from
			// the node's tail, the inline ordinal trick at node scope.
			l.spans = l.spans[:0]
			it := listElemIter{p: node.elems}
			for {
				e, ok := it.next()
				if !ok {
					break
				}
				l.spans = append(l.spans, e)
			}
			m := int64(0)
			for _, e := range l.spans {
				if bytes.Equal(e, elem) {
					m++
				}
			}
			k := min(budget-removed, m)
			lo, hi := int64(0), k
			if reverse {
				lo, hi = m-k, m
			}
			l.nodeBuf = grow(l.nodeBuf, listNodeHdrLen)
			ord := int64(0)
			kept := 0
			for _, e := range l.spans {
				if bytes.Equal(e, elem) {
					o := ord
					ord++
					if o >= lo && o < hi {
						continue
					}
				}
				l.nodeBuf = appendListElem(l.nodeBuf, e)
				kept++
			}
			removed += k
			changed := k > 0
			if changed {
				mutated = true
				pageChanged = true
			}

			if kept == 0 {
				if err := l.delNode(ctx, ent.segid); err != nil {
					return 0, err
				}
				l.fence[fi].count = 0
				continue
			}

			curBytes := len(l.nodeBuf) - listNodeHdrLen
			carryBytes := len(l.segBuf) - listNodeHdrLen
			if carry && (carryDirty || changed) &&
				listNodeHdrLen+carryBytes+curBytes <= listMergeMax &&
				carryN+kept <= listNodeMaxElems {
				// Merge: the current node's record dies, its bytes join
				// the carry on the walk-facing side so list order holds.
				if !reverse {
					l.segBuf = append(l.segBuf, l.nodeBuf[listNodeHdrLen:]...)
				} else {
					l.nodeBuf = append(l.nodeBuf, l.segBuf[listNodeHdrLen:]...)
					l.segBuf, l.nodeBuf = l.nodeBuf, l.segBuf
				}
				carryN += kept
				carryDirty = true
				mutated = true
				pageChanged = true
				if err := l.delNode(ctx, ent.segid); err != nil {
					return 0, err
				}
				l.fence[fi].count = 0
				continue
			}

			if err := flushCarry(); err != nil {
				return 0, err
			}
			l.segBuf, l.nodeBuf = l.nodeBuf, l.segBuf
			carry = true
			carryFi, carryN, carryDirty = fi, kept, changed
		}
		if err := flushCarry(); err != nil {
			return 0, err
		}

		if r.paged && pageChanged {
			l.fence2 = l.fence2[:0]
			for _, e := range l.fence {
				if e.count > 0 {
					l.fence2 = append(l.fence2, e)
				}
			}
			if len(l.fence2) == 0 {
				l.deadPages = append(l.deadPages, r.pidx[pj].segid)
				r.pidx[pj].count = 0
				l.pi = -1
			} else {
				l.fence, l.fence2 = l.fence2, l.fence
				if err := l.writeFencePage(ctx); err != nil {
					return 0, err
				}
			}
		}
	}
	if !mutated {
		return 0, nil
	}
	if removed == int64(r.count) {
		// Every node emptied and its record already deleted; the dead
		// pages and the root are all that is left.
		for _, pid := range l.deadPages {
			if err := l.delPage(ctx, pid); err != nil {
				return 0, err
			}
		}
		if _, err := l.t.Del(ctx, key); err != nil {
			return 0, err
		}
		return removed, nil
	}

	if r.paged {
		kept := r.pidx[:0]
		for _, e := range r.pidx {
			if e.count > 0 {
				kept = append(kept, e)
			}
		}
		r.pidx = kept
		l.pi = -1
	} else {
		l.fence2 = l.fence2[:0]
		for _, e := range l.fence {
			if e.count > 0 {
				l.fence2 = append(l.fence2, e)
			}
		}
		l.fence, l.fence2 = l.fence2, l.fence
		r.fence = l.fence
	}
	r.count -= uint64(removed)
	if err := l.writeNodeRoot(ctx, key); err != nil {
		return 0, err
	}
	for _, pid := range l.deadPages {
		if err := l.delPage(ctx, pid); err != nil {
			return 0, err
		}
	}
	return removed, l.restamp(ctx, key, expMs)
}
