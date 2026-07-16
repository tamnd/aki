package sqlo1

// The set algebra family, doc 08 section 2: SINTER, SINTERCARD,
// SUNION, SDIFF. All four are read-only multi-key streams, and the
// whole file leans on one property: hashFH is the same function for
// every set, so a driver walked in fence order produces members in
// globally ascending fh order, and probing them into any other set is
// a monotone co-walk of that set's fence. Pages load in order, the
// touched segments come out grouped and ascending, and the reads pack
// into LookupBatch rounds.
//
// The salgebra lab priced two arms: batched probes (touched segments
// only, rounds fragmented per one-segment gather window) and the full
// fh-order merge walk (everything read once, packed rounds), crossing
// at 4 driver members per target fence entry. The walk here takes a
// whole IO round of driver segments as its gather window, which packs
// the probe rounds the way the merge walk packs its own (the lab's
// window sweep shows fragmentation falling as the window widens), and
// it still reads only touched segments, which the merge walk cannot.
// A segment no window member routes to cannot change any member's
// verdict, so the wider window dominates both lab arms and no runtime
// switch is needed; the lab's crossover was pricing the one-segment
// window it swept.

import (
	"bytes"
	"context"
	"fmt"
	"sort"
)

// algBatchSegs is the segment fetch width of the algebra walks,
// hashIterBatchSegs for the same reason: 16 segments bound a round
// near 64 KiB. It is both the driver's gather window and the probe
// fetch width.
const algBatchSegs = hashIterBatchSegs

// algSrc is one loaded algebra source: the aux ladder holding the
// key's decoded root (root state is per-Hash, and a multi-key op
// needs every source's root at once), the classified state, the O(1)
// count, and for inline sets a copy of the members, because the
// inline view aliases the root read and the walk does Tiered calls
// between probes.
type algSrc struct {
	h         *Hash
	st        hashState
	count     int64
	inMembers [][]byte
	inArena   []byte
}

// algHash returns the i-th aux ladder, growing the pool on demand.
// Aux ladders ride s.h's Tiered and config with the set stamps, so
// they are the same machinery pointed at a different key.
func (s *Set) algHash(i int) (*Hash, error) {
	for len(s.alg) <= i {
		h, err := newSegLadder(s.h.t, s.h.cfg)
		if err != nil {
			return nil, err
		}
		h.tag, h.subSeg, h.subInline = TagSet, setSubSeg, setSubInline
		h.enc = encSet
		s.alg = append(s.alg, h)
	}
	return s.alg[i], nil
}

// loadSrcs classifies every key in order onto its own ladder. The
// order carries Redis's doors: a wrong type errors at its position,
// and with stopOnAbsent (SINTER's rule) the first absent key returns
// early without looking at later keys, so a wrong type behind it
// stays masked exactly as Redis masks it. Inline members are copied
// out because their view dies at the next Tiered call and the walk
// makes many.
func (s *Set) loadSrcs(ctx context.Context, keys [][]byte, stopOnAbsent bool) ([]algSrc, bool, error) {
	for len(s.srcs) < len(keys) {
		s.srcs = append(s.srcs, algSrc{})
	}
	srcs := s.srcs[:len(keys)]
	for i, key := range keys {
		h, err := s.algHash(i)
		if err != nil {
			return nil, false, err
		}
		st, hi, _, err := h.stateOf(ctx, key)
		if err != nil {
			return nil, false, err
		}
		if st == hashAbsent && stopOnAbsent {
			return nil, true, nil
		}
		sc := &srcs[i]
		sc.h, sc.st, sc.count = h, st, 0
		sc.inMembers = sc.inMembers[:0]
		switch st {
		case hashInlineState:
			sc.count = int64(hi.count)
			// Pre-size the arena so appends cannot move the member
			// aliases already handed out; the entry region bounds the
			// member bytes.
			sc.inArena = grow(sc.inArena, len(hi.entries))[:0]
			it := hashEntryIter{p: hi.entries, enc: encSet}
			for {
				m, _, _, ok, err := it.next()
				if err != nil {
					return nil, false, err
				}
				if !ok {
					break
				}
				off := len(sc.inArena)
				sc.inArena = append(sc.inArena, m...)
				sc.inMembers = append(sc.inMembers, sc.inArena[off:len(sc.inArena)])
			}
		case hashSegState:
			sc.count = int64(h.segRoot.count)
		}
	}
	return srcs, false, nil
}

// SInter streams the intersection of the sets at keys, driving from
// the smallest set (root counts are O(1)) and probing its members
// into the rest. Redis's doors: any absent key is an empty result,
// returned at the first absent key before later keys are looked at;
// a wrong type before that errors. Emitted bytes live in the window
// arena and die when emit returns. One key streams the set itself.
func (s *Set) SInter(ctx context.Context, keys [][]byte, emit func(member []byte)) error {
	srcs, absent, err := s.loadSrcs(ctx, keys, true)
	if err != nil || absent {
		return err
	}
	d := 0
	for i := 1; i < len(srcs); i++ {
		if srcs[i].count < srcs[d].count {
			d = i
		}
	}
	s.rest = s.rest[:0]
	for i := range srcs {
		if i != d {
			s.rest = append(s.rest, &srcs[i])
		}
	}
	_, err = s.algWalk(ctx, &srcs[d], s.rest, true, 0, func(m []byte, _ uint64) error {
		emit(m)
		return nil
	})
	return err
}

// SInterCard is SINTER's cardinality form: the same driver choice and
// doors, no emission, and limit > 0 stops the driver walk as soon as
// that many members survived (limit 0 is unlimited, Redis's LIMIT 0).
func (s *Set) SInterCard(ctx context.Context, keys [][]byte, limit int64) (int64, error) {
	srcs, absent, err := s.loadSrcs(ctx, keys, true)
	if err != nil || absent {
		return 0, err
	}
	d := 0
	for i := 1; i < len(srcs); i++ {
		if srcs[i].count < srcs[d].count {
			d = i
		}
	}
	s.rest = s.rest[:0]
	for i := range srcs {
		if i != d {
			s.rest = append(s.rest, &srcs[i])
		}
	}
	return s.algWalk(ctx, &srcs[d], s.rest, true, limit, nil)
}

// SDiff streams the members of the first set that are in none of the
// rest, Redis's SDIFF semantics: the first set is the driver whatever
// its size, absent keys anywhere act as empty sets, and a wrong type
// anywhere errors. Emitted bytes die when emit returns.
func (s *Set) SDiff(ctx context.Context, keys [][]byte, emit func(member []byte)) error {
	srcs, _, err := s.loadSrcs(ctx, keys, false)
	if err != nil {
		return err
	}
	s.rest = s.rest[:0]
	for i := 1; i < len(srcs); i++ {
		s.rest = append(s.rest, &srcs[i])
	}
	_, err = s.algWalk(ctx, &srcs[0], s.rest, false, 0, func(m []byte, _ uint64) error {
		emit(m)
		return nil
	})
	return err
}

// SUnion streams the union of the sets at keys, each member once, in
// ascending (fh, member) order out of the k-way merge in setstore.go.
// Every source already ascends individually (segments are internally
// sorted, the fence partitions by fh, inline sets sort at cursor
// init), so equal members surface adjacent and the dedupe is one
// comparison against the previous emit: exact where the earlier
// digest table was probabilistic, and bounded at one IO round per
// source where the table grew with the result. Union order is
// unspecified for Redis sets, so the order change is free. Emitted
// bytes alias a cursor arena and die when emit returns.
func (s *Set) SUnion(ctx context.Context, keys [][]byte, emit func(member []byte)) error {
	srcs, _, err := s.loadSrcs(ctx, keys, false)
	if err != nil {
		return err
	}
	return s.mergeUnion(ctx, srcs, func(m []byte, _ uint64) error {
		emit(m)
		return nil
	})
}

// algWalk drives SINTER, SINTERCARD, and SDIFF: the driver's members
// stream in fh order one IO round at a time, each round's members are
// copied into the window arena (the filters' own reads would recycle
// the round they alias), the window is filtered through every rest
// set, and the survivors emit. keepHits true keeps members every rest
// set holds, false keeps members no rest set holds; the two folds are
// the intersection and the difference. limit > 0 stops the walk at
// that many survivors and is the return value's cap; emit may be nil,
// gets each survivor with its fh (the STORE builder wants both), and
// an emit error aborts the walk.
func (s *Set) algWalk(ctx context.Context, d *algSrc, rest []*algSrc, keepHits bool, limit int64, emit func(member []byte, fh uint64) error) (int64, error) {
	emitted := int64(0)
	// flush filters the current window and emits the survivors,
	// reporting whether the limit was reached.
	flush := func() (bool, error) {
		for i := range rest {
			if len(s.winMem) == 0 {
				break
			}
			if err := s.filterWindow(ctx, rest[i], keepHits); err != nil {
				return false, err
			}
		}
		for k, m := range s.winMem {
			if limit > 0 && emitted >= limit {
				return true, nil
			}
			if emit != nil {
				if err := emit(m, s.winFH[k]); err != nil {
					return false, err
				}
			}
			emitted++
		}
		return limit > 0 && emitted >= limit, nil
	}
	switch d.st {
	case hashAbsent:
		return 0, nil
	case hashInlineState:
		// The one inline window; members are already stable copies,
		// but inline entries sit in insertion order and the routing
		// co-walk needs ascending fh.
		s.winMem = append(s.winMem[:0], d.inMembers...)
		s.winFH = s.winFH[:0]
		for _, m := range s.winMem {
			s.winFH = append(s.winFH, hashFH(m))
		}
		sort.Sort(&winSorter{mem: s.winMem, fh: s.winFH})
		_, err := flush()
		return emitted, err
	}

	h := d.h
	r := &h.segRoot
	pages := 1
	if r.paged {
		pages = len(r.pidx)
	}
	for p := range pages {
		if err := h.loadPage(ctx, p); err != nil {
			return emitted, err
		}
		for base := 0; base < len(r.fence); base += algBatchSegs {
			n := min(algBatchSegs, len(r.fence)-base)
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
			// Copy the round's members out before the filters recycle
			// it; sizing the arena first keeps the aliases in place.
			need := 0
			for j := range n {
				if h.mgVals[j] == nil {
					return emitted, fmt.Errorf("sqlo1: set segment %d of rooth %#x is missing", r.fence[base+j].segid, r.rooth)
				}
				need += len(h.mgVals[j])
			}
			s.winArena = grow(s.winArena, need)[:0]
			s.winMem = s.winMem[:0]
			s.winFH = s.winFH[:0]
			for j := range n {
				seg, err := decodeHashSeg(h.mgVals[j], encSet)
				if err != nil {
					return emitted, err
				}
				it := hashEntryIter{p: seg.entries, enc: encSet}
				for {
					m, _, _, ok, err := it.next()
					if err != nil {
						return emitted, err
					}
					if !ok {
						break
					}
					off := len(s.winArena)
					s.winArena = append(s.winArena, m...)
					s.winMem = append(s.winMem, s.winArena[off:len(s.winArena)])
					s.winFH = append(s.winFH, hashFH(m))
				}
			}
			done, err := flush()
			if err != nil {
				return emitted, err
			}
			if done {
				return emitted, nil
			}
		}
	}
	return emitted, nil
}

// filterWindow probes every window member against src and compacts
// the window in place to the hits (keepHits) or the misses. Window
// members ascend by fh, so the routing walks src's fence forward
// (pages load in order, at most once per window per rest set) and the
// touched segments come out as ascending groups, fetched in packed
// rounds of algBatchSegs and probed with the shared segment search.
func (s *Set) filterWindow(ctx context.Context, src *algSrc, keepHits bool) error {
	switch src.st {
	case hashAbsent:
		if keepHits {
			s.winMem = s.winMem[:0]
			s.winFH = s.winFH[:0]
		}
		return nil
	case hashInlineState:
		w := 0
		for k, m := range s.winMem {
			hit := false
			for _, im := range src.inMembers {
				if bytes.Equal(im, m) {
					hit = true
					break
				}
			}
			if hit == keepHits {
				s.winMem[w], s.winFH[w] = m, s.winFH[k]
				w++
			}
		}
		s.winMem, s.winFH = s.winMem[:w], s.winFH[:w]
		return nil
	}

	// Route the whole window first: routing can load fence pages,
	// which are Tiered calls, so they all happen before the segment
	// rounds whose views the probes hold. Groups are runs of members
	// sharing a segment; grpStart gets a sentinel so group g spans
	// [grpStart[g], grpStart[g+1]).
	h := src.h
	r := &h.segRoot
	s.grpSeg = s.grpSeg[:0]
	s.grpStart = s.grpStart[:0]
	idx := -1
	for k, fh := range s.winFH {
		if idx >= 0 {
			for idx+1 < len(r.fence) && fh >= r.fence[idx+1].lo {
				idx++
			}
			// Pinned at the loaded page's last entry with fh owned by
			// a later page: re-resolve through the page index.
			if r.paged && idx == len(r.fence)-1 && r.pi < len(r.pidx)-1 && fh >= r.pidx[r.pi+1].lo {
				idx = -1
			}
		}
		if idx < 0 {
			j, err := h.fenceIdx(ctx, fh)
			if err != nil {
				return err
			}
			idx = j
		}
		segid := r.fence[idx].segid
		if len(s.grpSeg) == 0 || s.grpSeg[len(s.grpSeg)-1] != segid {
			s.grpSeg = append(s.grpSeg, segid)
			s.grpStart = append(s.grpStart, k)
		}
	}
	s.grpStart = append(s.grpStart, len(s.winFH))

	if cap(s.hitBuf) < len(s.winMem) {
		s.hitBuf = make([]bool, len(s.winMem))
	}
	s.hitBuf = s.hitBuf[:len(s.winMem)]
	for i := range s.hitBuf {
		s.hitBuf[i] = false
	}
	for base := 0; base < len(s.grpSeg); base += algBatchSegs {
		n := min(algBatchSegs, len(s.grpSeg)-base)
		h.mgKeyBuf = grow(h.mgKeyBuf, n*SubkeySize)
		h.mgKeys = h.mgKeys[:0]
		for j := range n {
			kb := h.mgKeyBuf[j*SubkeySize : (j+1)*SubkeySize]
			putHashSegKey(kb, r.rooth, s.grpSeg[base+j])
			h.mgKeys = append(h.mgKeys, kb)
		}
		var err error
		h.mgVals, h.mgRoots, h.mgExps, err = h.t.LookupBatch(ctx, h.mgKeys, h.mgVals, h.mgRoots, h.mgExps)
		if err != nil {
			return err
		}
		for j := range n {
			if h.mgVals[j] == nil {
				return fmt.Errorf("sqlo1: set segment %d of rooth %#x is missing", s.grpSeg[base+j], r.rooth)
			}
			seg, err := decodeHashSeg(h.mgVals[j], encSet)
			if err != nil {
				return err
			}
			for k := s.grpStart[base+j]; k < s.grpStart[base+j+1]; k++ {
				_, _, ok, err := hashSegGet(seg, s.winFH[k], s.winMem[k])
				if err != nil {
					return err
				}
				s.hitBuf[k] = ok
			}
		}
	}
	w := 0
	for k := range s.winMem {
		if s.hitBuf[k] == keepHits {
			s.winMem[w], s.winFH[w] = s.winMem[k], s.winFH[k]
			w++
		}
	}
	s.winMem, s.winFH = s.winMem[:w], s.winFH[:w]
	return nil
}

// winSorter orders a window's members and fhs together by (fh,
// member). The routing only groups by segment, so it needs the fh
// order alone; the byte tiebreak is for the union cursors and the
// STORE builder, whose merge and segment packing want the full
// segment-internal order.
type winSorter struct {
	mem [][]byte
	fh  []uint64
}

func (w *winSorter) Len() int { return len(w.fh) }
func (w *winSorter) Less(i, j int) bool {
	if w.fh[i] != w.fh[j] {
		return w.fh[i] < w.fh[j]
	}
	return bytes.Compare(w.mem[i], w.mem[j]) < 0
}
func (w *winSorter) Swap(i, j int) {
	w.mem[i], w.mem[j] = w.mem[j], w.mem[i]
	w.fh[i], w.fh[j] = w.fh[j], w.fh[i]
}
