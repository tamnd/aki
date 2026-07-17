package sqlo1

// The set algebra STORE variants, doc 08 section 2: SUNIONSTORE,
// SINTERSTORE, SDIFFSTORE. The result streams in ascending (fh,
// member) order into a bottom-up bulk build, the doc 09 section 6
// pattern: segments cut at seg_max in fh order, the fence built in
// one pass (paged past the flat cap), and the root PUT last as the
// commit point. The intermediate state is invisible because nothing
// references the fresh rooth until the root lands; a crash mid-build
// leaves orphan segments the liveness sweep reclaims, and the old
// dest plane retires through a Bump riding the commit's batch.
//
// The union's fh order comes from a k-way merge over per-source
// cursors rather than a dedupe table: every segmented set already
// streams in (fh, member) order (segments are internally sorted and
// the fence partitions by fh), so merging the sources with a byte
// tiebreak makes identical members adjacent and the dedupe is one
// comparison against the previous emit. That is exact where a digest
// is probabilistic, and its RAM is one IO round per source, which is
// what doc 08's SE-I3 asks of the family; the plain SUNION rides the
// same merge.

import (
	"bytes"
	"context"
	"fmt"
	"sort"
)

// algCursor streams one loaded source's members in ascending (fh,
// member) order with owned bytes: an inline source is sorted once at
// init, a segmented source refills from its fence one IO round at a
// time, copying the round's members out before the next Tiered call
// can recycle it.
type algCursor struct {
	sc    *algSrc
	arena []byte
	mem   [][]byte
	fh    []uint64
	pos   int
	page  int
	base  int
	done  bool
}

func (c *algCursor) init(ctx context.Context, sc *algSrc) error {
	c.sc = sc
	c.mem, c.fh = c.mem[:0], c.fh[:0]
	c.pos, c.page, c.base = 0, 0, 0
	c.done = false
	switch sc.st {
	case hashAbsent:
		c.done = true
		return nil
	case hashInlineState:
		c.done = true
		for _, m := range sc.inMembers {
			c.mem = append(c.mem, m)
			c.fh = append(c.fh, hashFH(m))
		}
		sort.Sort(&winSorter{mem: c.mem, fh: c.fh})
		return nil
	}
	return c.refill(ctx)
}

// refill loads the next IO round of segments into the cursor's own
// arena. The fence walk mirrors algWalk's driver: pages in order,
// algBatchSegs segments per round.
func (c *algCursor) refill(ctx context.Context) error {
	c.pos, c.mem, c.fh = 0, c.mem[:0], c.fh[:0]
	h := c.sc.h
	r := &h.segRoot
	for {
		pages := 1
		if r.paged {
			pages = len(r.pidx)
		}
		if c.page >= pages {
			c.done = true
			return nil
		}
		if err := h.loadPage(ctx, c.page); err != nil {
			return err
		}
		if c.base >= len(r.fence) {
			c.page++
			c.base = 0
			continue
		}
		n := min(algBatchSegs, len(r.fence)-c.base)
		h.mgKeyBuf = grow(h.mgKeyBuf, n*SubkeySize)
		h.mgKeys = h.mgKeys[:0]
		for j := range n {
			k := h.mgKeyBuf[j*SubkeySize : (j+1)*SubkeySize]
			putHashSegKey(k, r.rooth, r.fence[c.base+j].segid)
			h.mgKeys = append(h.mgKeys, k)
		}
		var err error
		h.mgVals, h.mgRoots, h.mgExps, err = h.t.LookupBatch(ctx, h.mgKeys, h.mgVals, h.mgRoots, h.mgExps)
		if err != nil {
			return err
		}
		need := 0
		for j := range n {
			if h.mgVals[j] == nil {
				return fmt.Errorf("sqlo1: set segment %d of rooth %#x is missing", r.fence[c.base+j].segid, r.rooth)
			}
			need += len(h.mgVals[j])
		}
		c.arena = grow(c.arena, need)[:0]
		for j := range n {
			seg, err := decodeHashSeg(h.mgVals[j], encSet)
			if err != nil {
				return err
			}
			it := hashEntryIter{p: seg.entries, enc: encSet}
			for {
				m, _, _, ok, err := it.next()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				off := len(c.arena)
				c.arena = append(c.arena, m...)
				c.mem = append(c.mem, c.arena[off:len(c.arena)])
				c.fh = append(c.fh, hashFH(m))
			}
		}
		c.base += n
		return nil
	}
}

// head returns the cursor's current member without consuming it.
func (c *algCursor) head() ([]byte, uint64, bool) {
	if c.pos >= len(c.mem) {
		return nil, 0, false
	}
	return c.mem[c.pos], c.fh[c.pos], true
}

func (c *algCursor) advance(ctx context.Context) error {
	c.pos++
	if c.pos >= len(c.mem) && !c.done {
		return c.refill(ctx)
	}
	return nil
}

// mergeUnion streams the union of the loaded sources in ascending
// (fh, member) order, each member once. Sources ascend individually,
// so the smallest head across cursors is the global next; equal
// members are adjacent by the byte tiebreak and one owned copy of the
// previous emit kills them. Emitted bytes alias a cursor arena and
// die when emit returns.
func (s *Set) mergeUnion(ctx context.Context, srcs []algSrc, emit func(m []byte, fh uint64) error) error {
	for len(s.curs) < len(srcs) {
		s.curs = append(s.curs, algCursor{})
	}
	curs := s.curs[:len(srcs)]
	for i := range srcs {
		if err := curs[i].init(ctx, &srcs[i]); err != nil {
			return err
		}
	}
	prevSet := false
	var prevFH uint64
	for {
		best := -1
		var bm []byte
		var bf uint64
		for i := range curs {
			m, f, ok := curs[i].head()
			if !ok {
				continue
			}
			if best < 0 || f < bf || (f == bf && bytes.Compare(m, bm) < 0) {
				best, bm, bf = i, m, f
			}
		}
		if best < 0 {
			return nil
		}
		if !prevSet || prevFH != bf || !bytes.Equal(s.prevEmit, bm) {
			if err := emit(bm, bf); err != nil {
				return err
			}
			s.prevEmit = append(s.prevEmit[:0], bm...)
			prevFH, prevSet = bf, true
		}
		if err := curs[best].advance(ctx); err != nil {
			return err
		}
	}
}

// setBuilder is the streaming bulk build: members arrive in ascending
// fh order (equal fh adjacent), accumulate inline until the inline
// thresholds break, then pack into segments cut at seg_max, never
// between equal fh values. Segments land as they cut, invisible under
// the fresh rooth until the commit.
type setBuilder struct {
	h         *Hash
	inline    bool
	count     int64
	allInt    bool
	rootBuf   []byte
	pend      []hashSegEntry
	pendArena []byte
	pendSize  int
	lastFH    uint64
	fence     []hashFenceEnt
	segBuf    []byte
}

// beginBuild resets the builder onto the set's own ladder, whose
// segRoot is free during a STORE (all sources ride aux ladders) and
// whose lease mints the destination's rooth.
func (s *Set) beginBuild() *setBuilder {
	b := &s.bld
	b.h = s.h
	b.inline = true
	b.count = 0
	b.allInt = true
	b.rootBuf = grow(b.rootBuf, hashInlineHdrLen)[:hashInlineHdrLen]
	b.pend = b.pend[:0]
	b.pendArena = b.pendArena[:0]
	b.pendSize = hashSegHdrLen
	b.fence = b.fence[:0]
	return b
}

func (b *setBuilder) add(ctx context.Context, m []byte, fh uint64) error {
	es := hashEntrySize(len(m), 0, 0, encSet)
	if b.inline {
		if b.count < hashInlineMaxCount && len(b.rootBuf)+es <= hashInlineMax {
			b.rootBuf = appendHashEntry(b.rootBuf, m, nil, 0, encSet)
			b.allInt = b.allInt && isCanonicalInt(m)
			b.count++
			return nil
		}
		// The thresholds broke: mint the plane and reparse the inline
		// region as the first pending entries (they alias rootBuf,
		// which nothing touches again until the builder resets).
		rooth, err := b.h.nextRooth(ctx)
		if err != nil {
			return err
		}
		b.h.segRoot = hashSegRoot{sub: b.h.subSeg, rootgen: 1, rooth: rooth, pi: -1}
		b.pend, err = parseHashSegEntries(b.pend[:0], b.rootBuf[hashInlineHdrLen:], encSet)
		if err != nil {
			return err
		}
		b.pendSize = hashSegHdrLen
		for _, e := range b.pend {
			b.pendSize += hashEntrySize(len(e.field), 0, 0, encSet)
		}
		if n := len(b.pend); n > 0 {
			b.lastFH = b.pend[n-1].fh
		}
		b.inline = false
	}
	if len(b.pend) > 0 && b.pendSize+es > hashSegMax && fh != b.lastFH {
		if err := b.cut(ctx); err != nil {
			return err
		}
	}
	off := len(b.pendArena)
	b.pendArena = append(b.pendArena, m...)
	b.pend = append(b.pend, hashSegEntry{fh: fh, field: b.pendArena[off:len(b.pendArena)]})
	b.pendSize += es
	b.lastFH = fh
	b.count++
	return nil
}

// cut packs the pending entries into one segment and lands it. The
// per-segment sort is the (fh, member) invariant segment scans lean
// on; input order already satisfies it except for equal-fh members
// out of an unsorted inline driver, which the sort repairs.
func (b *setBuilder) cut(ctx context.Context) error {
	sortHashSegEntries(b.pend)
	b.segBuf = appendHashSegPayload(b.segBuf, b.pend, encSet)
	r := &b.h.segRoot
	segid := r.nextSegid
	if err := b.h.writeSeg(ctx, segid, b.segBuf); err != nil {
		return err
	}
	lo := uint64(0)
	if len(b.fence) > 0 {
		lo = b.pend[0].fh
	}
	b.fence = append(b.fence, hashFenceEnt{
		lo:    lo,
		segid: segid,
		meta:  hashSegMeta(len(b.pend), 0),
	})
	r.nextSegid++
	if len(b.fence) > hashFencePageIdxMax*hashFencePageMax {
		return errHashFenceThirdLevel
	}
	b.pend = b.pend[:0]
	b.pendArena = b.pendArena[:0]
	b.pendSize = hashSegHdrLen
	return nil
}

// destPrep reads the destination's current representation right
// before the commit write: it registers the plane bump a planed root
// of any type needs (the bump rides the commit's drain batch, the
// same-batch contract), and reports existence and the current expiry
// so the caller can drop it, because a STORE destination is a fresh
// object and Redis clears the TTL. On Hash because every STORE
// family (set algebra, ZRANGESTORE, the zset algebra to come) lands
// through the same door.
func (h *Hash) destPrep(ctx context.Context, dest []byte) (exists bool, expMs int64, err error) {
	v, root, expMs, ok, err := h.t.LookupEntry(ctx, dest)
	if err != nil || !ok {
		return false, 0, err
	}
	if !root {
		return true, expMs, nil
	}
	_, planeless, err := sniffRoot(v)
	if err != nil {
		return false, 0, err
	}
	if !planeless {
		rooth, rootgen, err := planedRootInfo(v)
		if err != nil {
			return false, 0, err
		}
		h.t.Bump(dest, rooth, rootgen+1)
	}
	return true, expMs, nil
}

// storeEmpty is the empty-result door: the destination is deleted
// whatever it held, Redis's rule for every STORE variant.
func (h *Hash) storeEmpty(ctx context.Context, dest []byte) (int64, error) {
	exists, _, err := h.destPrep(ctx, dest)
	if err != nil || !exists {
		return 0, err
	}
	_, err = h.t.Del(ctx, dest)
	return 0, err
}

// commitFence installs a one-pass-built member fence into the seg
// root, paging it when it outgrew the flat cap: full pages except the
// last, the index entry carrying each page's first lo (page 0 covers
// from 0 like a flat fence's first entry). Shared by every bulk
// build.
func (h *Hash) commitFence(ctx context.Context, fence []hashFenceEnt) error {
	r := &h.segRoot
	if len(fence) > hashFenceMaxSegs {
		h.pidx = h.pidx[:0]
		for base := 0; base < len(fence); base += hashFencePageMax {
			n := min(hashFencePageMax, len(fence)-base)
			page := fence[base : base+n]
			pageid := r.nextSegid
			r.nextSegid++
			h.pageBuf = appendHashFencePage(h.pageBuf[:0], page)
			putHashFenceKey(h.kbuf2[:], r.rooth, pageid)
			if err := h.t.SetGen(ctx, h.kbuf2[:], h.pageBuf, h.tag|TagFence, r.rootgen); err != nil {
				return err
			}
			h.pidx = append(h.pidx, hashPageEnt{
				lo:     page[0].lo,
				pageid: pageid,
				weight: hashPageWeight(page),
			})
		}
		if len(h.pidx) > hashFencePageIdxMax {
			return errHashFenceThirdLevel
		}
		r.paged = true
		r.pidx = h.pidx
		r.pi = -1
		r.fence = nil
		return nil
	}
	r.paged = false
	r.pidx = nil
	r.fence = fence
	return nil
}

// storeCommit finishes the build and lands it on dest. Inline results
// are one root write; segmented results cut the last segment, page
// the fence when it outgrew the flat cap, flush so every segment and
// page sits below the root that references them, and land the root as
// the commit point.
func (s *Set) storeCommit(ctx context.Context, dest []byte, b *setBuilder) (int64, error) {
	if b.count == 0 {
		return s.h.storeEmpty(ctx, dest)
	}
	if b.inline {
		putHashInlineHdr(b.rootBuf, s.h.subInline, int(b.count), 0, b.allInt)
		exists, expMs, err := s.h.destPrep(ctx, dest)
		if err != nil {
			return 0, err
		}
		if err := s.h.t.Set(ctx, dest, b.rootBuf, s.h.tag|TagRoot); err != nil {
			return 0, err
		}
		return b.count, s.h.clearDestExp(ctx, dest, exists, expMs)
	}
	if len(b.pend) > 0 {
		if err := b.cut(ctx); err != nil {
			return 0, err
		}
	}
	if err := s.h.commitFence(ctx, b.fence); err != nil {
		return 0, err
	}
	s.h.segRoot.count = uint64(b.count)
	if err := s.h.t.Flush(ctx); err != nil {
		return 0, err
	}
	exists, expMs, err := s.h.destPrep(ctx, dest)
	if err != nil {
		return 0, err
	}
	if err := s.h.writeSegRoot(ctx, dest, false); err != nil {
		return 0, err
	}
	return b.count, s.h.clearDestExp(ctx, dest, exists, expMs)
}

// clearDestExp drops the expiry an overwritten destination carried; a
// hot header would otherwise preserve it across the commit write.
func (h *Hash) clearDestExp(ctx context.Context, dest []byte, exists bool, expMs int64) error {
	if !exists || expMs == 0 {
		return nil
	}
	_, err := h.t.ExpireAt(ctx, dest, 0)
	return err
}

// SInterStore computes SINTER of keys into dest and returns the
// result's cardinality. The doors are SINTER's on the sources (the
// first absent key short-circuits, masking later wrong types) plus
// the STORE rules on dest: any current value is overwritten whatever
// its type, an empty result deletes dest, and the old TTL is dropped.
func (s *Set) SInterStore(ctx context.Context, dest []byte, keys [][]byte) (int64, error) {
	srcs, absent, err := s.loadSrcs(ctx, keys, true)
	if err != nil {
		return 0, err
	}
	if absent {
		return s.h.storeEmpty(ctx, dest)
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
	b := s.beginBuild()
	add := func(m []byte, fh uint64) error { return b.add(ctx, m, fh) }
	if _, err := s.algWalk(ctx, &srcs[d], s.rest, true, 0, add); err != nil {
		return 0, err
	}
	return s.storeCommit(ctx, dest, b)
}

// SUnionStore computes SUNION of keys into dest via the k-way merge,
// which is already the build's fh order.
func (s *Set) SUnionStore(ctx context.Context, dest []byte, keys [][]byte) (int64, error) {
	srcs, _, err := s.loadSrcs(ctx, keys, false)
	if err != nil {
		return 0, err
	}
	b := s.beginBuild()
	if err := s.mergeUnion(ctx, srcs, func(m []byte, fh uint64) error {
		return b.add(ctx, m, fh)
	}); err != nil {
		return 0, err
	}
	return s.storeCommit(ctx, dest, b)
}

// SDiffStore computes SDIFF of keys into dest, driving from the first
// set whatever its size, SDIFF's rule.
func (s *Set) SDiffStore(ctx context.Context, dest []byte, keys [][]byte) (int64, error) {
	srcs, _, err := s.loadSrcs(ctx, keys, false)
	if err != nil {
		return 0, err
	}
	s.rest = s.rest[:0]
	for i := 1; i < len(srcs); i++ {
		s.rest = append(s.rest, &srcs[i])
	}
	b := s.beginBuild()
	add := func(m []byte, fh uint64) error { return b.add(ctx, m, fh) }
	if _, err := s.algWalk(ctx, &srcs[0], s.rest, false, 0, add); err != nil {
		return 0, err
	}
	return s.storeCommit(ctx, dest, b)
}
