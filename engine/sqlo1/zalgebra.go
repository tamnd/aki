package sqlo1

// The zset algebra family, doc 09 section 4: ZUNION, ZINTER, ZDIFF,
// their STORE forms, and ZINTERCARD. Everything streams the member
// family, never the score runs: a member segment already holds the
// 8-byte sortable score next to each member, so the co-walks that the
// set algebra proved (doc 08, the salgebra lab) carry the scores for
// free, and one (score, member) sort of the finished result serves
// the reply order and the bulk build alike. Reading score runs
// instead would double the IO and still not line up equal members
// across sources, which is what aggregation needs.
//
// Sources may be zsets or plain sets (a set member scores 1), Redis's
// rule for the whole family. Redis 8.8 folds aggregation in ascending
// cardinality order for union and inter both, not argument order,
// which is observable when infinities meet the NaN-to-0 clamps; the
// walks here fold the same way, smallest source first.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// zaggMode is the AGGREGATE option: how a member's weighted scores
// combine across sources.
type zaggMode uint8

const (
	zaggSum zaggMode = iota
	zaggMin
	zaggMax
)

// zsortableOne is the sortable image of 1.0, the score every plain-set
// member contributes.
var zsortableOne = zScoreSortable(1)

// zweighted is one source's contribution for a member: the decoded
// score times the source weight, with Redis's NaN clamp (0 times an
// infinity contributes 0).
func zweighted(sv uint64, w float64) float64 {
	v := zScoreFromSortable(sv) * w
	if math.IsNaN(v) {
		return 0
	}
	return v
}

// zaggApply folds one more weighted contribution into acc. SUM keeps
// Redis's NaN clamp: an infinity meeting its negation lands at 0.
func zaggApply(acc, v float64, agg zaggMode) float64 {
	switch agg {
	case zaggMin:
		if v < acc {
			return v
		}
		return acc
	case zaggMax:
		if v > acc {
			return v
		}
		return acc
	}
	acc += v
	if math.IsNaN(acc) {
		return 0
	}
	return acc
}

// zalgSrc is one loaded algebra source: the aux ladder holding the
// key's decoded root, the classified state, the O(1) count, the
// WEIGHTS multiplier, whether the key is a plain set, and for inline
// sources a copy of the members with their sortable scores, because
// the inline view aliases the root read and the walks do Tiered calls
// between probes.
type zalgSrc struct {
	h       *Hash
	st      hashState
	count   int64
	weight  float64
	set     bool
	inMem   [][]byte
	inSc    []uint64
	inArena []byte
}

// sval is the sortable score a probe hit carries: the fixed 1.0 for a
// plain set, the stored 8-byte image for a zset.
func (sc *zalgSrc) sval(val []byte) uint64 {
	if sc.set {
		return zsortableOne
	}
	return binary.BigEndian.Uint64(val)
}

// zalgHash returns the i-th aux ladder, growing the pool on demand.
// The ladders are minted unstamped: zloadSrcs stamps each one per
// source, because a source may be a zset or a plain set.
func (z *ZSet) zalgHash(i int) (*Hash, error) {
	for len(z.zalg) <= i {
		h, err := newSegLadder(z.h.t, z.h.cfg)
		if err != nil {
			return nil, err
		}
		z.zalg = append(z.zalg, h)
	}
	return z.zalg[i], nil
}

// zloadSrcs classifies every key in order onto its own aux ladder. A
// key holding neither a zset nor a set errors WRONGTYPE at its
// position, and absent keys never mask that (Redis type-checks the
// whole source list up front, unlike the set family's SINTER door).
// weights is nil for the unweighted forms.
func (z *ZSet) zloadSrcs(ctx context.Context, keys [][]byte, weights []float64) ([]zalgSrc, error) {
	for len(z.zasrcs) < len(keys) {
		z.zasrcs = append(z.zasrcs, zalgSrc{})
	}
	srcs := z.zasrcs[:len(keys)]
	for i, key := range keys {
		h, err := z.zalgHash(i)
		if err != nil {
			return nil, err
		}
		sc := &srcs[i]
		sc.h, sc.st, sc.count, sc.set = h, hashAbsent, 0, false
		sc.weight = 1
		if weights != nil {
			sc.weight = weights[i]
		}
		sc.inMem, sc.inSc = sc.inMem[:0], sc.inSc[:0]
		v, root, _, ok, err := h.t.LookupEntry(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if !root {
			return nil, ErrWrongType
		}
		tag, _, err := sniffRoot(v)
		if err != nil {
			return nil, err
		}
		switch tag {
		case TagZset:
			h.tag, h.subSeg, h.subInline, h.enc = TagZset, zsetSubSeg, zsetSubInline, encZMem
		case TagSet:
			h.tag, h.subSeg, h.subInline, h.enc = TagSet, setSubSeg, setSubInline, encSet
			sc.set = true
		default:
			return nil, ErrWrongType
		}
		st, hi, _, err := h.stateOf(ctx, key)
		if err != nil {
			return nil, err
		}
		sc.st = st
		switch st {
		case hashInlineState:
			sc.count = int64(hi.count)
			// Pre-size the arena so appends cannot move the member
			// aliases already handed out.
			sc.inArena = grow(sc.inArena, len(hi.entries))[:0]
			it := hashEntryIter{p: hi.entries, enc: h.enc}
			for {
				m, val, _, ok, err := it.next()
				if err != nil {
					return nil, err
				}
				if !ok {
					break
				}
				off := len(sc.inArena)
				sc.inArena = append(sc.inArena, m...)
				sc.inMem = append(sc.inMem, sc.inArena[off:len(sc.inArena)])
				sc.inSc = append(sc.inSc, sc.sval(val))
			}
		case hashSegState:
			sc.count = int64(h.segRoot.count)
		}
	}
	return srcs, nil
}

// zorderByCount fills z.zaorder with the sources in ascending
// cardinality order, ties in argument order: the fold order Redis's
// aggregation is observed to use for union and inter both.
func (z *ZSet) zorderByCount(srcs []zalgSrc) []*zalgSrc {
	z.zaorder = z.zaorder[:0]
	for i := range srcs {
		z.zaorder = append(z.zaorder, &srcs[i])
	}
	sort.SliceStable(z.zaorder, func(i, j int) bool {
		return z.zaorder[i].count < z.zaorder[j].count
	})
	return z.zaorder
}

// zwinSorter orders a window's members, fhs, and scores together by
// (fh, member), the co-walk routing order.
type zwinSorter struct {
	mem [][]byte
	fh  []uint64
	sc  []float64
}

func (w *zwinSorter) Len() int { return len(w.fh) }
func (w *zwinSorter) Less(i, j int) bool {
	if w.fh[i] != w.fh[j] {
		return w.fh[i] < w.fh[j]
	}
	return bytes.Compare(w.mem[i], w.mem[j]) < 0
}
func (w *zwinSorter) Swap(i, j int) {
	w.mem[i], w.mem[j] = w.mem[j], w.mem[i]
	w.fh[i], w.fh[j] = w.fh[j], w.fh[i]
	w.sc[i], w.sc[j] = w.sc[j], w.sc[i]
}

// zalgWalk drives ZINTER, ZINTERCARD, and ZDIFF: the driver's members
// stream in fh order one IO round at a time with their weighted
// scores, each window filters through every rest source, and the
// survivors emit with their aggregated score. keepHits true keeps
// members every rest source holds and aggregates their contributions
// (the intersection); false keeps members no rest source holds with
// the driver's score untouched (the difference). limit > 0 stops the
// walk at that many survivors; emit may be nil (ZINTERCARD), and
// needScores false skips the probe-side aggregation it would feed.
func (z *ZSet) zalgWalk(ctx context.Context, d *zalgSrc, rest []*zalgSrc, keepHits, needScores bool, agg zaggMode, limit int64, emit func(m []byte, sc float64) error) (int64, error) {
	emitted := int64(0)
	flush := func() (bool, error) {
		for i := range rest {
			if len(z.zwmem) == 0 {
				break
			}
			if err := z.zfilterWindow(ctx, rest[i], keepHits, needScores, agg); err != nil {
				return false, err
			}
		}
		for k, m := range z.zwmem {
			if limit > 0 && emitted >= limit {
				return true, nil
			}
			if emit != nil {
				if err := emit(m, z.zwsc[k]); err != nil {
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
		// The one inline window; the copies are stable, but inline
		// zsets sit in (score, member) order and the routing co-walk
		// needs ascending fh.
		z.zwmem = append(z.zwmem[:0], d.inMem...)
		z.zwfh, z.zwsc = z.zwfh[:0], z.zwsc[:0]
		for k, m := range z.zwmem {
			z.zwfh = append(z.zwfh, hashFH(m))
			z.zwsc = append(z.zwsc, zweighted(d.inSc[k], d.weight))
		}
		sort.Sort(&zwinSorter{mem: z.zwmem, fh: z.zwfh, sc: z.zwsc})
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
					return emitted, fmt.Errorf("sqlo1: algebra segment %d of rooth %#x is missing", r.fence[base+j].segid, r.rooth)
				}
				need += len(h.mgVals[j])
			}
			z.zwarena = grow(z.zwarena, need)[:0]
			z.zwmem, z.zwfh, z.zwsc = z.zwmem[:0], z.zwfh[:0], z.zwsc[:0]
			for j := range n {
				seg, err := decodeHashSeg(h.mgVals[j], h.enc)
				if err != nil {
					return emitted, err
				}
				it := hashEntryIter{p: seg.entries, enc: h.enc}
				for {
					m, val, _, ok, err := it.next()
					if err != nil {
						return emitted, err
					}
					if !ok {
						break
					}
					off := len(z.zwarena)
					z.zwarena = append(z.zwarena, m...)
					z.zwmem = append(z.zwmem, z.zwarena[off:len(z.zwarena)])
					z.zwfh = append(z.zwfh, hashFH(m))
					z.zwsc = append(z.zwsc, zweighted(d.sval(val), d.weight))
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

// zfilterWindow probes every window member against src and compacts
// the window in place to the hits (keepHits, aggregating each hit's
// weighted score) or the misses. Window members ascend by fh, so the
// routing walks src's fence forward and the touched segments come out
// as ascending groups, fetched in packed rounds.
func (z *ZSet) zfilterWindow(ctx context.Context, src *zalgSrc, keepHits, needScores bool, agg zaggMode) error {
	switch src.st {
	case hashAbsent:
		if keepHits {
			z.zwmem, z.zwfh, z.zwsc = z.zwmem[:0], z.zwfh[:0], z.zwsc[:0]
		}
		return nil
	case hashInlineState:
		w := 0
		for k, m := range z.zwmem {
			hit := false
			for j, im := range src.inMem {
				if bytes.Equal(im, m) {
					hit = true
					if keepHits && needScores {
						z.zwsc[k] = zaggApply(z.zwsc[k], zweighted(src.inSc[j], src.weight), agg)
					}
					break
				}
			}
			if hit == keepHits {
				z.zwmem[w], z.zwfh[w], z.zwsc[w] = m, z.zwfh[k], z.zwsc[k]
				w++
			}
		}
		z.zwmem, z.zwfh, z.zwsc = z.zwmem[:w], z.zwfh[:w], z.zwsc[:w]
		return nil
	}

	// Route the whole window first: routing can load fence pages,
	// which are Tiered calls, so they all happen before the segment
	// rounds whose views the probes hold.
	h := src.h
	r := &h.segRoot
	z.zagrpSeg = z.zagrpSeg[:0]
	z.zagrpStart = z.zagrpStart[:0]
	idx := -1
	for k, fh := range z.zwfh {
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
		if len(z.zagrpSeg) == 0 || z.zagrpSeg[len(z.zagrpSeg)-1] != segid {
			z.zagrpSeg = append(z.zagrpSeg, segid)
			z.zagrpStart = append(z.zagrpStart, k)
		}
	}
	z.zagrpStart = append(z.zagrpStart, len(z.zwfh))

	if cap(z.zahit) < len(z.zwmem) {
		z.zahit = make([]bool, len(z.zwmem))
	}
	z.zahit = z.zahit[:len(z.zwmem)]
	for i := range z.zahit {
		z.zahit[i] = false
	}
	for base := 0; base < len(z.zagrpSeg); base += algBatchSegs {
		n := min(algBatchSegs, len(z.zagrpSeg)-base)
		h.mgKeyBuf = grow(h.mgKeyBuf, n*SubkeySize)
		h.mgKeys = h.mgKeys[:0]
		for j := range n {
			kb := h.mgKeyBuf[j*SubkeySize : (j+1)*SubkeySize]
			putHashSegKey(kb, r.rooth, z.zagrpSeg[base+j])
			h.mgKeys = append(h.mgKeys, kb)
		}
		var err error
		h.mgVals, h.mgRoots, h.mgExps, err = h.t.LookupBatch(ctx, h.mgKeys, h.mgVals, h.mgRoots, h.mgExps)
		if err != nil {
			return err
		}
		for j := range n {
			if h.mgVals[j] == nil {
				return fmt.Errorf("sqlo1: algebra segment %d of rooth %#x is missing", z.zagrpSeg[base+j], r.rooth)
			}
			seg, err := decodeHashSeg(h.mgVals[j], h.enc)
			if err != nil {
				return err
			}
			for k := z.zagrpStart[base+j]; k < z.zagrpStart[base+j+1]; k++ {
				val, _, ok, err := hashSegGet(seg, z.zwfh[k], z.zwmem[k])
				if err != nil {
					return err
				}
				if ok {
					z.zahit[k] = true
					if keepHits && needScores {
						z.zwsc[k] = zaggApply(z.zwsc[k], zweighted(src.sval(val), src.weight), agg)
					}
				}
			}
		}
	}
	w := 0
	for k := range z.zwmem {
		if z.zahit[k] == keepHits {
			z.zwmem[w], z.zwfh[w], z.zwsc[w] = z.zwmem[k], z.zwfh[k], z.zwsc[k]
			w++
		}
	}
	z.zwmem, z.zwfh, z.zwsc = z.zwmem[:w], z.zwfh[:w], z.zwsc[:w]
	return nil
}

// zalgCursor streams one loaded source's members in ascending (fh,
// member) order with their raw sortable scores and owned bytes: an
// inline source is sorted once at init, a segmented source refills
// from its fence one IO round at a time.
type zalgCursor struct {
	src   *zalgSrc
	arena []byte
	mem   [][]byte
	fh    []uint64
	sv    []uint64
	pos   int
	page  int
	base  int
	done  bool
}

// zcurSorter orders a cursor's members, fhs, and sortable scores
// together by (fh, member).
type zcurSorter struct {
	mem [][]byte
	fh  []uint64
	sv  []uint64
}

func (w *zcurSorter) Len() int { return len(w.fh) }
func (w *zcurSorter) Less(i, j int) bool {
	if w.fh[i] != w.fh[j] {
		return w.fh[i] < w.fh[j]
	}
	return bytes.Compare(w.mem[i], w.mem[j]) < 0
}
func (w *zcurSorter) Swap(i, j int) {
	w.mem[i], w.mem[j] = w.mem[j], w.mem[i]
	w.fh[i], w.fh[j] = w.fh[j], w.fh[i]
	w.sv[i], w.sv[j] = w.sv[j], w.sv[i]
}

func (c *zalgCursor) init(ctx context.Context, src *zalgSrc) error {
	c.src = src
	c.mem, c.fh, c.sv = c.mem[:0], c.fh[:0], c.sv[:0]
	c.pos, c.page, c.base = 0, 0, 0
	c.done = false
	switch src.st {
	case hashAbsent:
		c.done = true
		return nil
	case hashInlineState:
		c.done = true
		for k, m := range src.inMem {
			c.mem = append(c.mem, m)
			c.fh = append(c.fh, hashFH(m))
			c.sv = append(c.sv, src.inSc[k])
		}
		sort.Sort(&zcurSorter{mem: c.mem, fh: c.fh, sv: c.sv})
		return nil
	}
	return c.refill(ctx)
}

// refill loads the next IO round of segments into the cursor's own
// arena.
func (c *zalgCursor) refill(ctx context.Context) error {
	c.pos, c.mem, c.fh, c.sv = 0, c.mem[:0], c.fh[:0], c.sv[:0]
	h := c.src.h
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
				return fmt.Errorf("sqlo1: algebra segment %d of rooth %#x is missing", r.fence[c.base+j].segid, r.rooth)
			}
			need += len(h.mgVals[j])
		}
		c.arena = grow(c.arena, need)[:0]
		for j := range n {
			seg, err := decodeHashSeg(h.mgVals[j], h.enc)
			if err != nil {
				return err
			}
			it := hashEntryIter{p: seg.entries, enc: h.enc}
			for {
				m, val, _, ok, err := it.next()
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
				c.sv = append(c.sv, c.src.sval(val))
			}
		}
		c.base += n
		return nil
	}
}

// head returns the cursor's current entry without consuming it.
func (c *zalgCursor) head() ([]byte, uint64, uint64, bool) {
	if c.pos >= len(c.mem) {
		return nil, 0, 0, false
	}
	return c.mem[c.pos], c.fh[c.pos], c.sv[c.pos], true
}

func (c *zalgCursor) advance(ctx context.Context) error {
	c.pos++
	if c.pos >= len(c.mem) && !c.done {
		return c.refill(ctx)
	}
	return nil
}

// mergeZUnion streams the union of the sources in ascending (fh,
// member) order and folds equal members as they surface: sources
// ascend individually, so a member's occurrences are adjacent in the
// merge, ties break to the lowest cursor index, and with srcs in
// ascending cardinality order the fold is Redis's. The finished
// pairs land in z.zapairs with members in z.zaarena, unsorted by
// score; the callers sort.
func (z *ZSet) mergeZUnion(ctx context.Context, srcs []*zalgSrc, agg zaggMode) error {
	for len(z.zacurs) < len(srcs) {
		z.zacurs = append(z.zacurs, zalgCursor{})
	}
	curs := z.zacurs[:len(srcs)]
	for i := range srcs {
		if err := curs[i].init(ctx, srcs[i]); err != nil {
			return err
		}
	}
	z.zaarena, z.zapairs = z.zaarena[:0], z.zapairs[:0]
	open := false
	var acc float64
	var curFH uint64
	var curOff, curEnd int
	finalize := func() {
		if open {
			z.zapairs = append(z.zapairs, zbuildPair{s: zScoreSortable(acc), off: curOff, end: curEnd})
		}
	}
	for {
		best := -1
		var bm []byte
		var bf, bsv uint64
		for i := range curs {
			m, f, sv, ok := curs[i].head()
			if !ok {
				continue
			}
			if best < 0 || f < bf || (f == bf && bytes.Compare(m, bm) < 0) {
				best, bm, bf, bsv = i, m, f, sv
			}
		}
		if best < 0 {
			finalize()
			return nil
		}
		v := zweighted(bsv, curs[best].src.weight)
		if open && curFH == bf && bytes.Equal(z.zaarena[curOff:curEnd], bm) {
			acc = zaggApply(acc, v, agg)
		} else {
			finalize()
			curOff = len(z.zaarena)
			z.zaarena = append(z.zaarena, bm...)
			curEnd = len(z.zaarena)
			curFH, acc, open = bf, v, true
		}
		if err := curs[best].advance(ctx); err != nil {
			return err
		}
	}
}

// zpairSorter orders result pairs by (score, member), the reply and
// build order; the sortable transform makes the score half a plain
// uint64 compare.
type zpairSorter struct {
	pairs []zbuildPair
	arena []byte
}

func (p *zpairSorter) Len() int { return len(p.pairs) }
func (p *zpairSorter) Less(i, j int) bool {
	a, b := p.pairs[i], p.pairs[j]
	if a.s != b.s {
		return a.s < b.s
	}
	return bytes.Compare(p.arena[a.off:a.end], p.arena[b.off:b.end]) < 0
}
func (p *zpairSorter) Swap(i, j int) {
	p.pairs[i], p.pairs[j] = p.pairs[j], p.pairs[i]
}

// zalgCollect is the shared result sink: the emitted member and final
// score append to z.zaarena and z.zapairs.
func (z *ZSet) zalgCollect(m []byte, sc float64) error {
	off := len(z.zaarena)
	z.zaarena = append(z.zaarena, m...)
	z.zapairs = append(z.zapairs, zbuildPair{s: zScoreSortable(sc), off: off, end: len(z.zaarena)})
	return nil
}

// ZUnion computes the union of the zsets or sets at keys with the
// given weights (nil is all 1) and aggregation, answering the result
// as (score, member)-sorted pairs over an arena; both alias the
// zset's scratch and die at the next algebra call.
func (z *ZSet) ZUnion(ctx context.Context, keys [][]byte, weights []float64, agg zaggMode) ([]zbuildPair, []byte, error) {
	srcs, err := z.zloadSrcs(ctx, keys, weights)
	if err != nil {
		return nil, nil, err
	}
	if err := z.mergeZUnion(ctx, z.zorderByCount(srcs), agg); err != nil {
		return nil, nil, err
	}
	sort.Sort(&zpairSorter{pairs: z.zapairs, arena: z.zaarena})
	return z.zapairs, z.zaarena, nil
}

// ZInter computes the intersection of the zsets or sets at keys,
// driving from the smallest source and probing its windows into the
// rest in ascending cardinality order, ZUnion's return contract.
func (z *ZSet) ZInter(ctx context.Context, keys [][]byte, weights []float64, agg zaggMode) ([]zbuildPair, []byte, error) {
	srcs, err := z.zloadSrcs(ctx, keys, weights)
	if err != nil {
		return nil, nil, err
	}
	order := z.zorderByCount(srcs)
	z.zaarena, z.zapairs = z.zaarena[:0], z.zapairs[:0]
	if order[0].count > 0 {
		if _, err := z.zalgWalk(ctx, order[0], order[1:], true, true, agg, 0, z.zalgCollect); err != nil {
			return nil, nil, err
		}
	}
	sort.Sort(&zpairSorter{pairs: z.zapairs, arena: z.zaarena})
	return z.zapairs, z.zaarena, nil
}

// ZInterCard answers the intersection's cardinality without scores;
// limit > 0 stops the walk as soon as that many members survived
// (limit 0 is unlimited, Redis's LIMIT 0).
func (z *ZSet) ZInterCard(ctx context.Context, keys [][]byte, limit int64) (int64, error) {
	srcs, err := z.zloadSrcs(ctx, keys, nil)
	if err != nil {
		return 0, err
	}
	order := z.zorderByCount(srcs)
	if order[0].count == 0 {
		return 0, nil
	}
	return z.zalgWalk(ctx, order[0], order[1:], true, false, zaggSum, limit, nil)
}

// ZDiff computes the members of the first source that are in none of
// the rest, with the first source's scores: ZDIFF has no weights and
// no aggregation, and the driver is the first key whatever its size.
func (z *ZSet) ZDiff(ctx context.Context, keys [][]byte) ([]zbuildPair, []byte, error) {
	srcs, err := z.zloadSrcs(ctx, keys, nil)
	if err != nil {
		return nil, nil, err
	}
	z.zarest = z.zarest[:0]
	for i := 1; i < len(srcs); i++ {
		z.zarest = append(z.zarest, &srcs[i])
	}
	z.zaarena, z.zapairs = z.zaarena[:0], z.zapairs[:0]
	if _, err := z.zalgWalk(ctx, &srcs[0], z.zarest, false, true, zaggSum, 0, z.zalgCollect); err != nil {
		return nil, nil, err
	}
	sort.Sort(&zpairSorter{pairs: z.zapairs, arena: z.zaarena})
	return z.zapairs, z.zaarena, nil
}

// zalgStore lands sorted result pairs on dest through the shared bulk
// build: a fresh-rooth bottom-up build whose root PUT is the commit
// point, dest deleted on an empty result, doc 09 section 6.
func (z *ZSet) zalgStore(ctx context.Context, dest []byte, pairs []zbuildPair, arena []byte) (int64, error) {
	b := z.beginZBuild()
	for _, p := range pairs {
		b.add(p.s, arena[p.off:p.end])
	}
	return b.commit(ctx, dest)
}

// ZUnionStore computes ZUnion into dest and answers the stored
// cardinality.
func (z *ZSet) ZUnionStore(ctx context.Context, dest []byte, keys [][]byte, weights []float64, agg zaggMode) (int64, error) {
	pairs, arena, err := z.ZUnion(ctx, keys, weights, agg)
	if err != nil {
		return 0, err
	}
	return z.zalgStore(ctx, dest, pairs, arena)
}

// ZInterStore computes ZInter into dest.
func (z *ZSet) ZInterStore(ctx context.Context, dest []byte, keys [][]byte, weights []float64, agg zaggMode) (int64, error) {
	pairs, arena, err := z.ZInter(ctx, keys, weights, agg)
	if err != nil {
		return 0, err
	}
	return z.zalgStore(ctx, dest, pairs, arena)
}

// ZDiffStore computes ZDiff into dest.
func (z *ZSet) ZDiffStore(ctx context.Context, dest []byte, keys [][]byte) (int64, error) {
	pairs, arena, err := z.ZDiff(ctx, keys)
	if err != nil {
		return 0, err
	}
	return z.zalgStore(ctx, dest, pairs, arena)
}
