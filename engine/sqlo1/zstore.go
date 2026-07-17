package sqlo1

// The zset bulk build, doc 09 section 6: a STORE destination builds
// bottom-up under a fresh rooth, so the intermediate state is
// invisible and the root PUT is the commit point, exactly the set
// STORE discipline. Both families build in one pass each: the score
// runs cut at zRunMax straight off the score-ordered input (paging
// the fence when it outgrows the flat cap), and the member segments
// cut at seg_max after one sort into (fh, member) order, through the
// shared commitFence. ZRANGESTORE collects its source window first
// and builds after, because the member sort needs the whole result in
// hand anyway and collecting first frees the shared ladder state the
// source walk was using; the zset algebra (slice 9) will feed the
// same builder from its merges.

import (
	"context"
	"encoding/binary"
)

// zbuildPair is one collected build entry: the sortable score and the
// member's span in the build arena, offsets because the arena
// reallocates as it grows.
type zbuildPair struct {
	s        uint64
	off, end int
}

// zBuilder accumulates a bulk build's entries in (score, member)
// order and lands them on a destination in one commit.
type zBuilder struct {
	z     *ZSet
	arena []byte
	pairs []zbuildPair

	// Member-side scratch: the sortable images (8 bytes per pair, one
	// arena so hashSegEntry values stay put), the sortable-ordered
	// entries re-sorted into segment order, and the fence under
	// construction.
	valArena []byte
	ments    []hashSegEntry
	mfence   []hashFenceEnt

	// Score-side scratch: the fence under construction and the page
	// index levels when it pages.
	sfence []zFenceEnt
	leaves []zIdxEnt
	uppers []zIdxEnt
}

// beginZBuild resets the zset's builder. The zset's own ladder is
// free during a STORE: the source window was collected before the
// build starts.
func (z *ZSet) beginZBuild() *zBuilder {
	b := &z.zbld
	b.z = z
	b.arena = b.arena[:0]
	b.pairs = b.pairs[:0]
	return b
}

// add collects one entry. Entries must arrive in ascending (score,
// member) order with unique members; both come free from a range walk
// or an ordered merge.
func (b *zBuilder) add(s uint64, member []byte) {
	off := len(b.arena)
	b.arena = append(b.arena, member...)
	b.pairs = append(b.pairs, zbuildPair{s: s, off: off, end: len(b.arena)})
}

// commit lands the collected entries on dest and answers the stored
// cardinality. Empty results delete dest, inline-sized results are
// one root write, and segmented results land segments, runs, and
// pages under a fresh rooth, flush, and commit with the root PUT,
// clearing any expiry dest carried.
func (b *zBuilder) commit(ctx context.Context, dest []byte) (int64, error) {
	z, h := b.z, b.z.h
	n := len(b.pairs)
	if n == 0 {
		return h.storeEmpty(ctx, dest)
	}

	// The inline door: the pairs are already in the region's (score,
	// member) order.
	if n <= hashInlineMaxCount {
		h.rootBuf = appendHashInlineHdr(h.rootBuf[:0], h.subInline, n, 0, false)
		fits := true
		for _, p := range b.pairs {
			binary.BigEndian.PutUint64(z.sbuf[:], p.s)
			h.rootBuf = appendHashEntry(h.rootBuf, b.arena[p.off:p.end], z.sbuf[:], 0, h.enc)
			if len(h.rootBuf) > hashInlineMax {
				fits = false
				break
			}
		}
		if fits {
			exists, expMs, err := h.destPrep(ctx, dest)
			if err != nil {
				return 0, err
			}
			if err := h.t.Set(ctx, dest, h.rootBuf, h.tag|TagRoot); err != nil {
				return 0, err
			}
			return int64(n), h.clearDestExp(ctx, dest, exists, expMs)
		}
	}

	rooth, err := h.nextRooth(ctx)
	if err != nil {
		return 0, err
	}
	h.segRoot = hashSegRoot{sub: h.subSeg, rootgen: 1, rooth: rooth, pi: -1}
	r := &h.segRoot

	// Member family: one sort into (fh, member) order, segments cut
	// at seg_max, never between equal fh values, fence built in the
	// same pass.
	b.valArena = grow(b.valArena, n*zmemScoreLen)
	b.ments = b.ments[:0]
	for i, p := range b.pairs {
		v := b.valArena[i*zmemScoreLen : (i+1)*zmemScoreLen]
		binary.BigEndian.PutUint64(v, p.s)
		m := b.arena[p.off:p.end]
		b.ments = append(b.ments, hashSegEntry{fh: hashFH(m), field: m, val: v})
	}
	sortHashSegEntries(b.ments)
	b.mfence = b.mfence[:0]
	base, size := 0, hashSegHdrLen
	cutSeg := func(hi int) error {
		h.segBuf = appendHashSegPayload(h.segBuf[:0], b.ments[base:hi], encZMem)
		segid := r.nextSegid
		if err := h.writeSeg(ctx, segid, h.segBuf); err != nil {
			return err
		}
		lo := uint64(0)
		if len(b.mfence) > 0 {
			lo = b.ments[base].fh
		}
		b.mfence = append(b.mfence, hashFenceEnt{
			lo:    lo,
			segid: segid,
			meta:  hashSegMeta(hi-base, 0),
		})
		r.nextSegid++
		if len(b.mfence) > hashFencePageIdxMax*hashFencePageMax {
			return errHashFenceThirdLevel
		}
		base, size = hi, hashSegHdrLen
		return nil
	}
	for i := range b.ments {
		es := hashEntrySize(len(b.ments[i].field), zmemScoreLen, 0, encZMem)
		if i > base && size+es > hashSegMax && b.ments[i].fh != b.ments[i-1].fh {
			if err := cutSeg(i); err != nil {
				return 0, err
			}
		}
		size += es
	}
	if err := cutSeg(len(b.ments)); err != nil {
		return 0, err
	}
	if err := h.commitFence(ctx, b.mfence); err != nil {
		return 0, err
	}

	// Score family: runs cut at zRunMax in arrival order, the
	// zupgrade shape under a bulk fence.
	b.sfence = b.sfence[:0]
	z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
	runN := 0
	runLo := uint64(0)
	closeRun := func() error {
		if len(b.sfence) >= zFenceRootMax*zFenceUpperMax*zFenceLeafMax {
			return errZFenceThirdLevel
		}
		putZRunHdr(z.zrbuf, runN)
		if err := z.writeRun(ctx, r.nextSegid, z.zrbuf); err != nil {
			return err
		}
		lo := runLo
		if len(b.sfence) == 0 {
			lo = 0 // the sentinel separator, below every legal score
		}
		b.sfence = append(b.sfence, zFenceEnt{lo: lo, segid: r.nextSegid, count: uint32(runN)})
		r.nextSegid++
		z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
		runN = 0
		return nil
	}
	for _, p := range b.pairs {
		m := b.arena[p.off:p.end]
		if runN > 0 && len(z.zrbuf)+zRunEntHdrLen+len(m) > zRunMax {
			if err := closeRun(); err != nil {
				return 0, err
			}
		}
		if runN == 0 {
			runLo = p.s
		}
		z.zrbuf = appendZRunEnt(z.zrbuf, p.s, m)
		runN++
	}
	if err := closeRun(); err != nil {
		return 0, err
	}
	if err := z.commitZFence(ctx, b); err != nil {
		return 0, err
	}

	r.count = uint64(n)
	if err := h.t.Flush(ctx); err != nil {
		return 0, err
	}
	exists, expMs, err := h.destPrep(ctx, dest)
	if err != nil {
		return 0, err
	}
	if err := z.writeZRoot(ctx, dest); err != nil {
		return 0, err
	}
	return int64(n), h.clearDestExp(ctx, dest, exists, expMs)
}

// commitZFence installs the built score fence: flat in the root tail
// under the cap, otherwise paged two-level in one pass, full pages
// except the last at each level, mirroring the member fence's bulk
// paging.
func (z *ZSet) commitZFence(ctx context.Context, b *zBuilder) error {
	if len(b.sfence) <= zFenceMaxRuns {
		z.zpaged = false
		z.zridx = z.zridx[:0]
		z.zfence = append(z.zfence[:0], b.sfence...)
		return nil
	}
	r := &z.h.segRoot
	b.leaves = b.leaves[:0]
	for base := 0; base < len(b.sfence); base += zFenceLeafMax {
		n := min(zFenceLeafMax, len(b.sfence)-base)
		page := b.sfence[base : base+n]
		pageid := r.nextSegid
		r.nextSegid++
		z.zpbuf = appendZLeafPage(z.zpbuf[:0], page)
		if err := z.writeZPageRaw(ctx, pageid, z.zpbuf); err != nil {
			return err
		}
		b.leaves = append(b.leaves, zIdxEnt{
			lo:     page[0].lo,
			pageid: pageid,
			runs:   uint16(n),
			count:  zfenceSum(page),
		})
	}
	b.uppers = b.uppers[:0]
	for base := 0; base < len(b.leaves); base += zFenceUpperMax {
		n := min(zFenceUpperMax, len(b.leaves)-base)
		page := b.leaves[base : base+n]
		pageid := r.nextSegid
		r.nextSegid++
		z.zpbuf = appendZUpperPage(z.zpbuf[:0], page)
		if err := z.writeZPageRaw(ctx, pageid, z.zpbuf); err != nil {
			return err
		}
		runs, count := zidxSum(page)
		b.uppers = append(b.uppers, zIdxEnt{
			lo:     page[0].lo,
			pageid: pageid,
			runs:   uint16(runs),
			count:  count,
		})
	}
	if len(b.uppers) > zFenceRootMax {
		return errZFenceThirdLevel
	}
	z.zpaged = true
	z.zridx = append(z.zridx[:0], b.uppers...)
	z.zui, z.zli = -1, -1
	return nil
}

// ZRangeStore stores the source window of forward ranks [lo, hi) into
// dest and answers the stored cardinality, the ZRANGESTORE surface
// once the command layer has resolved the BY form, REV, and LIMIT
// into ranks. An empty window deletes dest whatever it held.
func (z *ZSet) ZRangeStore(ctx context.Context, dest, src []byte, lo, hi int64) (int64, error) {
	b := z.beginZBuild()
	if err := z.zwalkRank(ctx, src, lo, hi, func(s uint64, m []byte) bool {
		b.add(s, m)
		return true
	}); err != nil {
		return 0, err
	}
	return b.commit(ctx, dest)
}
