package sqlo1

// PEL fence paging, the scoped follow-up the xpel lab's verdict routed
// (#1270): once a group's pending set outgrows the inline fence, the
// fence rows move into kind 6 pages and the group record keeps a page
// index in the same 28-byte row shape, so a delivery or ack batch
// rebills the record at the index's size instead of the whole fence.
// The layer keeps every PEL algorithm untouched: a paged group
// materializes its full fence into g.pelf on demand (pelfLoad), the
// algorithms edit that flat view exactly as they edit an inline fence,
// and the store side (pelfPlan then pelfFresh and pelfAmend) diffs the
// edited fence against the loaded image and rewrites only the touched
// pages. Fresh pages mint pageids from the shared root counter and
// flush before the record that references them, in-place page rewrites
// ride the record's batch, and dead pages die after the record write,
// the discipline every other paged fence here follows.

import (
	"context"
	"encoding/binary"
	"fmt"
)

// streamSubkindPelPage is the stream plane's PEL fence page kind, the
// kind 6 the paging follow-up added beside doc 10's kinds 1 through 5.
const streamSubkindPelPage uint8 = 6

// streamPelPageMax caps a page at 146 fence rows, 4 + 146*28 = 4092
// encoded bytes, the same 4 KiB line every other page here sits under.
// A var so tests reach the transition and split paths at test sizes.
var streamPelPageMax = (4096 - streamPelPageHdrLen) / streamPelFenceEntLen

// streamPelPageHdrLen is the page payload header: u16 n, u16 reserved.
const streamPelPageHdrLen = 4

// putStreamPelPageKey writes the subkey of PEL fence page pageid under
// rooth, the doc 03 6.3 layout with kind streamSubkindPelPage.
func putStreamPelPageKey(dst []byte, rooth uint64, pageid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = streamSubkindPelPage
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], pageid)
	copy(dst[9:SubkeySize], b[:7])
}

// appendStreamPelFencePage encodes ents as one page payload onto dst:
// u16 n, u16 reserved, then n fence rows in the record's own row
// encoding. The contract violations are writer bugs, so they panic.
func appendStreamPelFencePage(dst []byte, ents []streamPelFenceEnt) []byte {
	if len(ents) == 0 {
		panic("sqlo1: empty stream PEL fence page")
	}
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(ents)))
	dst = binary.LittleEndian.AppendUint16(dst, 0)
	for i := range ents {
		f := &ents[i]
		if f.count == 0 {
			panic("sqlo1: stream PEL fence page row with count zero")
		}
		if i > 0 && !ents[i-1].base.less(f.base) {
			panic("sqlo1: stream PEL fence page rows out of order")
		}
		dst = binary.LittleEndian.AppendUint64(dst, f.base.ms)
		dst = binary.LittleEndian.AppendUint64(dst, f.base.seq)
		dst = binary.LittleEndian.AppendUint64(dst, f.segid)
		dst = binary.LittleEndian.AppendUint32(dst, f.count)
	}
	return dst
}

// decodeStreamPelFencePage validates v and decodes it into the ents
// scratch: canonical form is exact length, a zero reserved field, one
// row at least, strictly increasing bases, counts at least one, and
// segids below the root's next mint.
func decodeStreamPelFencePage(v []byte, nextSegid uint64, ents []streamPelFenceEnt) ([]streamPelFenceEnt, uint64, error) {
	if len(v) < streamPelPageHdrLen {
		return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page of %d bytes has no header", len(v))
	}
	n := int(binary.LittleEndian.Uint16(v))
	if binary.LittleEndian.Uint16(v[2:]) != 0 {
		return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page has a nonzero reserved field")
	}
	if n == 0 {
		return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page is empty")
	}
	if len(v) != streamPelPageHdrLen+n*streamPelFenceEntLen {
		return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page of %d bytes does not hold %d rows", len(v), n)
	}
	p := v[streamPelPageHdrLen:]
	var sum uint64
	for i := range n {
		f := streamPelFenceEnt{
			base:  streamID{ms: binary.LittleEndian.Uint64(p[0:]), seq: binary.LittleEndian.Uint64(p[8:])},
			segid: binary.LittleEndian.Uint64(p[16:]),
			count: binary.LittleEndian.Uint32(p[24:]),
		}
		if f.count == 0 {
			return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page row %d is empty", i)
		}
		if f.segid >= nextSegid {
			return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page row %d holds unminted segid %d", i, f.segid)
		}
		if len(ents) > 0 && !ents[len(ents)-1].base.less(f.base) {
			return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page out of order at row %d", i)
		}
		sum += uint64(f.count)
		ents = append(ents, f)
		p = p[streamPelFenceEntLen:]
	}
	return ents, sum, nil
}

// readPelPage reads page pageid, decoding into ents.
func (x *Stream) readPelPage(ctx context.Context, pageid uint64, ents []streamPelFenceEnt) ([]streamPelFenceEnt, uint64, error) {
	putStreamPelPageKey(x.pelPgKbuf[:], x.root.rooth, pageid)
	v, ok, err := x.t.Get(ctx, x.pelPgKbuf[:])
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, fmt.Errorf("sqlo1: stream PEL fence page %d of rooth %#x is missing", pageid, x.root.rooth)
	}
	return decodeStreamPelFencePage(v, x.root.nextSegid, ents)
}

// writePelPage encodes ents and lands them at pageid.
func (x *Stream) writePelPage(ctx context.Context, pageid uint64, ents []streamPelFenceEnt) error {
	x.pelPgBuf = appendStreamPelFencePage(x.pelPgBuf[:0], ents)
	putStreamPelPageKey(x.pelPgKbuf[:], x.root.rooth, pageid)
	return x.t.SetGen(ctx, x.pelPgKbuf[:], x.pelPgBuf, TagStream|TagFence, x.root.rootgen)
}

// delPelPage drops page pageid, always after the record write that
// stopped referencing it, in the same batch.
func (x *Stream) delPelPage(ctx context.Context, pageid uint64) error {
	putStreamPelPageKey(x.pelPgKbuf[:], x.root.rooth, pageid)
	_, err := x.t.Del(ctx, x.pelPgKbuf[:])
	return err
}

// pelfLoad materializes a paged group's fence into g.pelf, page by
// page in index order, cross-checking the two-level invariant per
// page: the page's first base is the index base, its counts sum to
// the index total, and the pages stay in strict base order across the
// boundaries. The load keeps the pristine image (pelfOrig and the
// per-page row counts in pelPageN) for the store-side diff, backed by
// stream-level scratch, safe because ops run one at a time and a
// group is loaded once per op. Inline groups are a no-op.
func (x *Stream) pelfLoad(ctx context.Context, g *streamGroup) error {
	if !g.pelPaged || g.pelLoaded {
		return nil
	}
	x.pelfBuf = x.pelfBuf[:0]
	x.pelfPageN = x.pelfPageN[:0]
	for j := range g.pelIdx {
		ie := &g.pelIdx[j]
		before := len(x.pelfBuf)
		ents, sum, err := x.readPelPage(ctx, ie.segid, x.pelfBuf)
		if err != nil {
			return err
		}
		x.pelfBuf = ents
		rows := x.pelfBuf[before:]
		if sum != uint64(ie.count) {
			return fmt.Errorf("sqlo1: stream PEL fence page %d sums to %d entries, index says %d", ie.segid, sum, ie.count)
		}
		if rows[0].base != ie.base {
			return fmt.Errorf("sqlo1: stream PEL fence page %d starts at %d-%d, index says %d-%d", ie.segid, rows[0].base.ms, rows[0].base.seq, ie.base.ms, ie.base.seq)
		}
		if before > 0 && !x.pelfBuf[before-1].base.less(rows[0].base) {
			return fmt.Errorf("sqlo1: stream PEL fence page %d out of order against its predecessor", ie.segid)
		}
		x.pelfPageN = append(x.pelfPageN, len(rows))
	}
	x.pelfPrist = append(x.pelfPrist[:0], x.pelfBuf...)
	g.pelf = x.pelfBuf
	g.pelfOrig = x.pelfPrist
	g.pelPageN = x.pelfPageN
	g.pelLoaded = true
	return nil
}

// pelPagePlanEnt is one planned page: rows g.pelf[lo:hi), landing at
// pageid, minted at write time when fresh. write marks pages whose
// bytes must land this op; kept prefix and suffix pages stay put.
type pelPagePlanEnt struct {
	pageid uint64
	lo, hi int
	write  bool
	fresh  bool
}

// pelfPlan diffs the edited fence in g.pelf against the loaded page
// image and plans the page layout: the longest page-aligned common
// prefix and suffix keep their pages untouched, and the middle window
// re-chunks front-first at the page cap, reusing the replaced middle
// pages' ids before minting fresh ones and dropping the leftovers.
// The plan is pure: nothing is written, minted, or mutated, so a
// capacity refusal here is side-effect free. Three modes fall out of
// the same walk: an inline fence within the cap plans nothing, an
// inline fence past the cap transitions (no pristine pages, so the
// whole fence is middle window and every page is fresh), and a paged
// fence at or under half the cap flips back inline, every page
// dropping. planned reports whether pelfFresh and pelfAmend have work.
func (x *Stream) pelfPlan(g *streamGroup) (planned bool, err error) {
	x.pelPagePlan = x.pelPagePlan[:0]
	x.pelPageDrops = x.pelPageDrops[:0]
	x.pelfFlip = false
	if g.pelPaged && !g.pelLoaded {
		return false, fmt.Errorf("sqlo1: stream PEL fence plan over an unloaded paged group %q", g.name)
	}
	n := len(g.pelf)
	if !g.pelPaged && n <= streamPelFenceMax {
		return false, nil
	}
	if g.pelPaged && n <= streamPelFenceMax/2 {
		// Flip back inline: the record takes the rows, the pages die.
		for j := range g.pelIdx {
			x.pelPageDrops = append(x.pelPageDrops, g.pelIdx[j].segid)
		}
		x.pelfFlip = true
		return true, nil
	}
	old := g.pelfOrig
	oldN := g.pelPageN
	// Entry-wise common prefix and suffix, clipped so they never
	// overlap on the shorter side.
	p := 0
	for p < len(old) && p < n && old[p] == g.pelf[p] {
		p++
	}
	s := 0
	for s < len(old)-p && s < n-p && old[len(old)-1-s] == g.pelf[n-1-s] {
		s++
	}
	// Align to pristine page boundaries: kp pages of the prefix stay,
	// km pages of the suffix stay, and the rows between are the middle
	// window on both sides.
	kp, prefixRows := 0, 0
	for kp < len(oldN) && prefixRows+oldN[kp] <= p {
		prefixRows += oldN[kp]
		kp++
	}
	km, suffixRows := 0, 0
	for km < len(oldN)-kp && suffixRows+oldN[len(oldN)-1-km] <= s {
		suffixRows += oldN[len(oldN)-1-km]
		km++
	}
	mid := g.pelf[prefixRows : n-suffixRows]
	chunks := (len(mid) + streamPelPageMax - 1) / streamPelPageMax
	if kp+chunks+km > streamPelFenceMax {
		return false, errStreamPelFenceFull
	}
	for j := range kp {
		x.pelPagePlan = append(x.pelPagePlan, pelPagePlanEnt{
			pageid: g.pelIdx[j].segid,
			lo:     pageRowStart(oldN, j),
			hi:     pageRowStart(oldN, j+1),
		})
	}
	// The middle window's replaced pages hand over their ids in order;
	// chunks past them mint fresh, leftovers die.
	midIDs := 0
	if g.pelPaged {
		midIDs = len(oldN) - kp - km
	}
	for c := range chunks {
		lo := prefixRows + c*streamPelPageMax
		hi := min(lo+streamPelPageMax, prefixRows+len(mid))
		pe := pelPagePlanEnt{lo: lo, hi: hi, write: true}
		if c < midIDs {
			pe.pageid = g.pelIdx[kp+c].segid
		} else {
			pe.fresh = true
		}
		x.pelPagePlan = append(x.pelPagePlan, pe)
	}
	for c := chunks; c < midIDs; c++ {
		x.pelPageDrops = append(x.pelPageDrops, g.pelIdx[kp+c].segid)
	}
	for j := range km {
		oj := len(oldN) - km + j
		shift := n - len(old)
		x.pelPagePlan = append(x.pelPagePlan, pelPagePlanEnt{
			pageid: g.pelIdx[oj].segid,
			lo:     pageRowStart(oldN, oj) + shift,
			hi:     pageRowStart(oldN, oj+1) + shift,
		})
	}
	return true, nil
}

// pageRowStart is the row offset of page j under the pristine chunking.
func pageRowStart(oldN []int, j int) int {
	off := 0
	for i := range j {
		off += oldN[i]
	}
	return off
}

// pelfFresh mints and writes the plan's fresh pages. It runs before
// the caller's flush barrier, so a crash prefix without the record
// leaves only orphan pages; rootDirty reports the mints the caller
// must land with a root write.
func (x *Stream) pelfFresh(ctx context.Context, g *streamGroup) (rootDirty bool, err error) {
	for j := range x.pelPagePlan {
		pe := &x.pelPagePlan[j]
		if !pe.fresh {
			continue
		}
		pe.pageid = x.root.nextSegid
		x.root.nextSegid++
		rootDirty = true
		if err := x.writePelPage(ctx, pe.pageid, g.pelf[pe.lo:pe.hi]); err != nil {
			return false, err
		}
	}
	return rootDirty, nil
}

// pelfAmend rewrites the plan's in-place pages, riding the caller's
// record batch, and rebuilds the group's page index (or clears it on
// a flip back inline). The dead pages queued at plan time die after
// the record write, pelDropPending's batch.
func (x *Stream) pelfAmend(ctx context.Context, g *streamGroup) error {
	if x.pelfFlip {
		g.pelPaged = false
		g.pelIdx = nil
		g.pelPageN = nil
		g.pelfOrig = nil
		return nil
	}
	x.pelIdxBuf = x.pelIdxBuf[:0]
	for j := range x.pelPagePlan {
		pe := &x.pelPagePlan[j]
		if pe.write && !pe.fresh {
			if err := x.writePelPage(ctx, pe.pageid, g.pelf[pe.lo:pe.hi]); err != nil {
				return err
			}
		}
		var sum uint64
		for i := pe.lo; i < pe.hi; i++ {
			sum += uint64(g.pelf[i].count)
		}
		x.pelIdxBuf = append(x.pelIdxBuf, streamPelFenceEnt{base: g.pelf[pe.lo].base, segid: pe.pageid, count: uint32(sum)})
	}
	g.pelIdx = x.pelIdxBuf
	g.pelPaged = true
	return nil
}

// pelfStore is the shrink-side store pass: plan, then fresh and amend
// back to back. Shrinking edits never mint (a re-chunk of a shrunken
// middle never outgrows the pages it replaces), so no flush barrier
// sits between and everything rides the caller's record batch; the
// return value reports mints anyway, belt and braces.
func (x *Stream) pelfStore(ctx context.Context, g *streamGroup) (rootDirty bool, err error) {
	planned, err := x.pelfPlan(g)
	if err != nil {
		return false, err
	}
	if !planned {
		return false, nil
	}
	rootDirty, err = x.pelfFresh(ctx, g)
	if err != nil {
		return false, err
	}
	return rootDirty, x.pelfAmend(ctx, g)
}
