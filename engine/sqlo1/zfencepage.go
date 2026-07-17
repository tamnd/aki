package sqlo1

// The paged score fence, doc 09 section 2 at the zrank lab's verdict
// (#1014): past the flat tail the fence pages two-level at 250/250.
// The root tail holds an index of upper pages, each upper page an
// index of leaf pages, each leaf page a slice of the fence itself in
// the flat 20-byte entry encoding. Index entries carry two subtree
// totals: the live member count (u64, a top subtree overflows u32),
// which makes a rank a three-level prefix sum plus the in-run index,
// and the run count in the 16 bits over the pageid, which gives every
// run a global position without loading anything below the walk.
//
// Reach is root x upper x leaf runs, ~1.6e9 members at the hsegz
// occupancy; the split that would need a third level fails loudly
// like the hash fence does, and the dual command probes that corner
// before either family writes, so capacity can never tear Z-I1.
//
// Pages are kind 4 records under the plane and bill strictly in the
// command's frame group (the lab priced the drain-coalescing lever at
// 5 percent on the gate cell and slice 5 does not build it): a count
// edit rewrites its leaf and upper in place beside the root, one
// batch, atomic at the store seam. Splits write BOTH halves under
// fresh pageids and flush before the root that references them, so a
// crash prefix reads the old page whole through the old root; the
// replaced records die after the root lands, a bounded orphan the
// plane retire cleans. Page images therefore always carry exact
// counts, no clipping, and every load cross-checks its parent's
// totals.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// Score fence fanouts, the zrank lab's 250/250 verdict. Vars, not
// consts, so the paged ladder (transition, leaf split, upper split,
// third-level error) is reachable in test-sized zsets; production
// never changes them. The transition builds one leaf from the whole
// flat fence, so zFenceMaxRuns+1 must fit zFenceLeafMax.
var (
	// zFenceMaxRuns is the flat tail's run cap, doc 09's ~100: 20
	// bytes per run keeps the tail's every-command bill around 2 KB.
	// The split past it is the paging transition.
	zFenceMaxRuns = 100

	// zFenceLeafMax is the most fence entries one leaf page holds,
	// ~5.0 KB encoded.
	zFenceLeafMax = 250

	// zFenceUpperMax is the most leaf entries one upper page indexes,
	// ~5.9 KB encoded.
	zFenceUpperMax = 250

	// zFenceRootMax is the most upper entries the root tail indexes,
	// 5.9 KB worst; at the 1e9 headline it sits at ~154 entries,
	// 3.7 KB. Past it is errZFenceThirdLevel.
	zFenceRootMax = 250
)

const (
	// zPageKind is the subkey kind of score fence pages, doc 09's
	// kind 4. Pageids mint from the root's segid counter like runs.
	zPageKind = 4

	// zPageHdrLen is the page payload header: u16 entry count, u8
	// level (leaf or upper), u8 zero.
	zPageHdrLen = 4

	zPageLeaf  = 0
	zPageUpper = 1

	// zIdxEntLen is one index entry, upper pages and the paged root
	// tail alike: u64 score_lo, 48-bit pageid under the u16 subtree
	// run count, u64 subtree member count.
	zIdxEntLen = 24
)

// errZFenceThirdLevel marks the end of the two-level score fence: a
// zset whose root index, covering upper, and covering leaf are all
// full is out of the format's design envelope, and the write that
// would need a third level fails loudly before touching anything.
var errZFenceThirdLevel = errors.New("sqlo1: zset score fence root index is full, the two-level fence is spent")

// zIdxEnt is one decoded index entry: the subtree's first run
// separator, the child page's record id, and the subtree's two
// totals.
type zIdxEnt struct {
	lo     uint64
	pageid uint64
	runs   uint16
	count  uint64
}

// putZPageKey writes the subkey of score fence page pageid under
// rooth: the doc 03 6.3 layout with kind zPageKind.
func putZPageKey(dst []byte, rooth, pageid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = zPageKind
	var pg [8]byte
	binary.LittleEndian.PutUint64(pg[:], pageid)
	copy(dst[9:SubkeySize], pg[:7])
}

// appendZIdxEnts encodes index entries, the shared tail of the paged
// root and upper pages.
func appendZIdxEnts(dst []byte, f []zIdxEnt) []byte {
	var e [zIdxEntLen]byte
	for i := range f {
		binary.LittleEndian.PutUint64(e[:8], f[i].lo)
		binary.LittleEndian.PutUint64(e[8:16], f[i].pageid|uint64(f[i].runs)<<48)
		binary.LittleEndian.PutUint64(e[16:], f[i].count)
		dst = append(dst, e[:]...)
	}
	return dst
}

// decodeZIdxEnts parses n index entries at p into dst[:0], with the
// ordering and sentinel rules shared by both index levels: los are
// non-decreasing (an equal-separator chain may span subtrees), lo 0
// and count 0 belong to the sentinel subtree alone, which is entry 0
// of a first page only.
func decodeZIdxEnts(p []byte, n int, dst []zIdxEnt, first bool, what string) ([]zIdxEnt, error) {
	dst = dst[:0]
	for i := range n {
		e := p[i*zIdxEntLen:]
		pw := binary.LittleEndian.Uint64(e[8:16])
		ent := zIdxEnt{
			lo:     binary.LittleEndian.Uint64(e[:8]),
			pageid: pw & hashFenceSegidMax,
			runs:   uint16(pw >> 48),
			count:  binary.LittleEndian.Uint64(e[16:]),
		}
		if ent.runs == 0 {
			return nil, fmt.Errorf("sqlo1: %s entry %d spans zero runs", what, i)
		}
		sentinel := first && i == 0
		if sentinel {
			if ent.lo != 0 {
				return nil, fmt.Errorf("sqlo1: %s starts at %#x, want the 0 sentinel", what, ent.lo)
			}
		} else {
			if ent.lo == 0 {
				return nil, fmt.Errorf("sqlo1: %s entry %d separates at 0 past the sentinel", what, i)
			}
			if i > 0 && ent.lo < dst[i-1].lo {
				return nil, fmt.Errorf("sqlo1: %s out of order at entry %d", what, i)
			}
			if ent.count == 0 {
				return nil, fmt.Errorf("sqlo1: %s entry %d is empty, only the sentinel subtree may be", what, i)
			}
		}
		dst = append(dst, ent)
	}
	return dst, nil
}

// appendZTailPaged encodes the paged score section of a zset root:
// the flat header with zflagFencePaged set, then the root index.
func appendZTailPaged(dst []byte, f []zIdxEnt) []byte {
	var hdr [zTailHdrLen]byte
	hdr[0] = zflagFencePaged
	binary.LittleEndian.PutUint16(hdr[2:], uint16(len(f)))
	dst = append(dst, hdr[:]...)
	return appendZIdxEnts(dst, f)
}

// decodeZTailPaged parses a paged root's score section into dst[:0].
func decodeZTailPaged(p []byte, dst []zIdxEnt) ([]zIdxEnt, error) {
	if len(p) < zTailHdrLen {
		return nil, fmt.Errorf("sqlo1: paged zset score section of %d bytes, header needs %d", len(p), zTailHdrLen)
	}
	if p[0] != zflagFencePaged || p[1] != 0 {
		return nil, fmt.Errorf("sqlo1: paged zset score section flags %#x unknown", p[0])
	}
	n := int(binary.LittleEndian.Uint16(p[2:]))
	if n == 0 || n > zFenceRootMax {
		return nil, fmt.Errorf("sqlo1: zset score root index of %d entries outside [1, %d]", n, zFenceRootMax)
	}
	if want := zTailHdrLen + n*zIdxEntLen; len(p) != want {
		return nil, fmt.Errorf("sqlo1: paged zset score section of %d bytes, %d index entries need %d", len(p), n, want)
	}
	return decodeZIdxEnts(p[zTailHdrLen:], n, dst, true, "zset score root index")
}

// appendZUpperPage encodes an upper page payload.
func appendZUpperPage(dst []byte, f []zIdxEnt) []byte {
	var hdr [zPageHdrLen]byte
	binary.LittleEndian.PutUint16(hdr[:2], uint16(len(f)))
	hdr[2] = zPageUpper
	dst = append(dst, hdr[:]...)
	return appendZIdxEnts(dst, f)
}

// decodeZUpperPage parses an upper page payload into dst[:0]. first
// marks the root's first subtree, the only place the sentinel rules
// apply; nonzero nextSegid is the mint ceiling for the pageids.
func decodeZUpperPage(p []byte, dst []zIdxEnt, nextSegid uint64, first bool) ([]zIdxEnt, error) {
	if len(p) < zPageHdrLen {
		return nil, fmt.Errorf("sqlo1: zset upper page of %d bytes, header needs %d", len(p), zPageHdrLen)
	}
	if p[2] != zPageUpper || p[3] != 0 {
		return nil, fmt.Errorf("sqlo1: zset upper page header %#x %#x, want level %d", p[2], p[3], zPageUpper)
	}
	n := int(binary.LittleEndian.Uint16(p))
	if n == 0 || n > zFenceUpperMax {
		return nil, fmt.Errorf("sqlo1: zset upper page of %d entries outside [1, %d]", n, zFenceUpperMax)
	}
	if want := zPageHdrLen + n*zIdxEntLen; len(p) != want {
		return nil, fmt.Errorf("sqlo1: zset upper page of %d bytes, %d entries need %d", len(p), n, want)
	}
	out, err := decodeZIdxEnts(p[zPageHdrLen:], n, dst, first, "zset upper page")
	if err != nil {
		return nil, err
	}
	if nextSegid != 0 {
		for i := range out {
			if out[i].pageid >= nextSegid {
				return nil, fmt.Errorf("sqlo1: zset upper page leaf id %d at or past the mint counter %d", out[i].pageid, nextSegid)
			}
		}
	}
	return out, nil
}

// appendZLeafPage encodes a leaf page payload: the fence entries in
// the flat tail's 20-byte encoding behind a page header.
func appendZLeafPage(dst []byte, f []zFenceEnt) []byte {
	var hdr [zPageHdrLen]byte
	binary.LittleEndian.PutUint16(hdr[:2], uint16(len(f)))
	hdr[2] = zPageLeaf
	dst = append(dst, hdr[:]...)
	var e [zFenceEntLen]byte
	for i := range f {
		binary.LittleEndian.PutUint64(e[:8], f[i].lo)
		binary.LittleEndian.PutUint64(e[8:16], f[i].segid|uint64(f[i].meta)<<48)
		binary.LittleEndian.PutUint32(e[16:], f[i].count)
		dst = append(dst, e[:]...)
	}
	return dst
}

// decodeZLeafPage parses a leaf page payload into dst[:0]. first
// marks the fence's first leaf, the only page that may hold the
// sentinel (lo 0, count 0 allowed at entry 0).
func decodeZLeafPage(p []byte, dst []zFenceEnt, nextSegid uint64, first bool) ([]zFenceEnt, error) {
	if len(p) < zPageHdrLen {
		return nil, fmt.Errorf("sqlo1: zset leaf page of %d bytes, header needs %d", len(p), zPageHdrLen)
	}
	if p[2] != zPageLeaf || p[3] != 0 {
		return nil, fmt.Errorf("sqlo1: zset leaf page header %#x %#x, want level %d", p[2], p[3], zPageLeaf)
	}
	n := int(binary.LittleEndian.Uint16(p))
	if n == 0 || n > zFenceLeafMax {
		return nil, fmt.Errorf("sqlo1: zset leaf page of %d entries outside [1, %d]", n, zFenceLeafMax)
	}
	if want := zPageHdrLen + n*zFenceEntLen; len(p) != want {
		return nil, fmt.Errorf("sqlo1: zset leaf page of %d bytes, %d entries need %d", len(p), n, want)
	}
	dst = dst[:0]
	for i := range n {
		e := p[zPageHdrLen+i*zFenceEntLen:]
		sm := binary.LittleEndian.Uint64(e[8:16])
		ent := zFenceEnt{
			lo:    binary.LittleEndian.Uint64(e[:8]),
			segid: sm & hashFenceSegidMax,
			meta:  uint16(sm >> 48),
			count: binary.LittleEndian.Uint32(e[16:]),
		}
		sentinel := first && i == 0
		if sentinel {
			if ent.lo != 0 {
				return nil, fmt.Errorf("sqlo1: zset leaf page starts at %#x, want the 0 sentinel", ent.lo)
			}
		} else {
			if ent.lo == 0 {
				return nil, fmt.Errorf("sqlo1: zset leaf page entry %d separates at 0 past the sentinel", i)
			}
			if i > 0 && ent.lo < dst[i-1].lo {
				return nil, fmt.Errorf("sqlo1: zset leaf page out of order at entry %d", i)
			}
			if ent.count == 0 {
				return nil, fmt.Errorf("sqlo1: zset leaf page entry %d is empty, only the sentinel may be", i)
			}
		}
		if nextSegid != 0 && ent.segid >= nextSegid {
			return nil, fmt.Errorf("sqlo1: zset leaf page segid %d at or past the mint counter %d", ent.segid, nextSegid)
		}
		dst = append(dst, ent)
	}
	return dst, nil
}

// zidxSum totals an index slice's two subtree measures.
func zidxSum(f []zIdxEnt) (runs int, count uint64) {
	for i := range f {
		runs += int(f[i].runs)
		count += f[i].count
	}
	return runs, count
}

// zfenceSum totals a fence slice's live members.
func zfenceSum(f []zFenceEnt) uint64 {
	var count uint64
	for i := range f {
		count += uint64(f[i].count)
	}
	return count
}

// zloadTail decodes the current root's score section: the flat fence
// into z.zfence, or the paged root index into z.zridx with the page
// caches invalidated. Every score-side op starts here, so a stale
// loaded page can never serve across roots.
func (z *ZSet) zloadTail() error {
	p := z.h.segRoot.tail
	z.zui, z.zli = -1, -1
	if len(p) > 0 && p[0]&zflagFencePaged != 0 {
		var err error
		z.zridx, err = decodeZTailPaged(p, z.zridx)
		if err != nil {
			return err
		}
		z.zpaged = true
		z.zfence = z.zfence[:0]
		return nil
	}
	z.zpaged = false
	z.zridx = z.zridx[:0]
	var err error
	z.zfence, err = decodeZTail(p, z.zfence)
	return err
}

// readZPage reads the page record at pageid. The image aliases the
// read and dies on the next Tiered call, so decoders copy out.
func (z *ZSet) readZPage(ctx context.Context, pageid uint64) ([]byte, error) {
	putZPageKey(z.zkbuf2[:], z.h.segRoot.rooth, pageid)
	v, ok, err := z.h.t.Get(ctx, z.zkbuf2[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("sqlo1: zset fence page %d of rooth %#x is missing", pageid, z.h.segRoot.rooth)
	}
	return v, nil
}

// loadZUpper makes upper ui the loaded upper page, cross-checking the
// image against its root entry's separator and both subtree totals. A
// no-op when already loaded; the cache is one page per level, living
// within one op.
func (z *ZSet) loadZUpper(ctx context.Context, ui int) error {
	if z.zui == ui {
		return nil
	}
	e := z.zridx[ui]
	img, err := z.readZPage(ctx, e.pageid)
	if err != nil {
		return err
	}
	ents, err := decodeZUpperPage(img, z.zupper[:0], z.h.segRoot.nextSegid, ui == 0)
	if err != nil {
		return err
	}
	runs, count := zidxSum(ents)
	if ents[0].lo != e.lo || runs != int(e.runs) || count != e.count {
		return fmt.Errorf("sqlo1: zset upper page %d disagrees with its root entry (lo %#x/%#x, runs %d/%d, count %d/%d)",
			e.pageid, ents[0].lo, e.lo, runs, e.runs, count, e.count)
	}
	z.zupper = ents
	z.zui, z.zli = ui, -1
	return nil
}

// loadZLeaf makes leaf li of upper ui the loaded leaf: z.zfence
// becomes its entries, the shape every fence op already works on.
func (z *ZSet) loadZLeaf(ctx context.Context, ui, li int) error {
	if err := z.loadZUpper(ctx, ui); err != nil {
		return err
	}
	if z.zli == li {
		return nil
	}
	e := z.zupper[li]
	img, err := z.readZPage(ctx, e.pageid)
	if err != nil {
		return err
	}
	ents, err := decodeZLeafPage(img, z.zfence[:0], z.h.segRoot.nextSegid, ui == 0 && li == 0)
	if err != nil {
		return err
	}
	if ents[0].lo != e.lo || len(ents) != int(e.runs) || zfenceSum(ents) != e.count {
		return fmt.Errorf("sqlo1: zset leaf page %d disagrees with its upper entry (lo %#x/%#x, runs %d/%d, count %d/%d)",
			e.pageid, ents[0].lo, e.lo, len(ents), e.runs, zfenceSum(ents), e.count)
	}
	z.zfence = ents
	z.zli = li
	return nil
}

// writeZLeafPage lands the loaded leaf's current entries in place, an
// exact image under the same pageid, riding the command's batch
// beside the root.
func (z *ZSet) writeZLeafPage(ctx context.Context) error {
	z.zpbuf = appendZLeafPage(z.zpbuf[:0], z.zfence)
	return z.writeZPageRaw(ctx, z.zupper[z.zli].pageid, z.zpbuf)
}

// writeZUpperPage lands the loaded upper's current entries in place.
func (z *ZSet) writeZUpperPage(ctx context.Context) error {
	z.zpbuf = appendZUpperPage(z.zpbuf[:0], z.zupper)
	return z.writeZPageRaw(ctx, z.zridx[z.zui].pageid, z.zpbuf)
}

// writeZPageRaw writes a page payload under the current root's plane.
func (z *ZSet) writeZPageRaw(ctx context.Context, pageid uint64, payload []byte) error {
	putZPageKey(z.zkbuf2[:], z.h.segRoot.rooth, pageid)
	return z.h.t.SetGen(ctx, z.zkbuf2[:], payload, z.h.tag|TagFence, z.h.segRoot.rootgen)
}

// delZPage retires a replaced page record, always after the root that
// stopped referencing it; a crash between leaves a bounded orphan.
func (z *ZSet) delZPage(ctx context.Context, pageid uint64) error {
	putZPageKey(z.zkbuf2[:], z.h.segRoot.rooth, pageid)
	_, err := z.h.t.Del(ctx, z.zkbuf2[:])
	return err
}

// zbumpLoaded moves the loaded position's subtree member totals by d,
// the paged half of a fence count edit; flat mode has no totals.
func (z *ZSet) zbumpLoaded(d int64) {
	if !z.zpaged {
		return
	}
	z.zupper[z.zli].count = uint64(int64(z.zupper[z.zli].count) + d)
	z.zridx[z.zui].count = uint64(int64(z.zridx[z.zui].count) + d)
}

// zrunsBump moves the loaded position's subtree run counts by d, the
// bookkeeping of a run entry inserted into or removed from the loaded
// leaf.
func (z *ZSet) zrunsBump(d int) {
	if !z.zpaged {
		return
	}
	z.zupper[z.zli].runs = uint16(int(z.zupper[z.zli].runs) + d)
	z.zridx[z.zui].runs = uint16(int(z.zridx[z.zui].runs) + d)
}

// zfenceCommit lands a fence edit contained in the loaded leaf: the
// leaf and upper images in place, then the root, one batch. Flat mode
// is the root alone.
func (z *ZSet) zfenceCommit(ctx context.Context, key []byte) error {
	if z.zpaged {
		if err := z.writeZLeafPage(ctx); err != nil {
			return err
		}
		if err := z.writeZUpperPage(ctx); err != nil {
			return err
		}
	}
	return z.writeZRoot(ctx, key)
}

// zprefixLoaded is the member count of every run before the loaded
// leaf's entry ri: the three-level prefix sum a rank starts from.
func (z *ZSet) zprefixLoaded(ri int) int64 {
	var p int64
	if z.zpaged {
		for i := range z.zui {
			p += int64(z.zridx[i].count)
		}
		for i := range z.zli {
			p += int64(z.zupper[i].count)
		}
	}
	for i := range ri {
		p += int64(z.zfence[i].count)
	}
	return p
}

// zidxLastLE answers the last index entry with lo <= s, which exists
// for any legal sortable because the sentinel entry's lo is 0.
func zidxLastLE(f []zIdxEnt, s uint64) int {
	return sort.Search(len(f), func(i int) bool { return f[i].lo > s }) - 1
}

// zidxLastLT is zidxLastLE strict: the last entry with lo < s.
func zidxLastLT(f []zIdxEnt, s uint64) int {
	return sort.Search(len(f), func(i int) bool { return f[i].lo >= s }) - 1
}

// zseek loads the pages covering global run g and answers its index
// inside the loaded leaf.
func (z *ZSet) zseek(ctx context.Context, g int) (int, error) {
	ui := 0
	for ; ui < len(z.zridx); ui++ {
		if r := int(z.zridx[ui].runs); g < r {
			break
		} else {
			g -= r
		}
	}
	if ui == len(z.zridx) {
		return 0, fmt.Errorf("sqlo1: zset run position %d past the fence of rooth %#x", g, z.h.segRoot.rooth)
	}
	if err := z.loadZUpper(ctx, ui); err != nil {
		return 0, err
	}
	li := 0
	for ; li < len(z.zupper); li++ {
		if r := int(z.zupper[li].runs); g < r {
			break
		} else {
			g -= r
		}
	}
	if err := z.loadZLeaf(ctx, ui, li); err != nil {
		return 0, err
	}
	return g, nil
}

// zentGlobal answers the fence entry of global run g, loading its
// pages.
func (z *ZSet) zentGlobal(ctx context.Context, g int) (zFenceEnt, error) {
	ei, err := z.zseek(ctx, g)
	if err != nil {
		return zFenceEnt{}, err
	}
	return z.zfence[ei], nil
}

// zlastLE descends to the last run with separator <= s and answers
// its global index, the paged half of the fence binary search.
func (z *ZSet) zlastLE(ctx context.Context, s uint64) (int, error) {
	ui := zidxLastLE(z.zridx, s)
	g := 0
	for i := range ui {
		g += int(z.zridx[i].runs)
	}
	if err := z.loadZUpper(ctx, ui); err != nil {
		return 0, err
	}
	li := zidxLastLE(z.zupper, s)
	for i := range li {
		g += int(z.zupper[i].runs)
	}
	if err := z.loadZLeaf(ctx, ui, li); err != nil {
		return 0, err
	}
	ei := sort.Search(len(z.zfence), func(i int) bool { return z.zfence[i].lo > s }) - 1
	return g + ei, nil
}

// zfirstGE descends to the first run with separator >= s, or the run
// count when none has: the equal-separator chain's low bound. The
// descent follows the last subtree with lo < s, whose interior may
// still hold separators at s; when the leaf runs out the answer is
// the next subtree's first run, which the global index arithmetic
// yields for free. An s at or below the sentinel separator has no
// such subtree and descends the first one, where the answer is run 0.
func (z *ZSet) zfirstGE(ctx context.Context, s uint64) (int, error) {
	ui := max(zidxLastLT(z.zridx, s), 0)
	g := 0
	for i := range ui {
		g += int(z.zridx[i].runs)
	}
	if err := z.loadZUpper(ctx, ui); err != nil {
		return 0, err
	}
	li := max(zidxLastLT(z.zupper, s), 0)
	for i := range li {
		g += int(z.zupper[i].runs)
	}
	if err := z.loadZLeaf(ctx, ui, li); err != nil {
		return 0, err
	}
	ei := sort.Search(len(z.zfence), func(i int) bool { return z.zfence[i].lo >= s })
	return g + ei, nil
}

// zaddRoom is the dual command's capacity probe: with the root index
// full, an add whose covering upper and leaf are also full and whose
// run image would split has nowhere to put the new run, and the
// command must fail before either family writes. Anywhere short of
// that exact corner answers nil.
func (z *ZSet) zaddRoom(ctx context.Context, s uint64, member []byte) error {
	if !z.zpaged || len(z.zridx) < zFenceRootMax {
		return nil
	}
	ri, err := z.zrunRoute(ctx, s, member)
	if err != nil {
		return err
	}
	if len(z.zupper) < zFenceUpperMax || len(z.zfence) < zFenceLeafMax {
		return nil
	}
	e := z.zfence[ri]
	if e.count == 0 {
		return nil
	}
	img, err := z.readRun(ctx, e.segid)
	if err != nil {
		return err
	}
	_, found, liveEnd, err := zrunPos(img, e.count, s, member)
	if err != nil || found {
		return err
	}
	if liveEnd+zRunEntHdrLen+len(member) > zRunMax {
		return errZFenceThirdLevel
	}
	return nil
}
