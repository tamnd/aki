package sqlo1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
)

// The score side of the doc 09 zset: runs of (score, member)-sorted
// entries under subkey kind 2, fenced by an array in the zset root's
// tail section with an exact per-run count. The member side answers
// ZSCORE; this side answers everything ordered: ranks are prefix sums
// over the fence counts, ranges are fence-guided run scans.
//
// The fence separator is the sortable score alone, 20 bytes per run,
// which cannot distinguish adjacent runs that both start inside one
// score (a split in the middle of a lex-shaped zset, where every
// member shares the score). Routing resolves that chain by reading
// first entries: a run's first live entry is always at the head of
// its image (every rewrite drops dead bytes from the front), so a
// binary search over the chain costs log2(chain) point reads and
// nothing when scores are distinct, the common case. The alternative
// of refusing to split inside a score would make a lex zset one
// unbounded run.
//
// Separator invariant: every entry of run k-1 orders at or below
// sep_k, every entry of run k at or above it. Splits cut at the
// median entry and stamp its score as the new separator, which the
// invariant admits even mid-score; head deletes and inserts keep it
// because a separator only ever sits at or below its run's first
// entry. The first run's separator is 0, below every legal sortable
// score (NaN is rejected at the command layer and the transform maps
// no legal double to 0), and that sentinel run never dies: emptied it
// keeps a count of 0 and its record goes stale, count-clipped by
// every reader.
//
// The fence count is the live-entry authority everywhere: a run image
// may carry dead bytes past the counted region (an untrimmed split
// survivor), and every reader stops at the count. Write order inside
// one op mirrors splitSeg: a freshly minted run lands and flushes
// before the root that references it; everything else rides one
// batch with the root, atomic at the store seam.

const (
	// zRunKind is the subkey kind of score-run records; doc 09 gives
	// kind 4 to score fence pages, a later slice.
	zRunKind = 2

	// Run split threshold and lazy-merge floor in encoded bytes, the
	// hsegz lab verdict (#955): 4032 is a real knee, 2016 doubles the
	// fence under every rank prefix-sum and 8064 carries 45 percent
	// more WAL bytes per score move.
	zRunMax = 4032
	zRunMin = 1024

	// zRunHdrLen is the run image header: u16 entry count (advisory,
	// the fence count clips it), u16 zero.
	zRunHdrLen = 4

	// zRunEntHdrLen precedes each entry's member bytes: u16 mlen,
	// then the 8-byte big-endian sortable score, the same image the
	// member side stores as its value.
	zRunEntHdrLen = 10

	// The root tail: u8 zflags, u8 zero, u16 run count, then the
	// fence entries. 20 bytes per run holds roughly a hundred runs
	// before the tail needs its own paging ladder, doc 09's ~100.
	zTailHdrLen   = 4
	zFenceEntLen  = 20
	zFenceMaxRuns = 100

	// zflagFencePaged flips when the score fence moves to kind 4
	// pages; reserved until that slice lands.
	zflagFencePaged = 1 << 0
)

// errZFenceFull is the standing edge of the flat score fence: past
// zFenceMaxRuns the fence needs its paging slice, and until that
// lands the insert that would split fails loudly and leaves the zset
// untouched.
var errZFenceFull = errors.New("sqlo1: zset score fence is full, fence paging is a later slice")

// zFenceEnt is one decoded score-fence entry: the sortable-score
// separator (0 on the sentinel first run), the run's segid, reserved
// meta, and the exact live-entry count rank arithmetic sums.
type zFenceEnt struct {
	lo    uint64
	segid uint64
	meta  uint16
	count uint32
}

// putZRunKey writes the subkey of score run segid under rooth: the
// doc 03 6.3 layout with kind zRunKind. Run segids mint from the same
// root counter as member segids; the kind byte keeps the planes
// apart.
func putZRunKey(dst []byte, rooth, segid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = zRunKind
	var seg [8]byte
	binary.LittleEndian.PutUint64(seg[:], segid)
	copy(dst[9:SubkeySize], seg[:7])
}

// appendZTail encodes the score section of a zset root.
func appendZTail(dst []byte, f []zFenceEnt) []byte {
	var hdr [zTailHdrLen]byte
	binary.LittleEndian.PutUint16(hdr[2:], uint16(len(f)))
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

// decodeZTail parses a zset root's tail into dst[:0]. An empty tail
// is a zset whose score side has not been built yet and decodes to no
// runs.
func decodeZTail(p []byte, dst []zFenceEnt) ([]zFenceEnt, error) {
	dst = dst[:0]
	if len(p) == 0 {
		return dst, nil
	}
	if len(p) < zTailHdrLen {
		return nil, fmt.Errorf("sqlo1: zset score section of %d bytes, header needs %d", len(p), zTailHdrLen)
	}
	if p[0]&zflagFencePaged != 0 {
		return nil, errors.New("sqlo1: zset score fence claims paged mode, which no slice writes yet")
	}
	if p[0]&^byte(zflagFencePaged) != 0 || p[1] != 0 {
		return nil, fmt.Errorf("sqlo1: zset score section flags %#x unknown", p[0])
	}
	n := int(binary.LittleEndian.Uint16(p[2:]))
	if n == 0 {
		return nil, errors.New("sqlo1: zset score section with a header but no runs")
	}
	if want := zTailHdrLen + n*zFenceEntLen; len(p) != want {
		return nil, fmt.Errorf("sqlo1: zset score section of %d bytes, %d fence entries need %d", len(p), n, want)
	}
	for i := range n {
		e := p[zTailHdrLen+i*zFenceEntLen:]
		sm := binary.LittleEndian.Uint64(e[8:16])
		ent := zFenceEnt{
			lo:    binary.LittleEndian.Uint64(e[:8]),
			segid: sm & hashFenceSegidMax,
			meta:  uint16(sm >> 48),
			count: binary.LittleEndian.Uint32(e[16:]),
		}
		if i == 0 && ent.lo != 0 {
			return nil, fmt.Errorf("sqlo1: zset score fence starts at %#x, want the 0 sentinel", ent.lo)
		}
		if i > 0 && ent.lo < dst[i-1].lo {
			return nil, fmt.Errorf("sqlo1: zset score fence out of order at entry %d", i)
		}
		if i > 0 && ent.count == 0 {
			return nil, fmt.Errorf("sqlo1: zset score fence entry %d is empty, only the sentinel may be", i)
		}
		dst = append(dst, ent)
	}
	return dst, nil
}

// appendZRunEnt appends one run entry: u16 mlen, the big-endian
// sortable score, the member.
func appendZRunEnt(dst []byte, sortable uint64, member []byte) []byte {
	var hdr [zRunEntHdrLen]byte
	binary.LittleEndian.PutUint16(hdr[:2], uint16(len(member)))
	binary.BigEndian.PutUint64(hdr[2:], sortable)
	dst = append(dst, hdr[:]...)
	return append(dst, member...)
}

// zRunEntAt decodes the entry at off, answering its sortable score,
// member (aliasing p), and the next entry's offset.
func zRunEntAt(p []byte, off int) (uint64, []byte, int, error) {
	if off+zRunEntHdrLen > len(p) {
		return 0, nil, 0, fmt.Errorf("sqlo1: zset run entry header at %d overruns %d bytes", off, len(p))
	}
	mlen := int(binary.LittleEndian.Uint16(p[off:]))
	s := binary.BigEndian.Uint64(p[off+2:])
	next := off + zRunEntHdrLen + mlen
	if next > len(p) {
		return 0, nil, 0, fmt.Errorf("sqlo1: zset run entry at %d claims %d member bytes past the image", off, mlen)
	}
	return s, p[off+zRunEntHdrLen : next], next, nil
}

// putZRunHdr stamps a run image header over dst[:zRunHdrLen].
func putZRunHdr(dst []byte, n int) {
	binary.LittleEndian.PutUint16(dst, uint16(n))
	dst[2], dst[3] = 0, 0
}

// zrunPos walks a run image's live region for (s, member): the byte
// offset the pair sits at or inserts at, whether it was found, and
// the offset just past the counted region.
func zrunPos(img []byte, count uint32, s uint64, member []byte) (pos int, found bool, liveEnd int, err error) {
	off := zRunHdrLen
	pos = -1
	for i := uint32(0); i < count; i++ {
		es, em, next, err := zRunEntAt(img, off)
		if err != nil {
			return 0, false, 0, err
		}
		if pos < 0 && (es > s || (es == s && bytes.Compare(em, member) >= 0)) {
			pos = off
			found = es == s && bytes.Equal(em, member)
		}
		off = next
	}
	if pos < 0 {
		pos = off
	}
	return pos, found, off, nil
}

// zscoreState reads key, requires the segmented rung, and decodes the
// score fence into z.zfence. The inline rung keeps its pairs in the
// root and has no runs; slice 4's upgrade builds both families, so an
// inline root here is a caller error.
func (z *ZSet) zscoreState(ctx context.Context, key []byte) (int64, error) {
	st, _, expMs, err := z.h.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	if st != hashSegState {
		return 0, fmt.Errorf("sqlo1: zset score runs need a segmented root, key is %v", st)
	}
	z.zfence, err = decodeZTail(z.h.segRoot.tail, z.zfence)
	return expMs, err
}

// readRun reads the run record at segid. The image aliases the read
// and dies on the next Tiered call.
func (z *ZSet) readRun(ctx context.Context, segid uint64) ([]byte, error) {
	putZRunKey(z.zkbuf[:], z.h.segRoot.rooth, segid)
	v, ok, err := z.h.t.Get(ctx, z.zkbuf[:])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("sqlo1: zset run %d of rooth %#x is missing", segid, z.h.segRoot.rooth)
	}
	return v, nil
}

// writeRun writes a run image under the current root's plane.
func (z *ZSet) writeRun(ctx context.Context, segid uint64, img []byte) error {
	putZRunKey(z.zkbuf[:], z.h.segRoot.rooth, segid)
	return z.h.t.SetGen(ctx, z.zkbuf[:], img, z.h.tag, z.h.segRoot.rootgen)
}

// writeZRoot re-encodes the score fence into the root tail and lands
// the root. Score fence edits are never W2 delta-safe: replay
// reconciliation cannot rebuild exact run counts from skipped frames,
// so the root frame is always its own.
func (z *ZSet) writeZRoot(ctx context.Context, key []byte) error {
	z.ztail = appendZTail(z.ztail[:0], z.zfence)
	z.h.segRoot.tail = z.ztail
	return z.h.writeSegRoot(ctx, key, false)
}

// runFirst reads a run's first live entry, the routing key of an
// equal-separator chain. The member aliases the read.
func (z *ZSet) runFirst(ctx context.Context, e zFenceEnt) (uint64, []byte, error) {
	if e.count == 0 {
		return 0, nil, fmt.Errorf("sqlo1: zset run %d is empty inside a separator chain", e.segid)
	}
	img, err := z.readRun(ctx, e.segid)
	if err != nil {
		return 0, nil, err
	}
	s, m, _, err := zRunEntAt(img, zRunHdrLen)
	return s, m, err
}

// zrunRoute answers the fence index of the run covering (s, member).
// Distinct scores resolve on the fence alone; a chain of runs sharing
// the separator binary-searches their first entries, log2(chain)
// point reads.
func (z *ZSet) zrunRoute(ctx context.Context, s uint64, member []byte) (int, error) {
	f := z.zfence
	j := sort.Search(len(f), func(i int) bool { return f[i].lo > s }) - 1
	k := sort.Search(len(f), func(i int) bool { return f[i].lo >= s })
	if k > j {
		return j, nil
	}
	ans, lo, hi := k-1, k, j
	for lo <= hi {
		mid := (lo + hi) / 2
		fs, fm, err := z.runFirst(ctx, f[mid])
		if err != nil {
			return 0, err
		}
		if fs < s || (fs == s && bytes.Compare(fm, member) <= 0) {
			ans, lo = mid, mid+1
		} else {
			hi = mid - 1
		}
	}
	return ans, nil
}

// zrunAdd inserts (score, member) into the score side, reporting
// whether the pair was absent. Slice 4's ZADD pairs this with the
// member write in one frame group; alone it maintains only the score
// family.
func (z *ZSet) zrunAdd(ctx context.Context, key []byte, score float64, member []byte) (bool, error) {
	expMs, err := z.zscoreState(ctx, key)
	if err != nil {
		return false, err
	}
	h := z.h
	r := &h.segRoot
	s := zScoreSortable(score)

	if len(z.zfence) == 0 {
		if r.nextSegid > hashFenceSegidMax {
			return false, fmt.Errorf("sqlo1: zset segid space of rooth %#x is spent", r.rooth)
		}
		segid := r.nextSegid
		z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
		putZRunHdr(z.zrbuf, 1)
		z.zrbuf = appendZRunEnt(z.zrbuf, s, member)
		if err := z.writeRun(ctx, segid, z.zrbuf); err != nil {
			return false, err
		}
		if err := h.t.Flush(ctx); err != nil {
			return false, err
		}
		r.nextSegid++
		z.zfence = append(z.zfence[:0], zFenceEnt{segid: segid, count: 1})
		if err := z.writeZRoot(ctx, key); err != nil {
			return false, err
		}
		return true, h.restamp(ctx, key, expMs)
	}

	ri, err := z.zrunRoute(ctx, s, member)
	if err != nil {
		return false, err
	}
	f := z.zfence
	e := &f[ri]
	n := int(e.count) + 1

	if e.count == 0 {
		// The emptied sentinel: its stale image is dead bytes, the
		// fresh image starts over.
		z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
		putZRunHdr(z.zrbuf, 1)
		z.zrbuf = appendZRunEnt(z.zrbuf, s, member)
	} else {
		img, err := z.readRun(ctx, e.segid)
		if err != nil {
			return false, err
		}
		pos, found, liveEnd, err := zrunPos(img, e.count, s, member)
		if err != nil {
			return false, err
		}
		if found {
			return false, nil
		}
		z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
		putZRunHdr(z.zrbuf, n)
		z.zrbuf = append(z.zrbuf, img[zRunHdrLen:pos]...)
		z.zrbuf = appendZRunEnt(z.zrbuf, s, member)
		z.zrbuf = append(z.zrbuf, img[pos:liveEnd]...)
	}

	if len(z.zrbuf) > zRunMax && n >= 2 {
		if err := z.zrunSplit(ctx, key, ri, n); err != nil {
			return false, err
		}
		return true, h.restamp(ctx, key, expMs)
	}
	if err := z.writeRun(ctx, e.segid, z.zrbuf); err != nil {
		return false, err
	}
	e.count++
	if err := z.writeZRoot(ctx, key); err != nil {
		return false, err
	}
	return true, h.restamp(ctx, key, expMs)
}

// zrunSplit cuts the oversize post-insert image in z.zrbuf at its
// median entry, splitSeg's write order: the new high run lands and
// flushes before the root that routes to it, the trimmed low image
// rides the root's batch as dead bytes until it lands.
func (z *ZSet) zrunSplit(ctx context.Context, key []byte, ri, n int) error {
	if len(z.zfence) >= zFenceMaxRuns {
		return errZFenceFull
	}
	h := z.h
	r := &h.segRoot
	if r.nextSegid > hashFenceSegidMax {
		return fmt.Errorf("sqlo1: zset segid space of rooth %#x is spent", r.rooth)
	}
	mid := n / 2
	off := zRunHdrLen
	var sep uint64
	for i := 0; i < mid; i++ {
		_, _, next, err := zRunEntAt(z.zrbuf, off)
		if err != nil {
			return err
		}
		off = next
	}
	sep, _, _, err := zRunEntAt(z.zrbuf, off)
	if err != nil {
		return err
	}

	newSegid := r.nextSegid
	z.zrbuf2 = append(z.zrbuf2[:0], make([]byte, zRunHdrLen)...)
	putZRunHdr(z.zrbuf2, n-mid)
	z.zrbuf2 = append(z.zrbuf2, z.zrbuf[off:]...)
	if err := z.writeRun(ctx, newSegid, z.zrbuf2); err != nil {
		return err
	}
	if err := h.t.Flush(ctx); err != nil {
		return err
	}
	r.nextSegid++

	z.zfence[ri].count = uint32(mid)
	z.zfence = slices.Insert(z.zfence, ri+1, zFenceEnt{lo: sep, segid: newSegid, count: uint32(n - mid)})
	if err := z.writeZRoot(ctx, key); err != nil {
		return err
	}
	putZRunHdr(z.zrbuf, mid)
	return z.writeRun(ctx, z.zfence[ri].segid, z.zrbuf[:off])
}

// zrunDel removes (score, member) from the score side, reporting
// whether it was present. An emptied run dies whole, root first, then
// the record (a crash between leaves a bounded orphan the plane
// retire cleans); the sentinel stays at count 0. A shrunken run folds
// into a neighbor when the merged image stays under zRunMin, the
// member family's lazy-merge rule.
func (z *ZSet) zrunDel(ctx context.Context, key []byte, score float64, member []byte) (bool, error) {
	expMs, err := z.zscoreState(ctx, key)
	if err != nil {
		return false, err
	}
	if len(z.zfence) == 0 {
		return false, nil
	}
	s := zScoreSortable(score)
	ri, err := z.zrunRoute(ctx, s, member)
	if err != nil {
		return false, err
	}
	f := z.zfence
	e := &f[ri]
	if e.count == 0 {
		return false, nil
	}
	img, err := z.readRun(ctx, e.segid)
	if err != nil {
		return false, err
	}
	pos, found, liveEnd, err := zrunPos(img, e.count, s, member)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	_, _, entEnd, err := zRunEntAt(img, pos)
	if err != nil {
		return false, err
	}
	n := int(e.count) - 1
	z.zrbuf = append(z.zrbuf[:0], make([]byte, zRunHdrLen)...)
	putZRunHdr(z.zrbuf, n)
	z.zrbuf = append(z.zrbuf, img[zRunHdrLen:pos]...)
	z.zrbuf = append(z.zrbuf, img[entEnd:liveEnd]...)

	if n == 0 {
		if ri == 0 {
			e.count = 0
			if err := z.writeZRoot(ctx, key); err != nil {
				return false, err
			}
			return true, z.h.restamp(ctx, key, expMs)
		}
		deadSegid := e.segid
		z.zfence = append(f[:ri], f[ri+1:]...)
		if err := z.writeZRoot(ctx, key); err != nil {
			return false, err
		}
		putZRunKey(z.zkbuf[:], z.h.segRoot.rooth, deadSegid)
		if _, err := z.h.t.Del(ctx, z.zkbuf[:]); err != nil {
			return false, err
		}
		return true, z.h.restamp(ctx, key, expMs)
	}

	merged, err := z.tryMergeRun(ctx, key, ri, n)
	if err != nil {
		return false, err
	}
	if merged {
		return true, z.h.restamp(ctx, key, expMs)
	}
	if err := z.writeRun(ctx, e.segid, z.zrbuf); err != nil {
		return false, err
	}
	e.count--
	if err := z.writeZRoot(ctx, key); err != nil {
		return false, err
	}
	return true, z.h.restamp(ctx, key, expMs)
}

// tryMergeRun folds the shrunken run at ri (post-image in z.zrbuf, n
// live entries) into a fence neighbor when the merged encoding stays
// under zRunMin: merged image to the low side's segid, the root drops
// the high entry, then the high record dies, tryMergeSeg's order.
func (z *ZSet) tryMergeRun(ctx context.Context, key []byte, ri, n int) (bool, error) {
	if len(z.zrbuf) >= zRunMin {
		return false, nil
	}
	f := z.zfence
	try := func(lo, hi int) (bool, error) {
		if lo < 0 || hi >= len(f) {
			return false, nil
		}
		other := lo
		if other == ri {
			other = hi
		}
		if f[other].count == 0 {
			return false, nil
		}
		img, err := z.readRun(ctx, f[other].segid)
		if err != nil {
			return false, err
		}
		_, _, otherEnd, err := zrunPos(img, f[other].count, 0, nil)
		if err != nil {
			return false, err
		}
		if len(z.zrbuf)+otherEnd-zRunHdrLen >= zRunMin {
			return false, nil
		}
		z.zrbuf2 = append(z.zrbuf2[:0], make([]byte, zRunHdrLen)...)
		putZRunHdr(z.zrbuf2, n+int(f[other].count))
		if other == lo {
			z.zrbuf2 = append(z.zrbuf2, img[zRunHdrLen:otherEnd]...)
			z.zrbuf2 = append(z.zrbuf2, z.zrbuf[zRunHdrLen:]...)
		} else {
			z.zrbuf2 = append(z.zrbuf2, z.zrbuf[zRunHdrLen:]...)
			z.zrbuf2 = append(z.zrbuf2, img[zRunHdrLen:otherEnd]...)
		}
		if err := z.writeRun(ctx, f[lo].segid, z.zrbuf2); err != nil {
			return false, err
		}
		deadSegid := f[hi].segid
		f[lo].count = uint32(n) + f[other].count
		z.zfence = append(f[:hi], f[hi+1:]...)
		if err := z.writeZRoot(ctx, key); err != nil {
			return false, err
		}
		putZRunKey(z.zkbuf[:], z.h.segRoot.rooth, deadSegid)
		if _, err := z.h.t.Del(ctx, z.zkbuf[:]); err != nil {
			return false, err
		}
		return true, nil
	}
	if done, err := try(ri, ri+1); done || err != nil {
		return done, err
	}
	return try(ri-1, ri)
}

// zrunWalk streams every score-side entry of key in (score, member)
// order, one run read at a time. Emitted bytes alias the read and die
// on the next Tiered call. The future range family streams the same
// walk with a fence-guided start.
func (z *ZSet) zrunWalk(ctx context.Context, key []byte, emit func(sortable uint64, member []byte)) error {
	if _, err := z.zscoreState(ctx, key); err != nil {
		return err
	}
	for i := range z.zfence {
		e := z.zfence[i]
		if e.count == 0 {
			continue
		}
		img, err := z.readRun(ctx, e.segid)
		if err != nil {
			return err
		}
		off := zRunHdrLen
		for j := uint32(0); j < e.count; j++ {
			s, m, next, err := zRunEntAt(img, off)
			if err != nil {
				return err
			}
			emit(s, m)
			off = next
		}
	}
	return nil
}
