package sqlo1

// The segmented hash root and its fence, doc 06 sections 2.2 and 2.3:
// the root payload carries the exact count, the segid mint counter,
// and the fence table mapping fh ranges to segment records. Ranges
// are sorted by lo and cover the full u64 space (the first lo is
// always 0); the covering segment for an fh is a binary search.
//
// Every planed root layout (sub 1..15) shares the same prefix:
// byte 0 the sub, rootgen at offset 4, rooth at offset 8. rope.go
// lays its root out the same way, and planedRootInfo reads the
// common prefix so a cross-type overwrite can retire any plane
// without knowing the type's full layout.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// errHashFenceThirdLevel marks the end of the two-level fence, doc 06
// section 2.3: a paged fence whose page index and every page are full
// addresses hashFencePageIdxMax*hashFencePageMax segments, on the
// order of 10^9 fields, and a hash that outgrows it is out of the
// format's design envelope. The write that would need a third level
// fails loudly before touching anything.
var errHashFenceThirdLevel = errors.New("sqlo1: hash fence page index is full, the two-level fence is spent")

const (
	// hashSegRootHdrLen is the fixed part of the segmented root
	// payload: sub, hflags, reserved, rootgen, rooth, count,
	// next_segid, min_expire_ms, seg_count.
	hashSegRootHdrLen = 44

	// hashFenceEntLen is one fence entry: u64 lo, then 48 bits of
	// segid under 16 bits of per-segment metadata.
	hashFenceEntLen = 16

	// hashFenceMaxSegs is fence_inline_max (2048 B) over the entry
	// size: the most segments an inline fence addresses before the
	// paging transition.
	hashFenceMaxSegs = 2048 / hashFenceEntLen

	// hashFenceSegidMax is the 48-bit segid ceiling a fence entry can
	// hold, tighter than the subkey's 56 bits. Page ids mint from the
	// same counter, so it bounds them too.
	hashFenceSegidMax = 1<<48 - 1

	// hflagFencePaged marks a root whose fence lives in rtype 5
	// pages: the root's entry area holds the page index instead of
	// fence entries, and seg_count counts index entries.
	hflagFencePaged = 1 << 0

	// hashFencePageHdrLen is the fixed part of a fence page payload:
	// u16 entry count, u16 reserved.
	hashFencePageHdrLen = 4

	// hashFencePageMax is the most fence entries one page holds
	// (about 4 KB encoded, one seg-sized record); the page past it
	// splits.
	hashFencePageMax = 250

	// hashFencePageIdxMax is the most pages the root's index holds,
	// the same 4 KB yardstick. Past it is errHashFenceThirdLevel.
	hashFencePageIdxMax = 250
)

// Fence entry metadata, the 16 bits above the segid: an approximate
// fill class in the low nibble (entry count / 16, clamped) for
// HRANDFIELD's weighted sampling, and a has-TTL bit. Both are
// advisory and recomputable from the segment; they refresh on the
// root writes that pass by anyway.
const (
	hashMetaFillMask = 0xF
	hashMetaHasTTL   = 1 << 4
)

// hashSegMeta derives a fence entry's metadata from the segment
// image it was just encoded from.
func hashSegMeta(n int, minExpMs int64) uint16 {
	m := uint16(min(n/16, int(hashMetaFillMask)))
	if minExpMs != 0 {
		m |= hashMetaHasTTL
	}
	return m
}

// hashFenceEnt is one decoded fence entry: the range start and the
// owning segment. hi of entry i is lo of entry i+1.
type hashFenceEnt struct {
	lo    uint64
	segid uint64
	meta  uint16
}

// hashPageEnt is one decoded page-index entry of a paged root: the
// range start of the page's first fence entry, the page record's id,
// and the page's summed fill-class weight (2*fillclass+1 over its
// entries) so HRANDFIELD can draw a page without loading it. hi of
// page j is lo of page j+1.
type hashPageEnt struct {
	lo     uint64
	pageid uint64
	weight uint16
}

// hashPageWeight sums a page's draw weights. 250 entries at the max
// class weigh 7750, comfortably inside the u16 the index entry holds.
func hashPageWeight(ents []hashFenceEnt) uint16 {
	w := uint16(0)
	for _, e := range ents {
		w += 2*uint16(e.meta&hashMetaFillMask) + 1
	}
	return w
}

// hashSegRoot is the decoded segmented root. The fence and pidx
// slices are built fresh on decode (they do not alias the payload),
// so the struct stays valid across the segment reads an operator does
// next. Flat mode: fence holds the whole fence and pidx is empty.
// Paged mode: pidx holds the page index, and fence holds the one
// loaded page's entries (pi is its index, -1 until loadPage runs).
type hashSegRoot struct {
	sub       uint8
	rootgen   uint32
	rooth     uint64
	count     uint64
	nextSegid uint64
	minExpMs  int64
	fence     []hashFenceEnt
	paged     bool
	pidx      []hashPageEnt
	pi        int

	// tail is the bytes past the member-side fence area, legal only
	// on a zset root: doc 09 hangs the score-run fence there, opaque
	// to the member-side machinery, which decodes it out and writes it
	// back untouched. On decode it aliases the payload (stateOf copies
	// it out before the next read); hash and set roots reject a tail.
	tail []byte
}

// appendHashSegRoot encodes r onto dst[:0]. A paged root writes the
// page index into the entry area; its fence entries live in page
// records the caller lands separately.
func appendHashSegRoot(dst []byte, r *hashSegRoot) []byte {
	out := grow(dst, hashSegRootHdrLen)
	out[0] = r.sub
	out[1] = 0
	if r.minExpMs != 0 {
		out[1] = hflagAnyTTL
	}
	if r.paged {
		out[1] |= hflagFencePaged
	}
	out[2] = 0
	out[3] = 0
	binary.LittleEndian.PutUint32(out[4:], r.rootgen)
	binary.LittleEndian.PutUint64(out[8:], r.rooth)
	binary.LittleEndian.PutUint64(out[16:], r.count)
	binary.LittleEndian.PutUint64(out[24:], r.nextSegid)
	binary.LittleEndian.PutUint64(out[32:], uint64(r.minExpMs))
	if r.paged {
		binary.LittleEndian.PutUint32(out[40:], uint32(len(r.pidx)))
		for _, e := range r.pidx {
			var b [hashFenceEntLen]byte
			binary.LittleEndian.PutUint64(b[:], e.lo)
			binary.LittleEndian.PutUint64(b[8:], e.pageid|uint64(e.weight)<<48)
			out = append(out, b[:]...)
		}
		return append(out, r.tail...)
	}
	binary.LittleEndian.PutUint32(out[40:], uint32(len(r.fence)))
	for _, e := range r.fence {
		var b [hashFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[:], e.lo)
		binary.LittleEndian.PutUint64(b[8:], e.segid|uint64(e.meta)<<48)
		out = append(out, b[:]...)
	}
	return append(out, r.tail...)
}

// decodeHashSegRoot parses and validates a segmented root payload,
// building the fence (flat) or page index (paged) into the scratch
// slices. Everything checkable without touching segments is checked
// here; a paged root's fence entries live in page records loadPage
// checks on read.
func decodeHashSegRoot(p []byte, fence []hashFenceEnt, pidx []hashPageEnt) (hashSegRoot, error) {
	if len(p) < hashSegRootHdrLen {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root of %d bytes, header needs %d", len(p), hashSegRootHdrLen)
	}
	if p[0] != hashSubSeg && p[0] != setSubSeg && p[0] != zsetSubSeg {
		return hashSegRoot{}, fmt.Errorf("sqlo1: root sub %d is not a segmented hash, set, or zset", p[0])
	}
	if p[1]&^uint8(hflagAnyTTL|hflagFencePaged) != 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash flags %#x has reserved bits set", p[1])
	}
	if p[2] != 0 || p[3] != 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root reserved bytes are set")
	}
	r := hashSegRoot{
		sub:       p[0],
		rootgen:   binary.LittleEndian.Uint32(p[4:]),
		rooth:     binary.LittleEndian.Uint64(p[8:]),
		count:     binary.LittleEndian.Uint64(p[16:]),
		nextSegid: binary.LittleEndian.Uint64(p[24:]),
		minExpMs:  int64(binary.LittleEndian.Uint64(p[32:])),
		paged:     p[1]&hflagFencePaged != 0,
		pi:        -1,
	}
	segCount := int(binary.LittleEndian.Uint32(p[40:]))
	if r.rootgen == 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root with generation zero")
	}
	if r.count == 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root with zero fields")
	}
	if (p[1]&hflagAnyTTL != 0) != (r.minExpMs != 0) {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash TTL flag disagrees with min_expire %d", r.minExpMs)
	}
	if r.paged {
		if segCount == 0 || segCount > hashFencePageIdxMax {
			return hashSegRoot{}, fmt.Errorf("sqlo1: paged hash index of %d entries outside [1, %d]", segCount, hashFencePageIdxMax)
		}
		want := hashSegRootHdrLen + segCount*hashFenceEntLen
		if r.sub == zsetSubSeg {
			if len(p) < want {
				return hashSegRoot{}, fmt.Errorf("sqlo1: paged zset root of %d bytes, %d index entries need %d", len(p), segCount, want)
			}
			r.tail = p[want:]
		} else if len(p) != want {
			return hashSegRoot{}, fmt.Errorf("sqlo1: paged hash root of %d bytes, %d index entries need %d", len(p), segCount, want)
		}
		pidx = pidx[:0]
		for i := range segCount {
			off := hashSegRootHdrLen + i*hashFenceEntLen
			pw := binary.LittleEndian.Uint64(p[off+8:])
			e := hashPageEnt{
				lo:     binary.LittleEndian.Uint64(p[off:]),
				pageid: pw & hashFenceSegidMax,
				weight: uint16(pw >> 48),
			}
			if i == 0 {
				if e.lo != 0 {
					return hashSegRoot{}, fmt.Errorf("sqlo1: hash page index starts at %#x, must cover from 0", e.lo)
				}
			} else if e.lo <= pidx[i-1].lo {
				return hashSegRoot{}, fmt.Errorf("sqlo1: hash page index out of order at entry %d", i)
			}
			if e.pageid >= r.nextSegid {
				return hashSegRoot{}, fmt.Errorf("sqlo1: hash page id %d at or past the mint counter %d", e.pageid, r.nextSegid)
			}
			if e.weight == 0 {
				return hashSegRoot{}, fmt.Errorf("sqlo1: hash page %d with weight zero, a page holds at least one entry", e.pageid)
			}
			pidx = append(pidx, e)
		}
		r.pidx = pidx
		r.fence = fence[:0]
		return r, nil
	}
	r.pidx = pidx[:0]
	if segCount == 0 || segCount > hashFenceMaxSegs {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash fence of %d entries outside [1, %d]", segCount, hashFenceMaxSegs)
	}
	want := hashSegRootHdrLen + segCount*hashFenceEntLen
	if r.sub == zsetSubSeg {
		if len(p) < want {
			return hashSegRoot{}, fmt.Errorf("sqlo1: segmented zset root of %d bytes, %d fence entries need %d", len(p), segCount, want)
		}
		r.tail = p[want:]
	} else if len(p) != want {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root of %d bytes, %d fence entries need %d", len(p), segCount, want)
	}
	fence = fence[:0]
	for i := range segCount {
		off := hashSegRootHdrLen + i*hashFenceEntLen
		sm := binary.LittleEndian.Uint64(p[off+8:])
		e := hashFenceEnt{
			lo:    binary.LittleEndian.Uint64(p[off:]),
			segid: sm & hashFenceSegidMax,
			meta:  uint16(sm >> 48),
		}
		if i == 0 {
			if e.lo != 0 {
				return hashSegRoot{}, fmt.Errorf("sqlo1: hash fence starts at %#x, must cover from 0", e.lo)
			}
		} else if e.lo <= fence[i-1].lo {
			return hashSegRoot{}, fmt.Errorf("sqlo1: hash fence out of order at entry %d", i)
		}
		if e.segid >= r.nextSegid {
			return hashSegRoot{}, fmt.Errorf("sqlo1: hash fence segid %d at or past the mint counter %d", e.segid, r.nextSegid)
		}
		fence = append(fence, e)
	}
	r.fence = fence
	return r, nil
}

// appendHashFencePage encodes a page's fence entries onto dst: the
// doc 06 section 2.3 page payload, u16 count, u16 reserved, then the
// entries in the root's 16-byte encoding.
func appendHashFencePage(dst []byte, ents []hashFenceEnt) []byte {
	out := grow(dst, hashFencePageHdrLen)
	binary.LittleEndian.PutUint16(out, uint16(len(ents)))
	out[2] = 0
	out[3] = 0
	for _, e := range ents {
		var b [hashFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[:], e.lo)
		binary.LittleEndian.PutUint64(b[8:], e.segid|uint64(e.meta)<<48)
		out = append(out, b[:]...)
	}
	return out
}

// decodeHashFencePage parses and validates a fence page payload into
// the fence scratch. nextSegid is the owning root's mint counter for
// the segid ceiling check; zero skips it, for the payload-blind
// replay path that has no root in hand. No written image exceeds
// hashFencePageMax: the insert that would splits before it lands.
func decodeHashFencePage(p []byte, fence []hashFenceEnt, nextSegid uint64) ([]hashFenceEnt, error) {
	if len(p) < hashFencePageHdrLen {
		return nil, fmt.Errorf("sqlo1: hash fence page of %d bytes, header needs %d", len(p), hashFencePageHdrLen)
	}
	if p[2] != 0 || p[3] != 0 {
		return nil, fmt.Errorf("sqlo1: hash fence page reserved bytes are set")
	}
	n := int(binary.LittleEndian.Uint16(p))
	if n == 0 || n > hashFencePageMax {
		return nil, fmt.Errorf("sqlo1: hash fence page of %d entries outside [1, %d]", n, hashFencePageMax)
	}
	if want := hashFencePageHdrLen + n*hashFenceEntLen; len(p) != want {
		return nil, fmt.Errorf("sqlo1: hash fence page of %d bytes, %d entries need %d", len(p), n, want)
	}
	fence = fence[:0]
	for i := range n {
		off := hashFencePageHdrLen + i*hashFenceEntLen
		sm := binary.LittleEndian.Uint64(p[off+8:])
		e := hashFenceEnt{
			lo:    binary.LittleEndian.Uint64(p[off:]),
			segid: sm & hashFenceSegidMax,
			meta:  uint16(sm >> 48),
		}
		if i > 0 && e.lo <= fence[i-1].lo {
			return nil, fmt.Errorf("sqlo1: hash fence page out of order at entry %d", i)
		}
		if nextSegid != 0 && e.segid >= nextSegid {
			return nil, fmt.Errorf("sqlo1: hash fence page segid %d at or past the mint counter %d", e.segid, nextSegid)
		}
		fence = append(fence, e)
	}
	return fence, nil
}

// hashFenceFind returns the index of the fence entry covering fh:
// the last entry with lo <= fh, which exists because the first lo is
// zero.
func hashFenceFind(fence []hashFenceEnt, fh uint64) int {
	return sort.Search(len(fence), func(i int) bool { return fence[i].lo > fh }) - 1
}

// hashPageFind is hashFenceFind over the page index: the page whose
// range covers fh.
func hashPageFind(pidx []hashPageEnt, fh uint64) int {
	return sort.Search(len(pidx), func(i int) bool { return pidx[i].lo > fh }) - 1
}

// putHashSegKey writes the subkey of hash segment segid under rooth
// into dst[:SubkeySize]: the doc 03 6.3 layout with kind SubkindSeg.
// A shared buffer works because every seam door copies key bytes
// before returning.
func putHashSegKey(dst []byte, rooth, segid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = SubkindSeg
	var seg [8]byte
	binary.LittleEndian.PutUint64(seg[:], segid)
	copy(dst[9:SubkeySize], seg[:7])
}

// putHashFenceKey writes the subkey of fence page pageid under rooth
// into dst[:SubkeySize]: the doc 03 6.3 layout with kind SubkindFence.
func putHashFenceKey(dst []byte, rooth, pageid uint64) {
	binary.LittleEndian.PutUint64(dst, rooth)
	dst[8] = SubkindFence
	var pg [8]byte
	binary.LittleEndian.PutUint64(pg[:], pageid)
	copy(dst[9:SubkeySize], pg[:7])
}

// planedRootInfo reads the shared prefix of any planed root layout
// (rootgen at offset 4, rooth at offset 8), for the cross-type
// overwrite that must retire a plane it cannot otherwise decode.
func planedRootInfo(v []byte) (rooth uint64, rootgen uint32, err error) {
	if len(v) < 16 {
		return 0, 0, fmt.Errorf("sqlo1: planed root payload of %d bytes has no plane prefix", len(v))
	}
	return binary.LittleEndian.Uint64(v[8:]), binary.LittleEndian.Uint32(v[4:]), nil
}
