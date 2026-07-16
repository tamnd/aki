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

// errHashFencePaged marks the fence-paging boundary: a hash whose
// fence outgrew the root. Fence pages (rtype 5) land in a later
// slice; until then the fence caps at hashFenceMaxSegs segments,
// roughly 10k+ fields, and the write that would cross returns this.
var errHashFencePaged = errors.New("sqlo1: hash fence outgrew the root, fence paging lands in a later slice")

const (
	// hashSegRootHdrLen is the fixed part of the segmented root
	// payload: sub, hflags, reserved, rootgen, rooth, count,
	// next_segid, min_expire_ms, seg_count.
	hashSegRootHdrLen = 44

	// hashFenceEntLen is one fence entry: u64 lo, then 48 bits of
	// segid under 16 bits of per-segment metadata.
	hashFenceEntLen = 16

	// hashFenceMaxSegs is fence_inline_max (2048 B) over the entry
	// size: the most segments an inline fence addresses.
	hashFenceMaxSegs = 2048 / hashFenceEntLen

	// hashFenceSegidMax is the 48-bit segid ceiling a fence entry can
	// hold, tighter than the subkey's 56 bits.
	hashFenceSegidMax = 1<<48 - 1

	// hflagFencePaged marks a root whose fence lives in rtype 5
	// pages. Written by no path yet; decode rejects it so the paging
	// slice cannot land bytes this build would misread.
	hflagFencePaged = 1 << 0
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

// hashSegRoot is the decoded segmented root. The fence slice is
// built fresh on decode (it does not alias the payload), so the
// struct stays valid across the segment reads an operator does next.
type hashSegRoot struct {
	rootgen   uint32
	rooth     uint64
	count     uint64
	nextSegid uint64
	minExpMs  int64
	fence     []hashFenceEnt
}

// appendHashSegRoot encodes r onto dst[:0].
func appendHashSegRoot(dst []byte, r *hashSegRoot) []byte {
	out := grow(dst, hashSegRootHdrLen)
	out[0] = hashSubSeg
	out[1] = 0
	if r.minExpMs != 0 {
		out[1] = hflagAnyTTL
	}
	out[2] = 0
	out[3] = 0
	binary.LittleEndian.PutUint32(out[4:], r.rootgen)
	binary.LittleEndian.PutUint64(out[8:], r.rooth)
	binary.LittleEndian.PutUint64(out[16:], r.count)
	binary.LittleEndian.PutUint64(out[24:], r.nextSegid)
	binary.LittleEndian.PutUint64(out[32:], uint64(r.minExpMs))
	binary.LittleEndian.PutUint32(out[40:], uint32(len(r.fence)))
	for _, e := range r.fence {
		var b [hashFenceEntLen]byte
		binary.LittleEndian.PutUint64(b[:], e.lo)
		binary.LittleEndian.PutUint64(b[8:], e.segid|uint64(e.meta)<<48)
		out = append(out, b[:]...)
	}
	return out
}

// decodeHashSegRoot parses and validates a segmented root payload,
// building the fence into the fence scratch. Everything checkable
// without touching segments is checked here.
func decodeHashSegRoot(p []byte, fence []hashFenceEnt) (hashSegRoot, error) {
	if len(p) < hashSegRootHdrLen {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root of %d bytes, header needs %d", len(p), hashSegRootHdrLen)
	}
	if p[0] != hashSubSeg {
		return hashSegRoot{}, fmt.Errorf("sqlo1: root sub %d is not a segmented hash", p[0])
	}
	if p[1]&hflagFencePaged != 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: fence-paged hash root, paging lands in a later slice")
	}
	if p[1]&^uint8(hflagAnyTTL) != 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash flags %#x has reserved bits set", p[1])
	}
	if p[2] != 0 || p[3] != 0 {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash root reserved bytes are set")
	}
	r := hashSegRoot{
		rootgen:   binary.LittleEndian.Uint32(p[4:]),
		rooth:     binary.LittleEndian.Uint64(p[8:]),
		count:     binary.LittleEndian.Uint64(p[16:]),
		nextSegid: binary.LittleEndian.Uint64(p[24:]),
		minExpMs:  int64(binary.LittleEndian.Uint64(p[32:])),
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
	if segCount == 0 || segCount > hashFenceMaxSegs {
		return hashSegRoot{}, fmt.Errorf("sqlo1: segmented hash fence of %d entries outside [1, %d]", segCount, hashFenceMaxSegs)
	}
	if want := hashSegRootHdrLen + segCount*hashFenceEntLen; len(p) != want {
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

// hashFenceFind returns the index of the fence entry covering fh:
// the last entry with lo <= fh, which exists because the first lo is
// zero.
func hashFenceFind(fence []hashFenceEnt, fh uint64) int {
	return sort.Search(len(fence), func(i int) bool { return fence[i].lo > fh }) - 1
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

// planedRootInfo reads the shared prefix of any planed root layout
// (rootgen at offset 4, rooth at offset 8), for the cross-type
// overwrite that must retire a plane it cannot otherwise decode.
func planedRootInfo(v []byte) (rooth uint64, rootgen uint32, err error) {
	if len(v) < 16 {
		return 0, 0, fmt.Errorf("sqlo1: planed root payload of %d bytes has no plane prefix", len(v))
	}
	return binary.LittleEndian.Uint64(v[8:]), binary.LittleEndian.Uint32(v[4:]), nil
}
