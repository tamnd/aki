package sqlo1

// Segment records of the segmented hash, doc 06 sections 2.1 and 2.4:
// the field space is partitioned by fh = xxhash64(field), each segment
// owns a half-open [lo, hi) range of fh values, and entries inside a
// segment sort by (fh, field) so a point lookup early-exits and a
// split is one cut. This file is the segment payload machinery: codec,
// point edits, the entry-median split, and the lazy merge. The fence
// that maps fh ranges to segids lives in the root and lands in the
// next slice; nothing here touches the store.
//
// The entry encoding is hash.go's appendHashEntry, byte-identical to
// the inline tier, so the inline-to-segmented upgrade re-sorts entries
// but never re-encodes them.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/cespare/xxhash/v2"
)

const (
	// hashSegMax is the split threshold: an encoded segment past it
	// splits at the entry-median fh. 4032 B is the doc 06 section 2.1
	// default the hseg lab sweeps on the gate box; it is not a format
	// fact (any segment size decodes), so a moved verdict changes only
	// future splits.
	hashSegMax = 4032

	// hashSegMin is the lazy-merge threshold: compaction merges two
	// adjacent segments when the merged encoding would stay under it,
	// so delete-heavy hashes shrink their segment count without any
	// foreground cost.
	hashSegMin = 1024

	// hashSegHdrLen is the segment payload header: u16 n, u16
	// reserved, u64 min_expire_ms.
	hashSegHdrLen = 12
)

// hashFH is the field-space partitioning hash, doc 06 section 2.1.
// This is a format fact: fences persist fh range boundaries, so the
// function can never change once segmented hashes exist on disk.
// hashseg_test pins golden values against an accidental swap.
func hashFH(field []byte) uint64 {
	return xxhash.Sum64(field)
}

// hashSegKeyLess is the segment entry order: by fh, then by field
// bytes. The field tiebreak makes the order total and deterministic
// under fh collisions; equal (fh, field) means the same field, since
// fh is derived.
func hashSegKeyLess(af uint64, afield []byte, bf uint64, bfield []byte) bool {
	if af != bf {
		return af < bf
	}
	return bytes.Compare(afield, bfield) < 0
}

// hashSeg is the decoded segment payload: the header fields plus the
// raw entry region, which aliases the payload.
type hashSeg struct {
	n        int
	minExpMs int64
	entries  []byte
}

// putHashSegHdr writes the 12-byte segment header into b.
func putHashSegHdr(b []byte, n int, minExpMs int64) {
	binary.LittleEndian.PutUint16(b, uint16(n))
	b[2] = 0
	b[3] = 0
	binary.LittleEndian.PutUint64(b[4:], uint64(minExpMs))
}

// decodeHashSeg parses and fully validates a segment payload: every
// entry header, strict (fh, field) order, and the count and min_expire
// must agree with the walk exactly, so corruption fails at the segment
// read. n = 0 is a valid empty segment; deletes can leave one behind
// for the lazy merge to fold away. There is no upper size check
// because a segment legitimately exceeds seg_max between the write
// that grew it and the split, and forever if an fh-collision run
// refuses the split.
func decodeHashSeg(p []byte) (hashSeg, error) {
	if len(p) < hashSegHdrLen {
		return hashSeg{}, fmt.Errorf("sqlo1: hash segment of %d bytes, header needs %d", len(p), hashSegHdrLen)
	}
	if p[2] != 0 || p[3] != 0 {
		return hashSeg{}, fmt.Errorf("sqlo1: hash segment reserved bytes are set")
	}
	s := hashSeg{
		n:        int(binary.LittleEndian.Uint16(p)),
		minExpMs: int64(binary.LittleEndian.Uint64(p[4:])),
		entries:  p[hashSegHdrLen:],
	}
	it := hashEntryIter{p: s.entries}
	seen := 0
	minExp := int64(0)
	var prevFH uint64
	var prevField []byte
	for {
		f, _, expMs, ok, err := it.next()
		if err != nil {
			return hashSeg{}, err
		}
		if !ok {
			break
		}
		fh := hashFH(f)
		if seen > 0 && !hashSegKeyLess(prevFH, prevField, fh, f) {
			return hashSeg{}, fmt.Errorf("sqlo1: hash segment entries out of (fh, field) order at %q", f)
		}
		prevFH, prevField = fh, f
		seen++
		if expMs != 0 && (minExp == 0 || expMs < minExp) {
			minExp = expMs
		}
	}
	if seen != s.n {
		return hashSeg{}, fmt.Errorf("sqlo1: hash segment claims %d entries, region holds %d", s.n, seen)
	}
	if minExp != s.minExpMs {
		return hashSeg{}, fmt.Errorf("sqlo1: hash segment min_expire %d, entries say %d", s.minExpMs, minExp)
	}
	return s, nil
}

// hashSegGet finds one field in a decoded segment, early-exiting on
// the sort order. Returned bytes alias the region.
func hashSegGet(s hashSeg, fh uint64, field []byte) (val []byte, expMs int64, ok bool, err error) {
	it := hashEntryIter{p: s.entries}
	for {
		f, v, eExp, ok, err := it.next()
		if err != nil || !ok {
			return nil, 0, false, err
		}
		efh := hashFH(f)
		if efh == fh && bytes.Equal(f, field) {
			return v, eExp, true, nil
		}
		if hashSegKeyLess(fh, field, efh, f) {
			return nil, 0, false, nil
		}
	}
}

// hashSegSet rebuilds a segment payload with one field written, into
// dst[:0]. created is false for an update; revived means the replaced
// entry was already expired at nowMs, so the write is a create on the
// wire (the dead field was never observable) while the count is an
// update (the dead entry was still counted). expMs is the field TTL
// the written entry carries (0 for none); HSET's clear-on-update rule
// is the caller passing 0.
func hashSegSet(dst []byte, s hashSeg, fh uint64, field, val []byte, expMs, nowMs int64) (out []byte, created, revived bool, err error) {
	out = grow(dst, hashSegHdrLen)
	it := hashEntryIter{p: s.entries}
	created = true
	placed := false
	minExp := expMs
	for {
		before := it.p
		f, _, eExp, ok, err := it.next()
		if err != nil {
			return nil, false, false, err
		}
		if !ok {
			break
		}
		efh := hashFH(f)
		if !placed {
			if efh == fh && bytes.Equal(f, field) {
				out = appendHashEntry(out, field, val, expMs)
				placed = true
				created = false
				revived = eExp != 0 && eExp <= nowMs
				continue
			}
			if hashSegKeyLess(fh, field, efh, f) {
				out = appendHashEntry(out, field, val, expMs)
				placed = true
			}
		}
		out = append(out, before[:len(before)-len(it.p)]...)
		if eExp != 0 && (minExp == 0 || eExp < minExp) {
			minExp = eExp
		}
	}
	if !placed {
		out = appendHashEntry(out, field, val, expMs)
	}
	n := s.n
	if created {
		n++
	}
	putHashSegHdr(out, n, minExp)
	return out, created, revived, nil
}

// hashSegDel rebuilds a segment payload with one field removed, into
// dst[:0]. removed is false when the field was not there, and out is
// nil in that case; an entry already expired at nowMs counts as not
// there (lazy expiry: the dead field was never observable, so HDEL
// answers 0, and the entry itself waits for the reaper). A delete
// that empties the segment returns the bare header; the fence layer
// keeps the empty segment until the lazy merge folds it into a
// neighbor.
func hashSegDel(dst []byte, s hashSeg, fh uint64, field []byte, nowMs int64) (out []byte, removed bool, err error) {
	out = grow(dst, hashSegHdrLen)
	it := hashEntryIter{p: s.entries}
	minExp := int64(0)
	for {
		before := it.p
		f, _, eExp, ok, err := it.next()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			break
		}
		if !removed && hashFH(f) == fh && bytes.Equal(f, field) && (eExp == 0 || eExp > nowMs) {
			removed = true
			continue
		}
		out = append(out, before[:len(before)-len(it.p)]...)
		if eExp != 0 && (minExp == 0 || eExp < minExp) {
			minExp = eExp
		}
	}
	if !removed {
		return nil, false, nil
	}
	putHashSegHdr(out, s.n-1, minExp)
	return out, true, nil
}

// hashSegEntry is one parsed segment entry with its fh materialized,
// the working form for split, merge sorting, and the upgrade path.
// field and val alias the payload they were parsed from.
type hashSegEntry struct {
	fh    uint64
	field []byte
	val   []byte
	expMs int64
}

// parseHashSegEntries parses a raw entry region into dst[:0].
func parseHashSegEntries(dst []hashSegEntry, region []byte) ([]hashSegEntry, error) {
	dst = dst[:0]
	it := hashEntryIter{p: region}
	for {
		f, v, expMs, ok, err := it.next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return dst, nil
		}
		dst = append(dst, hashSegEntry{fh: hashFH(f), field: f, val: v, expMs: expMs})
	}
}

// sortHashSegEntries puts entries into segment order. The upgrade path
// uses it on the inline tier's insertion-order entries.
func sortHashSegEntries(entries []hashSegEntry) {
	slices.SortFunc(entries, func(a, b hashSegEntry) int {
		if a.fh != b.fh {
			if a.fh < b.fh {
				return -1
			}
			return 1
		}
		return bytes.Compare(a.field, b.field)
	})
}

// appendHashSegPayload encodes sorted entries as a full segment
// payload onto dst[:0], computing the header from the entries.
func appendHashSegPayload(dst []byte, entries []hashSegEntry) []byte {
	out := grow(dst, hashSegHdrLen)
	minExp := int64(0)
	for _, e := range entries {
		out = appendHashEntry(out, e.field, e.val, e.expMs)
		if e.expMs != 0 && (minExp == 0 || e.expMs < minExp) {
			minExp = e.expMs
		}
	}
	putHashSegHdr(out, len(entries), minExp)
	return out
}

// splitHashSegEntries finds the split cut for an oversized segment:
// the entry-median fh, walked back so a run of equal fh values never
// straddles the boundary. lo is the segment's own range start. The
// hseg lab pinned this policy (labs/sqlo1/t2/01_hseg). ok is false
// when the segment cannot split, which a 64-bit fh never produces in
// practice; the guard refuses rather than corrupt the fence, and the
// segment just stays oversized.
//
// On ok, entries[:mid] keep the segment and entries[mid:] move to a
// new segment fenced at boundary = entries[mid].fh.
func splitHashSegEntries(entries []hashSegEntry, lo uint64) (mid int, boundary uint64, ok bool) {
	mid = len(entries) / 2
	boundary = entries[mid].fh
	for mid > 0 && entries[mid-1].fh == boundary {
		mid--
	}
	if mid == 0 || boundary <= lo {
		return 0, 0, false
	}
	return mid, boundary, true
}

// shouldMergeHashSegs is the lazy-merge test on two adjacent encoded
// segments: merge when the merged encoding stays under seg_min.
func shouldMergeHashSegs(loPayloadLen, hiPayloadLen int) bool {
	return loPayloadLen+hiPayloadLen-hashSegHdrLen < hashSegMin
}

// mergeHashSegs concatenates two adjacent segments into dst[:0]. lop
// owns the lower fh range, hip the upper; the regions concatenate
// without re-encoding because every lop key orders below every hip
// key, which the boundary check enforces before any bytes move.
func mergeHashSegs(dst, lop, hip []byte) ([]byte, error) {
	los, err := decodeHashSeg(lop)
	if err != nil {
		return nil, err
	}
	his, err := decodeHashSeg(hip)
	if err != nil {
		return nil, err
	}
	if los.n > 0 && his.n > 0 {
		var lastFH uint64
		var lastField []byte
		it := hashEntryIter{p: los.entries}
		for {
			f, _, _, ok, err := it.next()
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			lastFH, lastField = hashFH(f), f
		}
		it = hashEntryIter{p: his.entries}
		first, _, _, _, err := it.next()
		if err != nil {
			return nil, err
		}
		if !hashSegKeyLess(lastFH, lastField, hashFH(first), first) {
			return nil, fmt.Errorf("sqlo1: hash segment merge out of range order at %q", first)
		}
	}
	minExp := los.minExpMs
	if his.minExpMs != 0 && (minExp == 0 || his.minExpMs < minExp) {
		minExp = his.minExpMs
	}
	out := grow(dst, hashSegHdrLen)
	out = append(out, los.entries...)
	out = append(out, his.entries...)
	putHashSegHdr(out, los.n+his.n, minExp)
	return out, nil
}
