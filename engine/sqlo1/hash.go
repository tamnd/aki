package sqlo1

// Hash is the hash type layer, doc 06: the representation ladder
// (inline, segmented, fence-paged) over one Tiered runtime. This file
// carries the inline tier, the ladder's thresholds, and the shared
// root-sub namespace every type layer sniffs; segments and the fence
// land in the following slices.
//
// The inline tier exists because the overwhelming majority of real
// hashes are tiny (Redis's listpack tier exists for the same reason):
// the whole hash is one root record, fields encoded in the root value
// in insertion order, and every point operator is one record touch.
// Insertion order is kept on purpose, because it is what Redis's
// listpack iteration order is, and the compat section diffs iteration
// output byte for byte.
//
// Like Str, a Hash is single-owner: one goroutine, and returned values
// alias internal buffers only until the next call.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrWrongType is the cross-type guard: an operation met a key holding
// another type. It carries Redis's exact wire text (no ERR prefix), so
// the command layer maps it verbatim.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// errHashSegmented marks the paths the segment slice takes over: an
// inline hash that outgrew its tier, or an op meeting an already
// segmented root. Nothing user-visible can reach it until then; the
// threshold tests pin exactly where it trips.
var errHashSegmented = errors.New("sqlo1: hash outgrew the inline tier, segments land in the next slice")

// Root payload sub values live in one global namespace across the
// per-type docs, because the store seam's Record carries only the Root
// bit: a root read cold identifies its type from byte 0 of the
// payload, nothing else. Doc-assigned layouts count up from 1 (doc 05
// rope = 1, doc 06 segmented hash = 2, doc 07 noded list = 3); the
// planeless inline roots take the 0x10 block, 0x10 ORed with the type
// tag, so every future type gets its inline sub without a doc
// collision.
const (
	// hashSubSeg is the segmented hash root layout, doc 06 section 2.2.
	// Defined here for the sniffer; the layout itself lands with the
	// segment slice.
	hashSubSeg = 2

	// inlineSubBase is the planeless inline block: sub 0x10|tag holds
	// the type's elements in the root value, mints no rooth, and owns
	// no segments, so a cross-type overwrite of one is a plain record
	// write with nothing to retire.
	inlineSubBase = 0x10

	// hashSubInline is the inline hash root.
	hashSubInline = inlineSubBase | TagHash
)

// The inline ladder rung's thresholds, doc 06 section 1: a hash stays
// inline while the whole encoded root payload fits hashInlineMax and
// the field count fits hashInlineMaxCount. The first write past either
// upgrades to segments (one-way per key generation).
const (
	hashInlineMax      = 2048
	hashInlineMaxCount = 128
)

// Inline root payload layout:
//
//	u8   sub            // hashSubInline
//	u8   hflags         // bit1 any field TTL; bit0 (fence-paged) is segmented-only
//	u16  count
//	u64  min_expire_ms  // earliest field TTL, 0 if none
//	entries, insertion order
//
// The entry encoding is byte-identical to the segment entry of doc 06
// section 2.4, so the upgrade path copies entries wholesale:
//
//	u8  eflags          // bit0 has_ttl
//	u16 flen
//	u32 vlen
//	field bytes
//	value bytes
//	[u64 expire_ms]     // present iff eflags bit0
const (
	hashInlineHdrLen = 12
	hashEntryHdrLen  = 7

	hflagAnyTTL = 1 << 1
	eflagHasTTL = 1 << 0
)

// sniffRoot identifies a root payload's type from its sub byte:
// the type tag, and whether the root is planeless (an inline root, no
// minted rooth, nothing to retire on overwrite). An unknown sub is
// corruption and fails loudly here, at the first read that meets it.
func sniffRoot(v []byte) (tag uint8, planeless bool, err error) {
	if len(v) == 0 {
		return 0, false, fmt.Errorf("sqlo1: empty root payload")
	}
	switch sub := v[0]; {
	case sub == ropeSub:
		return TagString, false, nil
	case sub == hashSubSeg:
		return TagHash, false, nil
	case sub&0xF0 == inlineSubBase:
		t := sub & 0x0F
		if t >= TagString && t <= TagStream {
			return t, true, nil
		}
	}
	return 0, false, fmt.Errorf("sqlo1: unknown root sub %d", v[0])
}

// appendHashEntry encodes one field entry onto dst; expMs 0 means no
// field TTL. The caller has already bounded flen and vlen (the inline
// size guard does it structurally: an oversized field cannot fit the
// inline payload, and the segment slice owns its own guard).
func appendHashEntry(dst []byte, field, val []byte, expMs int64) []byte {
	var eflags uint8
	if expMs != 0 {
		eflags |= eflagHasTTL
	}
	var h [hashEntryHdrLen]byte
	h[0] = eflags
	binary.LittleEndian.PutUint16(h[1:], uint16(len(field)))
	binary.LittleEndian.PutUint32(h[3:], uint32(len(val)))
	dst = append(dst, h[:]...)
	dst = append(dst, field...)
	dst = append(dst, val...)
	if expMs != 0 {
		var e [8]byte
		binary.LittleEndian.PutUint64(e[:], uint64(expMs))
		dst = append(dst, e[:]...)
	}
	return dst
}

// hashEntryIter walks an encoded entry region. next returns aliases
// into the region; the caller owns their lifetime.
type hashEntryIter struct {
	p []byte
}

func (it *hashEntryIter) next() (field, val []byte, expMs int64, ok bool, err error) {
	if len(it.p) == 0 {
		return nil, nil, 0, false, nil
	}
	if len(it.p) < hashEntryHdrLen {
		return nil, nil, 0, false, fmt.Errorf("sqlo1: hash entry header short: %d bytes", len(it.p))
	}
	eflags := it.p[0]
	if eflags&^uint8(eflagHasTTL) != 0 {
		return nil, nil, 0, false, fmt.Errorf("sqlo1: hash entry flags %#x has reserved bits set", eflags)
	}
	flen := int(binary.LittleEndian.Uint16(it.p[1:]))
	vlen := int(binary.LittleEndian.Uint32(it.p[3:]))
	n := hashEntryHdrLen + flen + vlen
	if eflags&eflagHasTTL != 0 {
		n += 8
	}
	if len(it.p) < n {
		return nil, nil, 0, false, fmt.Errorf("sqlo1: hash entry of %d bytes overruns its region (%d left)", n, len(it.p))
	}
	field = it.p[hashEntryHdrLen : hashEntryHdrLen+flen]
	val = it.p[hashEntryHdrLen+flen : hashEntryHdrLen+flen+vlen]
	if eflags&eflagHasTTL != 0 {
		expMs = int64(binary.LittleEndian.Uint64(it.p[hashEntryHdrLen+flen+vlen:]))
		if expMs == 0 {
			return nil, nil, 0, false, fmt.Errorf("sqlo1: hash entry carries a zero expiry")
		}
	}
	it.p = it.p[n:]
	return field, val, expMs, true, nil
}

// hashInline is the decoded inline root: the header fields plus the
// raw entry region, which aliases the payload.
type hashInline struct {
	count    int
	minExpMs int64
	entries  []byte
}

// putHashInlineHdr writes the 12-byte inline header into b. hflags is
// derived: the TTL bit tracks min_expire.
func putHashInlineHdr(b []byte, count int, minExpMs int64) {
	b[0] = hashSubInline
	b[1] = 0
	if minExpMs != 0 {
		b[1] = hflagAnyTTL
	}
	binary.LittleEndian.PutUint16(b[2:], uint16(count))
	binary.LittleEndian.PutUint64(b[4:], uint64(minExpMs))
}

// appendHashInlineHdr is putHashInlineHdr onto the end of dst.
func appendHashInlineHdr(dst []byte, count int, minExpMs int64) []byte {
	var b [hashInlineHdrLen]byte
	putHashInlineHdr(b[:], count, minExpMs)
	return append(dst, b[:]...)
}

// decodeHashInline parses and fully validates an inline root payload:
// the entry walk checks every header, and the entry count and region
// must agree exactly, so corruption fails at the root read instead of
// as a wrong scan later. The walk is O(payload), which the inline cap
// bounds at 2 KiB.
func decodeHashInline(p []byte) (hashInline, error) {
	if len(p) < hashInlineHdrLen {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash root of %d bytes, header needs %d", len(p), hashInlineHdrLen)
	}
	if p[0] != hashSubInline {
		return hashInline{}, fmt.Errorf("sqlo1: root sub %d is not an inline hash", p[0])
	}
	if p[1]&^uint8(hflagAnyTTL) != 0 {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash flags %#x has reserved bits set", p[1])
	}
	h := hashInline{
		count:    int(binary.LittleEndian.Uint16(p[2:])),
		minExpMs: int64(binary.LittleEndian.Uint64(p[4:])),
		entries:  p[hashInlineHdrLen:],
	}
	if (p[1]&hflagAnyTTL != 0) != (h.minExpMs != 0) {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash TTL flag disagrees with min_expire %d", h.minExpMs)
	}
	if h.count == 0 || h.count > hashInlineMaxCount {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash count %d outside [1, %d]", h.count, hashInlineMaxCount)
	}
	if len(p) > hashInlineMax {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash payload of %d bytes exceeds %d", len(p), hashInlineMax)
	}
	it := hashEntryIter{p: h.entries}
	seen := 0
	minExp := int64(0)
	for {
		_, _, expMs, ok, err := it.next()
		if err != nil {
			return hashInline{}, err
		}
		if !ok {
			break
		}
		seen++
		if expMs != 0 && (minExp == 0 || expMs < minExp) {
			minExp = expMs
		}
	}
	if seen != h.count {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash claims %d fields, region holds %d", h.count, seen)
	}
	if minExp != h.minExpMs {
		return hashInline{}, fmt.Errorf("sqlo1: inline hash min_expire %d, entries say %d", h.minExpMs, minExp)
	}
	return h, nil
}

// Hash is the hash ladder over one Tiered. The segment slices add the
// minter dependency when the segmented rung arrives; the inline tier
// mints nothing.
type Hash struct {
	t *Tiered

	// rootBuf builds the replacement inline payload; the old payload
	// aliases an arena that the write recycles, so every rebuild
	// finishes before the store call.
	rootBuf []byte
}

// NewHash builds the hash layer over t.
func NewHash(t *Tiered) *Hash {
	return &Hash{t: t}
}

// restamp mirrors Str.restamp: puts a key's expiry back after a write
// that may have gone through a fresh hot header.
func (h *Hash) restamp(ctx context.Context, key []byte, expMs int64) error {
	if expMs == 0 {
		return nil
	}
	_, err := h.t.ExpireAt(ctx, key, expMs)
	return err
}

// hashState is what the point ops need from a key read: absent, an
// inline hash (decoded), a segmented hash, or another type entirely.
type hashState int

const (
	hashAbsent hashState = iota
	hashInlineState
	hashSegState
)

// stateOf reads key and classifies it for the hash ops. The decoded
// inline view aliases the read; it dies on the next Tiered call.
func (h *Hash) stateOf(ctx context.Context, key []byte) (hashState, hashInline, int64, error) {
	v, root, expMs, ok, err := h.t.LookupEntry(ctx, key)
	if err != nil || !ok {
		return hashAbsent, hashInline{}, 0, err
	}
	if !root {
		return hashAbsent, hashInline{}, 0, ErrWrongType
	}
	tag, _, err := sniffRoot(v)
	if err != nil {
		return hashAbsent, hashInline{}, 0, err
	}
	if tag != TagHash {
		return hashAbsent, hashInline{}, 0, ErrWrongType
	}
	if v[0] == hashSubSeg {
		return hashSegState, hashInline{}, expMs, nil
	}
	hi, err := decodeHashInline(v)
	if err != nil {
		return hashAbsent, hashInline{}, 0, err
	}
	return hashInlineState, hi, expMs, nil
}

// HSet writes one field and reports whether it was created (false for
// an update of an existing field). An update keeps the field's
// position, listpack-style, and clears any field TTL it carried, which
// is Redis's HSET rule. The write that would push the hash past the
// inline thresholds upgrades to segments; until that slice lands it
// returns errHashSegmented.
func (h *Hash) HSet(ctx context.Context, key, field, val []byte) (bool, error) {
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	switch st {
	case hashSegState:
		return false, errHashSegmented
	case hashAbsent:
		h.rootBuf = appendHashInlineHdr(h.rootBuf[:0], 1, 0)
		h.rootBuf = appendHashEntry(h.rootBuf, field, val, 0)
		if len(h.rootBuf) > hashInlineMax {
			return false, errHashSegmented
		}
		if err := h.t.Set(ctx, key, h.rootBuf, TagHash|TagRoot); err != nil {
			return false, err
		}
		return true, h.restamp(ctx, key, expMs)
	}
	// Rebuild the payload around the one field. The region is walked
	// once: matching entry replaced in place (its raw span dropped,
	// the new entry encoded), everything else copied span for span.
	// The header slot is filled last, when count and min_expire are
	// known.
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	it := hashEntryIter{p: hi.entries}
	created := true
	minExp := int64(0)
	for {
		before := it.p
		f, _, eExp, ok, err := it.next()
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		if bytes.Equal(f, field) {
			h.rootBuf = appendHashEntry(h.rootBuf, field, val, 0)
			created = false
			continue
		}
		h.rootBuf = append(h.rootBuf, before[:len(before)-len(it.p)]...)
		if eExp != 0 && (minExp == 0 || eExp < minExp) {
			minExp = eExp
		}
	}
	count := hi.count
	if created {
		h.rootBuf = appendHashEntry(h.rootBuf, field, val, 0)
		count++
	}
	if count > hashInlineMaxCount || len(h.rootBuf) > hashInlineMax {
		return false, errHashSegmented
	}
	putHashInlineHdr(h.rootBuf, count, minExp)
	if err := h.t.Set(ctx, key, h.rootBuf, TagHash|TagRoot); err != nil {
		return false, err
	}
	return created, h.restamp(ctx, key, expMs)
}

// HGet reads one field. The returned bytes alias internal buffers and
// are valid until the next call.
func (h *Hash) HGet(ctx context.Context, key, field []byte) ([]byte, bool, error) {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return nil, false, err
	}
	switch st {
	case hashAbsent:
		return nil, false, nil
	case hashSegState:
		return nil, false, errHashSegmented
	}
	it := hashEntryIter{p: hi.entries}
	for {
		f, v, _, ok, err := it.next()
		if err != nil || !ok {
			return nil, false, err
		}
		if bytes.Equal(f, field) {
			return v, true, nil
		}
	}
}

// HDel removes one field and reports whether it was there. Deleting
// the last field deletes the key, which is Redis's empty-hash rule.
func (h *Hash) HDel(ctx context.Context, key, field []byte) (bool, error) {
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	switch st {
	case hashAbsent:
		return false, nil
	case hashSegState:
		return false, errHashSegmented
	}
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	it := hashEntryIter{p: hi.entries}
	found := false
	minExp := int64(0)
	for {
		before := it.p
		f, _, eExp, ok, err := it.next()
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		if bytes.Equal(f, field) {
			found = true
			continue
		}
		h.rootBuf = append(h.rootBuf, before[:len(before)-len(it.p)]...)
		if eExp != 0 && (minExp == 0 || eExp < minExp) {
			minExp = eExp
		}
	}
	if !found {
		return false, nil
	}
	if hi.count == 1 {
		_, err := h.t.Del(ctx, key)
		return true, err
	}
	putHashInlineHdr(h.rootBuf, hi.count-1, minExp)
	if err := h.t.Set(ctx, key, h.rootBuf, TagHash|TagRoot); err != nil {
		return false, err
	}
	return true, h.restamp(ctx, key, expMs)
}

// HLen answers from the root header alone, at any representation: the
// count-exactness rules of doc 06 section 5 exist so this never has to
// touch a segment.
func (h *Hash) HLen(ctx context.Context, key []byte) (int64, error) {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return 0, err
	}
	switch st {
	case hashAbsent:
		return 0, nil
	case hashSegState:
		return 0, errHashSegmented
	}
	return int64(hi.count), nil
}

// Encoding is the OBJECT ENCODING answer for hash keys: listpack for
// the inline tier, hashtable past it, doc 06 section 1's parity rule.
func (h *Hash) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	st, _, _, err := h.stateOf(ctx, key)
	if err != nil {
		return "", false, err
	}
	switch st {
	case hashAbsent:
		return "", false, nil
	case hashSegState:
		return "hashtable", true, nil
	}
	return "listpack", true, nil
}
