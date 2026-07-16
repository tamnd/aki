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
	"slices"
	"time"
)

// ErrWrongType is the cross-type guard: an operation met a key holding
// another type. It carries Redis's exact wire text (no ERR prefix), so
// the command layer maps it verbatim.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

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

// HashConfig sizes a Hash. Zero values take the defaults.
type HashConfig struct {
	// Shard namespaces the rooth mint, doc 03 section 6.3.
	Shard uint16
	// LeaseN is the mint lease size. Default defaultLeaseN.
	LeaseN uint64
	// Seed seeds HRANDFIELD's generator; zero takes a clock seed.
	// Tests that want reproducible draws set it.
	Seed uint64
}

// Hash is the hash ladder over one Tiered. Construction requires the
// Minter capability, because segmented hashes cannot exist without
// durable rooth leases.
type Hash struct {
	t    *Tiered
	mint Minter
	cfg  HashConfig

	// The current mint lease: counters [leaseNext, leaseEnd) are ours.
	leaseNext uint64
	leaseEnd  uint64

	// Reusable scratch. rootBuf builds replacement root payloads (the
	// old payload aliases an arena the write recycles, so every
	// rebuild finishes before the store call), segBuf and segBuf2 the
	// segment images an op writes, kbuf and kbuf2 the segment subkeys,
	// ents the parsed entries of a split or upgrade, fence the decoded
	// fence of the root under operation.
	rootBuf []byte
	segBuf  []byte
	segBuf2 []byte
	kbuf    [SubkeySize]byte
	kbuf2   [SubkeySize]byte
	ents    []hashSegEntry
	fence   []hashFenceEnt

	// segRoot is the decoded root stateOf leaves behind at
	// hashSegState; its fence rides the fence scratch. Valid until
	// the next stateOf.
	segRoot hashSegRoot

	// valBuf carries a point op's value copy across the mutation that
	// would recycle the arena bytes it read (HGETDEL, HGETEX, the
	// INCR family's formatted result).
	valBuf []byte

	// HMGET scratch: per-field fence indexes, the field-to-slot dedupe
	// over the fence, the unique segment subkeys and their backing
	// bytes, the batch outputs, and the decoded segments.
	mgIdx    []int
	mgNeed   []int
	mgKeys   [][]byte
	mgKeyBuf []byte
	mgVals   [][]byte
	mgRoots  []bool
	mgExps   []int64
	mgSegs   []hashSeg

	// HRANDFIELD state: the splitmix64 walk, the cumulative fence
	// weights of one draw batch, the distinct-sample bookkeeping, and
	// the reservoir's copy arena (emitted bytes must not alias IO
	// rounds the pass has already left behind).
	rngState uint64
	wsum     []uint64
	picked   map[uint64]struct{}
	rvSlots  []rvSlot
	rvArena  []byte
}

// NewHash builds the hash layer over t.
func NewHash(t *Tiered, cfg HashConfig) (*Hash, error) {
	mint, ok := t.st.(Minter)
	if !ok {
		return nil, fmt.Errorf("sqlo1: store %T lacks the Minter capability the hash ladder needs", t.st)
	}
	if cfg.LeaseN == 0 {
		cfg.LeaseN = defaultLeaseN
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	return &Hash{t: t, mint: mint, cfg: cfg, rngState: seed}, nil
}

// nextRooth mints one rooth, taking a fresh durable lease when the
// current one is spent.
func (h *Hash) nextRooth(ctx context.Context) (uint64, error) {
	if h.leaseNext == h.leaseEnd {
		start, err := h.mint.MintLease(ctx, h.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		end, err := LeaseEnd(start, h.cfg.LeaseN)
		if err != nil {
			return 0, err
		}
		h.leaseNext, h.leaseEnd = start, end
	}
	c := h.leaseNext
	h.leaseNext++
	return MintRooth(h.cfg.Shard, c)
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
// inline view aliases the read; it dies on the next Tiered call. At
// hashSegState the decoded root lands in h.segRoot instead, which
// does not alias the read (the fence is copied out on decode) and
// stays valid across the segment reads the op does next.
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
		h.segRoot, err = decodeHashSegRoot(v, h.fence[:0])
		if err != nil {
			return hashAbsent, hashInline{}, 0, err
		}
		h.fence = h.segRoot.fence
		return hashSegState, hashInline{}, expMs, nil
	}
	hi, err := decodeHashInline(v)
	if err != nil {
		return hashAbsent, hashInline{}, 0, err
	}
	return hashInlineState, hi, expMs, nil
}

// readSeg reads the segment record at segid under the current root's
// plane into a decoded view. The view aliases the read and dies on
// the next Tiered call.
func (h *Hash) readSeg(ctx context.Context, segid uint64) (hashSeg, error) {
	putHashSegKey(h.kbuf[:], h.segRoot.rooth, segid)
	v, ok, err := h.t.Get(ctx, h.kbuf[:])
	if err != nil {
		return hashSeg{}, err
	}
	if !ok {
		return hashSeg{}, fmt.Errorf("sqlo1: hash segment %d of rooth %#x is missing", segid, h.segRoot.rooth)
	}
	return decodeHashSeg(v)
}

// writeSeg writes a segment image under the current root's plane.
func (h *Hash) writeSeg(ctx context.Context, segid uint64, payload []byte) error {
	putHashSegKey(h.kbuf[:], h.segRoot.rooth, segid)
	return h.t.SetGen(ctx, h.kbuf[:], payload, TagHash, h.segRoot.rootgen)
}

// writeSegRoot encodes h.segRoot and lands it under key. delta is the
// rule W2 claim: this image moved only count, min_expire, or fence
// meta, never the fence shape, so a store whose replay reconciles
// roots from segment frames may skip its WAL frame. Count-only paths
// (hsetSeg's create, hdelSeg's plain removal) pass true; anything that
// edits fence entries structurally (upgrade, split, merge) passes
// false. The hot tier downgrades the claim itself if the image
// coalesces over a structural write still waiting to drain.
func (h *Hash) writeSegRoot(ctx context.Context, key []byte, delta bool) error {
	h.rootBuf = appendHashSegRoot(h.rootBuf[:0], &h.segRoot)
	tag := TagHash | TagRoot
	if delta {
		tag |= TagDelta
	}
	return h.t.Set(ctx, key, h.rootBuf, tag)
}

// HSet writes one field and reports whether it was created (false for
// an update of an existing field). An update keeps the field's
// position, listpack-style, and clears any field TTL it carried, which
// is Redis's HSET rule. The write that pushes the hash past the
// inline thresholds upgrades it to segments.
func (h *Hash) HSet(ctx context.Context, key, field, val []byte) (bool, error) {
	return h.hset(ctx, key, field, val, 0)
}

// hset is the shared field write under every point mutator: entryExp
// is the field TTL the written entry ends up with, 0 for none. HSET
// passes 0 (its clear-the-TTL rule), the INCR family passes the old
// entry's expiry through (Redis preserves field TTLs across HINCRBY),
// and HGETEX passes the new one.
func (h *Hash) hset(ctx context.Context, key, field, val []byte, entryExp int64) (bool, error) {
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	switch st {
	case hashSegState:
		return h.hsetSeg(ctx, key, field, val, expMs, entryExp)
	case hashAbsent:
		h.rootBuf = appendHashInlineHdr(h.rootBuf[:0], 1, entryExp)
		h.rootBuf = appendHashEntry(h.rootBuf, field, val, entryExp)
		if len(h.rootBuf) > hashInlineMax {
			return h.upgrade(ctx, key, h.rootBuf[hashInlineHdrLen:], true, expMs)
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
			h.rootBuf = appendHashEntry(h.rootBuf, field, val, entryExp)
			created = false
			continue
		}
		h.rootBuf = append(h.rootBuf, before[:len(before)-len(it.p)]...)
		if eExp != 0 && (minExp == 0 || eExp < minExp) {
			minExp = eExp
		}
	}
	if entryExp != 0 && (minExp == 0 || entryExp < minExp) {
		minExp = entryExp
	}
	count := hi.count
	if created {
		h.rootBuf = appendHashEntry(h.rootBuf, field, val, entryExp)
		count++
	}
	if count > hashInlineMaxCount || len(h.rootBuf) > hashInlineMax {
		return h.upgrade(ctx, key, h.rootBuf[hashInlineHdrLen:], created, expMs)
	}
	putHashInlineHdr(h.rootBuf, count, minExp)
	if err := h.t.Set(ctx, key, h.rootBuf, TagHash|TagRoot); err != nil {
		return false, err
	}
	return created, h.restamp(ctx, key, expMs)
}

// upgrade moves a hash from the inline tier to segments. region is
// the finished inline entry region already carrying the write that
// crossed a threshold (it sits in h.rootBuf); created is that write's
// answer and rides through unchanged. Entries are parsed, sorted into
// segment order, packed into segments, and the plane lands before the
// root that references it: every crash prefix reads the old inline
// root over a plane nothing references yet, the setRope rule.
func (h *Hash) upgrade(ctx context.Context, key []byte, region []byte, created bool, expMs int64) (bool, error) {
	var err error
	h.ents, err = parseHashSegEntries(h.ents[:0], region)
	if err != nil {
		return false, err
	}
	sortHashSegEntries(h.ents)

	rooth, err := h.nextRooth(ctx)
	if err != nil {
		return false, err
	}
	h.segRoot = hashSegRoot{rootgen: 1, rooth: rooth, count: uint64(len(h.ents))}

	// Pack sorted entries into segments up to seg_max, never cutting
	// between equal fh values (the fence could not separate them).
	// The upgrade image is barely past the inline cap so this is one
	// segment in practice, but the packer is general. cuts[i] ends
	// segment i.
	cuts := make([]int, 0, 2)
	size := hashSegHdrLen
	start := 0
	for i, e := range h.ents {
		es := hashEntryHdrLen + len(e.field) + len(e.val)
		if e.expMs != 0 {
			es += 8
		}
		if i > start && size+es > hashSegMax && e.fh != h.ents[i-1].fh {
			cuts = append(cuts, i)
			start = i
			size = hashSegHdrLen
		}
		size += es
	}
	cuts = append(cuts, len(h.ents))

	h.fence = h.fence[:0]
	minExp := int64(0)
	start = 0
	for _, end := range cuts {
		seg := h.ents[start:end]
		h.segBuf = appendHashSegPayload(h.segBuf, seg)
		if err := h.writeSeg(ctx, h.segRoot.nextSegid, h.segBuf); err != nil {
			return false, err
		}
		segMin := int64(binary.LittleEndian.Uint64(h.segBuf[4:]))
		lo := uint64(0)
		if len(h.fence) > 0 {
			lo = seg[0].fh
		}
		h.fence = append(h.fence, hashFenceEnt{
			lo:    lo,
			segid: h.segRoot.nextSegid,
			meta:  hashSegMeta(len(seg), segMin),
		})
		h.segRoot.nextSegid++
		if segMin != 0 && (minExp == 0 || segMin < minExp) {
			minExp = segMin
		}
		start = end
	}
	if err := h.t.Flush(ctx); err != nil {
		return false, err
	}
	h.segRoot.minExpMs = minExp
	h.segRoot.fence = h.fence
	if err := h.writeSegRoot(ctx, key, false); err != nil {
		return false, err
	}
	return created, h.restamp(ctx, key, expMs)
}

// hsetSeg writes one field of a segmented hash: the covering segment
// rebuilds around the field, and the root is touched only when the
// write changed the count (rule W1: cardinality change pins the
// root) or moved something the root header carries, so the
// steady-state update costs one segment record. A rebuild past
// seg_max splits.
func (h *Hash) hsetSeg(ctx context.Context, key, field, val []byte, expMs, entryExp int64) (bool, error) {
	r := &h.segRoot
	f := hashFH(field)
	i := hashFenceFind(r.fence, f)
	s, err := h.readSeg(ctx, r.fence[i].segid)
	if err != nil {
		return false, err
	}
	out, created, err := hashSegSet(h.segBuf, s, f, field, val, entryExp)
	if err != nil {
		return false, err
	}
	h.segBuf = out
	// The root min_expire is lower-only from the segment post-image
	// (H-I6: stale-early is legal, stale-late is not). Lowering before
	// the split check means a split's root write carries it too.
	segMin := int64(binary.LittleEndian.Uint64(out[4:]))
	minLowered := segMin != 0 && (r.minExpMs == 0 || segMin < r.minExpMs)
	if minLowered {
		r.minExpMs = segMin
	}
	if len(out) > hashSegMax {
		h.ents, err = parseHashSegEntries(h.ents[:0], out[hashSegHdrLen:])
		if err != nil {
			return false, err
		}
		if mid, boundary, ok := splitHashSegEntries(h.ents, r.fence[i].lo); ok {
			return h.splitSeg(ctx, key, i, mid, boundary, created, expMs)
		}
		// No legal cut (an fh-collision run): the segment stays
		// oversized, which the codec allows.
	}
	if err := h.writeSeg(ctx, r.fence[i].segid, out); err != nil {
		return false, err
	}
	if !created {
		meta := hashSegMeta(int(binary.LittleEndian.Uint16(out)), segMin)
		if meta == r.fence[i].meta && !minLowered {
			// A pure update touches no root and no user-key header,
			// so there is no expiry to restamp either.
			return false, nil
		}
		// A TTL edit moved the fence meta or the root min: skipping
		// the root here would leave min_expire stale-late, so this
		// write pins it like a cardinality change would (rule W1).
		r.fence[i].meta = meta
		if err := h.writeSegRoot(ctx, key, true); err != nil {
			return false, err
		}
		return false, h.restamp(ctx, key, expMs)
	}
	r.count++
	r.fence[i].meta = hashSegMeta(int(binary.LittleEndian.Uint16(out)), segMin)
	if err := h.writeSegRoot(ctx, key, true); err != nil {
		return false, err
	}
	return true, h.restamp(ctx, key, expMs)
}

// splitSeg lands the split of fence entry i, whose post-write entries
// sit parsed in h.ents (aliasing h.segBuf) with the cut at mid and
// the new range starting at boundary. The order is the crash rule:
// the new segment's image lands and flushes before the root that
// references it, and the surviving segment's trimmed image lands
// after the root, because until then its extra entries are dead bytes
// the fence routes around (point reads go through the fence, so a
// crash prefix never reads them; iteration range-filters when it
// lands).
func (h *Hash) splitSeg(ctx context.Context, key []byte, i, mid int, boundary uint64, created bool, expMs int64) (bool, error) {
	r := &h.segRoot
	if len(r.fence) >= hashFenceMaxSegs {
		return false, errHashFencePaged
	}
	if r.nextSegid > hashFenceSegidMax {
		return false, fmt.Errorf("sqlo1: hash segid space of rooth %#x is spent", r.rooth)
	}
	segMin := func(ents []hashSegEntry) int64 {
		m := int64(0)
		for _, e := range ents {
			if e.expMs != 0 && (m == 0 || e.expMs < m) {
				m = e.expMs
			}
		}
		return m
	}

	newSegid := r.nextSegid
	h.segBuf2 = appendHashSegPayload(h.segBuf2, h.ents[mid:])
	if err := h.writeSeg(ctx, newSegid, h.segBuf2); err != nil {
		return false, err
	}
	if err := h.t.Flush(ctx); err != nil {
		return false, err
	}

	r.nextSegid++
	if created {
		r.count++
	}
	r.fence[i].meta = hashSegMeta(mid, segMin(h.ents[:mid]))
	r.fence = slices.Insert(r.fence, i+1, hashFenceEnt{
		lo:    boundary,
		segid: newSegid,
		meta:  hashSegMeta(len(h.ents)-mid, segMin(h.ents[mid:])),
	})
	h.fence = r.fence
	if err := h.writeSegRoot(ctx, key, false); err != nil {
		return false, err
	}

	h.segBuf2 = appendHashSegPayload(h.segBuf2[:0], h.ents[:mid])
	if err := h.writeSeg(ctx, r.fence[i].segid, h.segBuf2); err != nil {
		return false, err
	}
	return created, h.restamp(ctx, key, expMs)
}

// hdelSeg removes one field of a segmented hash. Every removal
// changes the count, so the root is always written (rule W1); the
// removal that empties the whole hash deletes the key and retires the
// plane in O(1), exactly like a cross-type overwrite. A removal that
// shrinks the segment enough tries the lazy merge with a neighbor.
func (h *Hash) hdelSeg(ctx context.Context, key, field []byte, expMs int64) (bool, error) {
	r := &h.segRoot
	f := hashFH(field)
	i := hashFenceFind(r.fence, f)
	s, err := h.readSeg(ctx, r.fence[i].segid)
	if err != nil {
		return false, err
	}
	out, removed, err := hashSegDel(h.segBuf, s, f, field)
	if err != nil {
		return false, err
	}
	if !removed {
		return false, nil
	}
	h.segBuf = out
	if r.count == 1 {
		// Last field: the key dies and the plane retires whole, doc 06
		// section 3. A recreate starts inline under a fresh rootgen, so
		// the retired segments can never be misread.
		h.t.Bump(key, r.rooth, r.rootgen+1)
		_, err := h.t.Del(ctx, key)
		return true, err
	}
	r.count--
	merged, err := h.tryMergeSeg(ctx, key, i, out)
	if err != nil {
		return false, err
	}
	if merged {
		return true, h.restamp(ctx, key, expMs)
	}
	if err := h.writeSeg(ctx, r.fence[i].segid, out); err != nil {
		return false, err
	}
	r.fence[i].meta = hashSegMeta(int(binary.LittleEndian.Uint16(out)), int64(binary.LittleEndian.Uint64(out[4:])))
	if err := h.writeSegRoot(ctx, key, true); err != nil {
		return false, err
	}
	return true, h.restamp(ctx, key, expMs)
}

// tryMergeSeg is the lazy merge, doc 06 section 2.1: segment i just
// shrank (its unwritten post-image is out, aliasing h.segBuf), and if
// the merged encoding with a neighbor stays under seg_min the two
// fold into the lower side's segment. The crash order needs no
// barrier: merged image to the low subkey first (a crash here leaves
// entries past the low entry's fence range, dead bytes point reads
// route around), then the root that drops the high entry, then the
// high subkey's delete (a crash before it leaves a bounded orphan the
// plane retire cleans).
func (h *Hash) tryMergeSeg(ctx context.Context, key []byte, i int, out []byte) (bool, error) {
	r := &h.segRoot
	try := func(lo, hi int) (bool, error) {
		if lo < 0 || hi >= len(r.fence) {
			return false, nil
		}
		other := lo
		if other == i {
			other = hi
		}
		putHashSegKey(h.kbuf2[:], r.rooth, r.fence[other].segid)
		nb, ok, err := h.t.Get(ctx, h.kbuf2[:])
		if err != nil {
			return false, err
		}
		if !ok {
			return false, fmt.Errorf("sqlo1: hash segment %d of rooth %#x is missing", r.fence[other].segid, r.rooth)
		}
		if !shouldMergeHashSegs(len(out), len(nb)) {
			return false, nil
		}
		lop, hip := out, nb
		if other == lo {
			lop, hip = nb, out
		}
		h.segBuf2, err = mergeHashSegs(h.segBuf2, lop, hip)
		if err != nil {
			return false, err
		}
		if err := h.writeSeg(ctx, r.fence[lo].segid, h.segBuf2); err != nil {
			return false, err
		}
		hiSegid := r.fence[hi].segid
		r.fence[lo].meta = hashSegMeta(int(binary.LittleEndian.Uint16(h.segBuf2)), int64(binary.LittleEndian.Uint64(h.segBuf2[4:])))
		r.fence = append(r.fence[:hi], r.fence[hi+1:]...)
		h.fence = r.fence
		if err := h.writeSegRoot(ctx, key, false); err != nil {
			return false, err
		}
		putHashSegKey(h.kbuf2[:], r.rooth, hiSegid)
		if _, err := h.t.Del(ctx, h.kbuf2[:]); err != nil {
			return false, err
		}
		return true, nil
	}
	if done, err := try(i, i+1); done || err != nil {
		return done, err
	}
	return try(i-1, i)
}

// HGet reads one field. The returned bytes alias internal buffers and
// are valid until the next call.
func (h *Hash) HGet(ctx context.Context, key, field []byte) ([]byte, bool, error) {
	v, _, ok, err := h.getEntry(ctx, key, field)
	return v, ok, err
}

// getEntry is HGet with the field TTL beside the value: the read half
// of every point op that must preserve or edit an entry expiry. The
// value aliases internal buffers and dies on the next Tiered call.
func (h *Hash) getEntry(ctx context.Context, key, field []byte) ([]byte, int64, bool, error) {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return nil, 0, false, err
	}
	switch st {
	case hashAbsent:
		return nil, 0, false, nil
	case hashSegState:
		f := hashFH(field)
		s, err := h.readSeg(ctx, h.segRoot.fence[hashFenceFind(h.segRoot.fence, f)].segid)
		if err != nil {
			return nil, 0, false, err
		}
		return hashSegGet(s, f, field)
	}
	it := hashEntryIter{p: hi.entries}
	for {
		f, v, eExp, ok, err := it.next()
		if err != nil || !ok {
			return nil, 0, false, err
		}
		if bytes.Equal(f, field) {
			return v, eExp, true, nil
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
		return h.hdelSeg(ctx, key, field, expMs)
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
		return int64(h.segRoot.count), nil
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
