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

	// setSubSeg is the segmented set root layout: doc 08 rides the doc
	// 06 machinery wholesale, so the planed sub takes the type tag slot
	// the count-up convention assigns it.
	setSubSeg = TagSet

	// setSubInline is the inline set root.
	setSubInline = inlineSubBase | TagSet

	// zsetSubSeg is the segmented zset root layout: doc 09's member
	// side rides the doc 06 machinery with the entry value fixed at
	// the 8-byte sortable score, under the type tag slot like the set.
	zsetSubSeg = TagZset

	// zsetSubInline is the inline zset root.
	zsetSubInline = inlineSubBase | TagZset
)

// hashEnc selects the entry codec of a segment family, the second
// axis of the ladder's type parameterization beside the tag and subs:
// the doc 06 hash entry (variable value, optional field TTL), the doc
// 08 set entry (no value slot, no TTL), or the doc 09 zset member
// entry (the set header and member followed by the fixed 8-byte
// sortable score, no TTL). The two valueless-header codecs share
// every byte except the score trailer, so the set machinery carries
// the member side wholesale.
type hashEnc uint8

const (
	encHash hashEnc = iota
	encSet
	encZMem
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
//
// The set variants of both layouts, doc 08: same headers, same
// insertion-order inline region, same segment reuse, but entries are
// valueless (u8 eflags, u16 mlen, member; no vlen, no TTL), and the
// inline header's hflags carries the all-integer bit that backs the
// intset OBJECT ENCODING answer. The bit is one-way like Redis's
// intset conversion: the first non-integer member clears it for the
// key generation, and removals never restore it.
const (
	hashInlineHdrLen = 12
	hashEntryHdrLen  = 7
	setEntryHdrLen   = 3

	// zmemScoreLen is the zset member entry's fixed value: the 8-byte
	// big-endian sortable score image (zscore.go), the same bytes the
	// score runs fence on, so a score crosses the two families of doc
	// 09 without re-encoding.
	zmemScoreLen = 8

	hflagAnyTTL = 1 << 1
	hflagAllInt = 1 << 2
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
	case sub == listSubNoded:
		return TagList, false, nil
	case sub == setSubSeg:
		return TagSet, false, nil
	case sub == zsetSubSeg:
		return TagZset, false, nil
	case sub&0xF0 == inlineSubBase:
		t := sub & 0x0F
		if t >= TagString && t <= TagStream {
			return t, true, nil
		}
	}
	return 0, false, fmt.Errorf("sqlo1: unknown root sub %d", v[0])
}

// appendHashEntry encodes one field entry onto dst; expMs 0 means no
// field TTL. encSet takes the doc 08 set encoding: no vlen slot, and
// val and expMs must be absent (sets have no per-member payload or
// TTL, and the callers are structured so they never pass one). encZMem
// is the same header with val the fixed zmemScoreLen score after the
// member, expMs likewise absent. The caller has already bounded flen
// and vlen (the inline size guard does it structurally: an oversized
// field cannot fit the inline payload, and the segment slice owns its
// own guard).
func appendHashEntry(dst []byte, field, val []byte, expMs int64, enc hashEnc) []byte {
	if enc != encHash {
		var h [setEntryHdrLen]byte
		binary.LittleEndian.PutUint16(h[1:], uint16(len(field)))
		dst = append(dst, h[:]...)
		dst = append(dst, field...)
		return append(dst, val...)
	}
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

// hashEntrySize is the encoded size appendHashEntry produces, for the
// packers that budget a segment before encoding it. The valueless
// codecs share one arithmetic because their vlen is structural: 0 for
// encSet, zmemScoreLen for encZMem, and the caller passes the length
// of the value it holds either way.
func hashEntrySize(flen, vlen int, expMs int64, enc hashEnc) int {
	if enc != encHash {
		return setEntryHdrLen + flen + vlen
	}
	n := hashEntryHdrLen + flen + vlen
	if expMs != 0 {
		n += 8
	}
	return n
}

// hashEntryIter walks an encoded entry region; encSet walks the set
// encoding (val always nil, expMs always 0), encZMem the member
// encoding (val always the zmemScoreLen score bytes, expMs always 0).
// next returns aliases into the region; the caller owns their
// lifetime.
type hashEntryIter struct {
	p   []byte
	enc hashEnc
}

func (it *hashEntryIter) next() (field, val []byte, expMs int64, ok bool, err error) {
	if len(it.p) == 0 {
		return nil, nil, 0, false, nil
	}
	if it.enc != encHash {
		if len(it.p) < setEntryHdrLen {
			return nil, nil, 0, false, fmt.Errorf("sqlo1: set entry header short: %d bytes", len(it.p))
		}
		if it.p[0] != 0 {
			return nil, nil, 0, false, fmt.Errorf("sqlo1: set entry flags %#x has reserved bits set", it.p[0])
		}
		mlen := int(binary.LittleEndian.Uint16(it.p[1:]))
		n := setEntryHdrLen + mlen
		if it.enc == encZMem {
			n += zmemScoreLen
		}
		if len(it.p) < n {
			return nil, nil, 0, false, fmt.Errorf("sqlo1: set entry of %d bytes overruns its region (%d left)", n, len(it.p))
		}
		field = it.p[setEntryHdrLen : setEntryHdrLen+mlen]
		if it.enc == encZMem {
			val = it.p[setEntryHdrLen+mlen : n]
		}
		it.p = it.p[n:]
		return field, val, 0, true, nil
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
// raw entry region, which aliases the payload. allInt is the set
// ladder's intset bit; always false on hash roots.
type hashInline struct {
	count    int
	minExpMs int64
	allInt   bool
	entries  []byte
}

// putHashInlineHdr writes the 12-byte inline header into b. hflags is
// derived: the TTL bit tracks min_expire, the all-integer bit is the
// set ladder's one-way intset flag (always false for hashes, whose
// entries carry values).
func putHashInlineHdr(b []byte, sub uint8, count int, minExpMs int64, allInt bool) {
	b[0] = sub
	b[1] = 0
	if minExpMs != 0 {
		b[1] = hflagAnyTTL
	}
	if allInt {
		b[1] |= hflagAllInt
	}
	binary.LittleEndian.PutUint16(b[2:], uint16(count))
	binary.LittleEndian.PutUint64(b[4:], uint64(minExpMs))
}

// appendHashInlineHdr is putHashInlineHdr onto the end of dst.
func appendHashInlineHdr(dst []byte, sub uint8, count int, minExpMs int64, allInt bool) []byte {
	var b [hashInlineHdrLen]byte
	putHashInlineHdr(b[:], sub, count, minExpMs, allInt)
	return append(dst, b[:]...)
}

// decodeHashInline parses and fully validates an inline root payload
// of the given sub: the entry walk checks every header, and the entry
// count and region must agree exactly, so corruption fails at the root
// read instead of as a wrong scan later. The walk is O(payload), which
// the inline cap bounds at 2 KiB. encSet picks the set entry codec
// and admits the all-integer flag; a hash payload carrying it is
// corruption, as is a set payload carrying a TTL. encZMem admits no
// flags at all: no TTLs, and the intset bit is a set-only answer.
func decodeHashInline(p []byte, sub uint8, enc hashEnc) (hashInline, error) {
	if len(p) < hashInlineHdrLen {
		return hashInline{}, fmt.Errorf("sqlo1: inline root of %d bytes, header needs %d", len(p), hashInlineHdrLen)
	}
	if p[0] != sub {
		return hashInline{}, fmt.Errorf("sqlo1: root sub %d is not the inline sub %d", p[0], sub)
	}
	legal := uint8(hflagAnyTTL)
	switch enc {
	case encSet:
		legal = hflagAllInt
	case encZMem:
		legal = 0
	}
	if p[1]&^legal != 0 {
		return hashInline{}, fmt.Errorf("sqlo1: inline root flags %#x has reserved bits set", p[1])
	}
	h := hashInline{
		count:    int(binary.LittleEndian.Uint16(p[2:])),
		minExpMs: int64(binary.LittleEndian.Uint64(p[4:])),
		allInt:   p[1]&hflagAllInt != 0,
		entries:  p[hashInlineHdrLen:],
	}
	if (p[1]&hflagAnyTTL != 0) != (h.minExpMs != 0) {
		return hashInline{}, fmt.Errorf("sqlo1: inline root TTL flag disagrees with min_expire %d", h.minExpMs)
	}
	if h.count == 0 || h.count > hashInlineMaxCount {
		return hashInline{}, fmt.Errorf("sqlo1: inline root count %d outside [1, %d]", h.count, hashInlineMaxCount)
	}
	if len(p) > hashInlineMax {
		return hashInline{}, fmt.Errorf("sqlo1: inline root payload of %d bytes exceeds %d", len(p), hashInlineMax)
	}
	it := hashEntryIter{p: h.entries, enc: enc}
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
		return hashInline{}, fmt.Errorf("sqlo1: inline root claims %d entries, region holds %d", h.count, seen)
	}
	if minExp != h.minExpMs {
		return hashInline{}, fmt.Errorf("sqlo1: inline root min_expire %d, entries say %d", h.minExpMs, minExp)
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

	// The type parameterization, doc 08: the set ladder is this exact
	// machinery with valueless entries under its own subs and tag.
	// NewHash wires the hash constants, newSetLadder the set ones.
	tag       uint8
	subSeg    uint8
	subInline uint8
	enc       hashEnc

	// The current mint lease: counters [leaseNext, leaseEnd) are ours.
	leaseNext uint64
	leaseEnd  uint64

	// Reusable scratch. rootBuf builds replacement root payloads (the
	// old payload aliases an arena the write recycles, so every
	// rebuild finishes before the store call), segBuf and segBuf2 the
	// segment images an op writes, kbuf and kbuf2 the segment subkeys,
	// ents the parsed entries of a split or upgrade, fence the decoded
	// fence of the root under operation (paged: the one loaded page's
	// entries), pidx a paged root's page index, pageBuf the fence page
	// image a paged write lands.
	rootBuf []byte
	segBuf  []byte
	segBuf2 []byte
	kbuf    [SubkeySize]byte
	kbuf2   [SubkeySize]byte
	ents    []hashSegEntry
	fence   []hashFenceEnt
	pidx    []hashPageEnt
	pageBuf []byte

	// segRoot is the decoded root stateOf leaves behind at
	// hashSegState; its fence rides the fence scratch. Valid until
	// the next stateOf. tailBuf holds the copied-out zset tail so the
	// decoded root survives the segment reads that recycle the arena
	// bytes the decode aliased.
	segRoot hashSegRoot
	tailBuf []byte

	// deferRoot is the zset dual write's one-root-per-command switch:
	// while set, writeSegRoot records rootPend instead of landing an
	// image, and the command wrapper writes the root once at the end,
	// full-frame. The zset claims rollback replay (RollbackRef), whose
	// commit point is the plane's last root frame, so a dual command
	// must emit exactly one root frame and emit it after every record
	// it references; the hot tier's re-dirtied-root deferral makes the
	// after hold in drain order. Never set on hash or set ladders,
	// whose W1-W3 discipline prices each root write individually.
	deferRoot bool
	rootPend  bool

	// valBuf carries a point op's value copy across the mutation that
	// would recycle the arena bytes it read (HGETDEL, HGETEX, the
	// INCR family's formatted result).
	valBuf []byte

	// HMGET scratch: per-field batch slots, the unique segids in
	// first-need order, the segment subkeys and their backing bytes,
	// the batch outputs, and the decoded segments.
	mgIdx    []int
	mgUniq   []uint64
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

	// ExpireHook, when set, fires after a write landed a hash root
	// whose min_expire differs from the previous root's, with the new
	// value (0 means no field TTLs remain, including the key dying).
	// This is the doc 11 registration door: the expiry loop files the
	// key into its wheel here, keyed by the root min, and Reap walks
	// the due segments through ReapDue. key aliases the caller's
	// argument and is only valid during the call.
	ExpireHook func(key []byte, minExpMs int64)
}

// fireMin runs the registration hook on a root min_expire change.
func (h *Hash) fireMin(key []byte, pre, post int64) {
	if h.ExpireHook != nil && pre != post {
		h.ExpireHook(key, post)
	}
}

// NewHash builds the hash layer over t.
func NewHash(t *Tiered, cfg HashConfig) (*Hash, error) {
	h, err := newSegLadder(t, cfg)
	if err != nil {
		return nil, err
	}
	h.tag, h.subSeg, h.subInline = TagHash, hashSubSeg, hashSubInline
	return h, nil
}

// newSegLadder is the shared constructor under NewHash and NewSet; the
// caller stamps the type parameterization.
func newSegLadder(t *Tiered, cfg HashConfig) (*Hash, error) {
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
	if tag != h.tag {
		return hashAbsent, hashInline{}, 0, ErrWrongType
	}
	if v[0] == h.subSeg {
		h.segRoot, err = decodeHashSegRoot(v, h.fence[:0], h.pidx[:0])
		if err != nil {
			return hashAbsent, hashInline{}, 0, err
		}
		h.fence = h.segRoot.fence
		h.pidx = h.segRoot.pidx
		if len(h.segRoot.tail) > 0 {
			h.tailBuf = append(h.tailBuf[:0], h.segRoot.tail...)
			h.segRoot.tail = h.tailBuf
		}
		return hashSegState, hashInline{}, expMs, nil
	}
	hi, err := decodeHashInline(v, h.subInline, h.enc)
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
	return decodeHashSeg(v, h.enc)
}

// writeSeg writes a segment image under the current root's plane.
func (h *Hash) writeSeg(ctx context.Context, segid uint64, payload []byte) error {
	putHashSegKey(h.kbuf[:], h.segRoot.rooth, segid)
	return h.t.SetGen(ctx, h.kbuf[:], payload, h.tag, h.segRoot.rootgen)
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
	if h.deferRoot {
		h.rootPend = true
		return nil
	}
	h.rootBuf = appendHashSegRoot(h.rootBuf[:0], &h.segRoot)
	tag := h.tag | TagRoot
	if delta {
		tag |= TagDelta
	}
	return h.t.Set(ctx, key, h.rootBuf, tag)
}

// loadPage makes page j of a paged root the loaded page: r.fence (and
// h.fence, the same slice) become the page's entries and r.pi its
// index. A no-op on a flat root or when j is already loaded, so the
// page cache is one page wide and lives within one op. The decode
// copies out of the read, so the entries stay valid across the
// segment reads that follow. Entries at or past the next page's lo
// are clipped: a page split trims the low page after the root lands,
// so a crash can leave a stale tail the index already routes to the
// high page, the same dead-bytes rule a split segment's untrimmed
// image rides.
func (h *Hash) loadPage(ctx context.Context, j int) error {
	r := &h.segRoot
	if !r.paged || r.pi == j {
		return nil
	}
	e := r.pidx[j]
	putHashFenceKey(h.kbuf[:], r.rooth, e.pageid)
	v, ok, err := h.t.Get(ctx, h.kbuf[:])
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("sqlo1: hash fence page %d of rooth %#x is missing", e.pageid, r.rooth)
	}
	ents, err := decodeHashFencePage(v, h.fence[:0], r.nextSegid)
	if err != nil {
		return err
	}
	if ents[0].lo != e.lo {
		return fmt.Errorf("sqlo1: hash fence page %d starts at %#x, index says %#x", e.pageid, ents[0].lo, e.lo)
	}
	if j+1 < len(r.pidx) {
		hi := r.pidx[j+1].lo
		for len(ents) > 0 && ents[len(ents)-1].lo >= hi {
			ents = ents[:len(ents)-1]
		}
	}
	if len(ents) == 0 {
		return fmt.Errorf("sqlo1: hash fence page %d of rooth %#x is empty after clipping", e.pageid, r.rooth)
	}
	h.fence = ents
	r.fence = ents
	r.pi = j
	return nil
}

// fenceIdx resolves the fence entry covering fh, loading the covering
// page first on a paged root. The returned index is into r.fence: the
// whole fence flat, the loaded page's entries paged.
func (h *Hash) fenceIdx(ctx context.Context, fh uint64) (int, error) {
	r := &h.segRoot
	if r.paged {
		if err := h.loadPage(ctx, hashPageFind(r.pidx, fh)); err != nil {
			return 0, err
		}
	}
	return hashFenceFind(r.fence, fh), nil
}

// writeFencePage lands the loaded page's current entries and
// refreshes its index weight, so the root write that follows carries
// the fresh weight. Callers order it before that root write; the two
// ride one drain batch (no Flush between), which is what lets replay
// treat page frames as neutral.
func (h *Hash) writeFencePage(ctx context.Context) error {
	r := &h.segRoot
	h.pageBuf = appendHashFencePage(h.pageBuf[:0], r.fence)
	putHashFenceKey(h.kbuf2[:], r.rooth, r.pidx[r.pi].pageid)
	r.pidx[r.pi].weight = hashPageWeight(r.fence)
	return h.t.SetGen(ctx, h.kbuf2[:], h.pageBuf, h.tag|TagFence, r.rootgen)
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
// and HGETEX passes the new one. Replacing an entry that already
// expired answers created (the dead field was never observable) while
// the count treats it as an update, since the dead entry was counted.
func (h *Hash) hset(ctx context.Context, key, field, val []byte, entryExp int64) (bool, error) {
	st, hi, expMs, err := h.stateOf(ctx, key)
	if err != nil {
		return false, err
	}
	switch st {
	case hashSegState:
		pre := h.segRoot.minExpMs
		created, err := h.hsetSeg(ctx, key, field, val, expMs, entryExp)
		if err == nil {
			h.fireMin(key, pre, h.segRoot.minExpMs)
		}
		return created, err
	case hashAbsent:
		allInt := h.enc == encSet && isCanonicalInt(field)
		h.rootBuf = appendHashInlineHdr(h.rootBuf[:0], h.subInline, 1, entryExp, allInt)
		h.rootBuf = appendHashEntry(h.rootBuf, field, val, entryExp, h.enc)
		if len(h.rootBuf) > hashInlineMax {
			created, err := h.upgrade(ctx, key, h.rootBuf[hashInlineHdrLen:], true, expMs)
			if err == nil {
				h.fireMin(key, 0, h.segRoot.minExpMs)
			}
			return created, err
		}
		if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
			return false, err
		}
		h.fireMin(key, 0, entryExp)
		return true, h.restamp(ctx, key, expMs)
	}
	// Rebuild the payload around the one field. The region is walked
	// once: matching entry replaced in place (its raw span dropped,
	// the new entry encoded), everything else copied span for span.
	// The header slot is filled last, when count and min_expire are
	// known.
	h.rootBuf = grow(h.rootBuf, hashInlineHdrLen)
	it := hashEntryIter{p: hi.entries, enc: h.enc}
	now := h.t.Now()
	created := true
	revived := false
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
			h.rootBuf = appendHashEntry(h.rootBuf, field, val, entryExp, h.enc)
			created = false
			revived = eExp != 0 && eExp <= now
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
		h.rootBuf = appendHashEntry(h.rootBuf, field, val, entryExp, h.enc)
		count++
	}
	if count > hashInlineMaxCount || len(h.rootBuf) > hashInlineMax {
		wire, err := h.upgrade(ctx, key, h.rootBuf[hashInlineHdrLen:], created || revived, expMs)
		if err == nil {
			h.fireMin(key, hi.minExpMs, h.segRoot.minExpMs)
		}
		return wire, err
	}
	// The intset bit is one-way, Redis's own conversion rule: an
	// existing key generation keeps it only while every added member
	// stays integer-shaped, and removals never restore it.
	allInt := hi.allInt && (!created || isCanonicalInt(field))
	putHashInlineHdr(h.rootBuf, h.subInline, count, minExp, allInt)
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return false, err
	}
	h.fireMin(key, hi.minExpMs, minExp)
	return created || revived, h.restamp(ctx, key, expMs)
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
	h.ents, err = parseHashSegEntries(h.ents[:0], region, h.enc)
	if err != nil {
		return false, err
	}
	sortHashSegEntries(h.ents)

	rooth, err := h.nextRooth(ctx)
	if err != nil {
		return false, err
	}
	h.segRoot = hashSegRoot{sub: h.subSeg, rootgen: 1, rooth: rooth, count: uint64(len(h.ents))}

	// Pack sorted entries into segments up to seg_max, never cutting
	// between equal fh values (the fence could not separate them).
	// The upgrade image is barely past the inline cap so this is one
	// segment in practice, but the packer is general. cuts[i] ends
	// segment i.
	cuts := make([]int, 0, 2)
	size := hashSegHdrLen
	start := 0
	for i, e := range h.ents {
		es := hashEntrySize(len(e.field), len(e.val), e.expMs, h.enc)
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
		h.segBuf = appendHashSegPayload(h.segBuf, seg, h.enc)
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
	i, err := h.fenceIdx(ctx, f)
	if err != nil {
		return false, err
	}
	s, err := h.readSeg(ctx, r.fence[i].segid)
	if err != nil {
		return false, err
	}
	out, created, revived, err := hashSegSet(h.segBuf, s, f, field, val, entryExp, h.t.Now())
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
		h.ents, err = parseHashSegEntries(h.ents[:0], out[hashSegHdrLen:], h.enc)
		if err != nil {
			return false, err
		}
		if mid, boundary, ok := splitHashSegEntries(h.ents, r.fence[i].lo); ok {
			return h.splitSeg(ctx, key, i, mid, boundary, created, revived, expMs)
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
			return revived, nil
		}
		// A TTL edit moved the fence meta or the root min: skipping
		// the root here would leave min_expire stale-late, so this
		// write pins it like a cardinality change would (rule W1).
		if err := h.setFenceMeta(ctx, i, meta); err != nil {
			return false, err
		}
		if err := h.writeSegRoot(ctx, key, true); err != nil {
			return false, err
		}
		return revived, h.restamp(ctx, key, expMs)
	}
	r.count++
	if err := h.setFenceMeta(ctx, i, hashSegMeta(int(binary.LittleEndian.Uint16(out)), segMin)); err != nil {
		return false, err
	}
	if err := h.writeSegRoot(ctx, key, true); err != nil {
		return false, err
	}
	return true, h.restamp(ctx, key, expMs)
}

// setFenceMeta updates fence entry i's metadata, landing the loaded
// page's image first when the edit actually moved it on a paged root
// (about one create in sixteen crosses a fill-class step). The fence
// meta is advisory, so a root whose page write was skipped is still
// exact where it matters; the page and root that do land ride one
// batch and stay a delta pair under rule W2.
func (h *Hash) setFenceMeta(ctx context.Context, i int, meta uint16) error {
	r := &h.segRoot
	if r.fence[i].meta == meta {
		return nil
	}
	r.fence[i].meta = meta
	if !r.paged {
		return nil
	}
	return h.writeFencePage(ctx)
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
//
// The fence edit is where the paging ladder climbs, doc 06 section
// 2.3. A flat fence at hashFenceMaxSegs transitions: the whole fence
// (with the new entry) becomes page 0, which lands and flushes before
// the root that flips paged mode, so a crash prefix reads the flat
// root and the page sits orphaned until compaction. A full page
// splits the same way a segment does: the high half lands under a
// fresh pageid and flushes before the root whose index gains the
// entry, and the low half's trim lands after the root, dead bytes
// loadPage clips. A split inside a page with room writes the page
// beside the root in one batch, no barrier needed. Every one of these
// writes a full (non-delta) root: they all mint from nextSegid, which
// replay reconciliation can never patch, so the frame must be the
// root's own.
func (h *Hash) splitSeg(ctx context.Context, key []byte, i, mid int, boundary uint64, created, revived bool, expMs int64) (bool, error) {
	r := &h.segRoot
	transition := !r.paged && len(r.fence) >= hashFenceMaxSegs
	pageSplit := r.paged && len(r.fence) >= hashFencePageMax
	if pageSplit && len(r.pidx) >= hashFencePageIdxMax {
		return false, errHashFenceThirdLevel
	}
	ids := uint64(1)
	if transition || pageSplit {
		ids = 2
	}
	if r.nextSegid+ids-1 > hashFenceSegidMax {
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
	h.segBuf2 = appendHashSegPayload(h.segBuf2, h.ents[mid:], h.enc)
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
	// The trimmed low-seg image below needs the surviving entry's
	// segid; a page split may cut r.fence before it happens.
	survivor := r.fence[i].segid

	trimTo := -1
	switch {
	case transition:
		pageid := r.nextSegid
		r.nextSegid++
		h.pageBuf = appendHashFencePage(h.pageBuf[:0], r.fence)
		putHashFenceKey(h.kbuf2[:], r.rooth, pageid)
		if err := h.t.SetGen(ctx, h.kbuf2[:], h.pageBuf, h.tag|TagFence, r.rootgen); err != nil {
			return false, err
		}
		if err := h.t.Flush(ctx); err != nil {
			return false, err
		}
		h.pidx = append(h.pidx[:0], hashPageEnt{pageid: pageid, weight: hashPageWeight(r.fence)})
		r.pidx = h.pidx
		r.paged = true
		r.pi = 0
	case pageSplit:
		pm := len(r.fence) / 2
		pageid := r.nextSegid
		r.nextSegid++
		h.pageBuf = appendHashFencePage(h.pageBuf[:0], r.fence[pm:])
		putHashFenceKey(h.kbuf2[:], r.rooth, pageid)
		if err := h.t.SetGen(ctx, h.kbuf2[:], h.pageBuf, h.tag|TagFence, r.rootgen); err != nil {
			return false, err
		}
		if err := h.t.Flush(ctx); err != nil {
			return false, err
		}
		r.pidx = slices.Insert(r.pidx, r.pi+1, hashPageEnt{
			lo:     r.fence[pm].lo,
			pageid: pageid,
			weight: hashPageWeight(r.fence[pm:]),
		})
		h.pidx = r.pidx
		r.pidx[r.pi].weight = hashPageWeight(r.fence[:pm])
		trimTo = pm
	case r.paged:
		if err := h.writeFencePage(ctx); err != nil {
			return false, err
		}
	}
	if err := h.writeSegRoot(ctx, key, false); err != nil {
		return false, err
	}
	if trimTo >= 0 {
		// The low page's trim mirrors the low segment's: after the
		// root, dead entries loadPage clips until then.
		r.fence = r.fence[:trimTo]
		h.fence = r.fence
		if err := h.writeFencePage(ctx); err != nil {
			return false, err
		}
	}

	h.segBuf2 = appendHashSegPayload(h.segBuf2[:0], h.ents[:mid], h.enc)
	if err := h.writeSeg(ctx, survivor, h.segBuf2); err != nil {
		return false, err
	}
	return created || revived, h.restamp(ctx, key, expMs)
}

// hdelSeg removes one field of a segmented hash. Every removal
// changes the count, so the root is always written (rule W1); the
// removal that empties the whole hash deletes the key and retires the
// plane in O(1), exactly like a cross-type overwrite. A removal that
// shrinks the segment enough tries the lazy merge with a neighbor.
func (h *Hash) hdelSeg(ctx context.Context, key, field []byte, expMs int64) (bool, error) {
	r := &h.segRoot
	f := hashFH(field)
	i, err := h.fenceIdx(ctx, f)
	if err != nil {
		return false, err
	}
	s, err := h.readSeg(ctx, r.fence[i].segid)
	if err != nil {
		return false, err
	}
	out, removed, err := hashSegDel(h.segBuf, s, f, field, h.t.Now())
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
		if err == nil {
			h.fireMin(key, r.minExpMs, 0)
		}
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
	meta := hashSegMeta(int(binary.LittleEndian.Uint16(out)), int64(binary.LittleEndian.Uint64(out[4:])))
	if err := h.setFenceMeta(ctx, i, meta); err != nil {
		return false, err
	}
	if err := h.writeSegRoot(ctx, key, true); err != nil {
		return false, err
	}
	return true, h.restamp(ctx, key, expMs)
}

// hdelSegBatch removes a batch of fields from a segmented hash, every
// one of which must be live: the range-trim path collected them from
// the walked window, so a miss is corruption. Fields sort by fh so
// the fields of one segment are contiguous, and each touched segment
// rewrites once; every group resolves its fence index fresh, so a
// lazy merge behind one group can never shift the next. The caller
// owns the deferred root, guarantees the batch never empties the
// hash, and lands the root and restamp with its flush.
func (h *Hash) hdelSegBatch(ctx context.Context, key []byte, fields [][]byte) error {
	r := &h.segRoot
	if uint64(len(fields)) >= r.count {
		return fmt.Errorf("sqlo1: batch delete of %d fields would empty rooth %#x", len(fields), r.rooth)
	}
	slices.SortFunc(fields, func(a, b []byte) int {
		fa, fb := hashFH(a), hashFH(b)
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}
		return bytes.Compare(a, b)
	})
	for i := 0; i < len(fields); {
		si, err := h.fenceIdx(ctx, hashFH(fields[i]))
		if err != nil {
			return err
		}
		j := i + 1
		for ; j < len(fields); j++ {
			sj, err := h.fenceIdx(ctx, hashFH(fields[j]))
			if err != nil {
				return err
			}
			if sj != si {
				break
			}
		}
		batch := make(map[string]bool, j-i)
		for _, f := range fields[i:j] {
			batch[string(f)] = true
		}
		s, err := h.readSeg(ctx, r.fence[si].segid)
		if err != nil {
			return err
		}
		out, removed, err := hashSegDelMulti(h.segBuf, s, batch, h.t.Now())
		if err != nil {
			return err
		}
		if removed != j-i {
			return fmt.Errorf("sqlo1: batch delete found %d of %d fields in segment %d of rooth %#x",
				removed, j-i, r.fence[si].segid, r.rooth)
		}
		h.segBuf = out
		r.count -= uint64(removed)
		merged, err := h.tryMergeSeg(ctx, key, si, out)
		if err != nil {
			return err
		}
		if !merged {
			if err := h.writeSeg(ctx, r.fence[si].segid, out); err != nil {
				return err
			}
			meta := hashSegMeta(int(binary.LittleEndian.Uint16(out)), int64(binary.LittleEndian.Uint64(out[4:])))
			if err := h.setFenceMeta(ctx, si, meta); err != nil {
				return err
			}
			if err := h.writeSegRoot(ctx, key, true); err != nil {
				return err
			}
		}
		i = j
	}
	return nil
}

// tryMergeSeg is the lazy merge, doc 06 section 2.1: segment i just
// shrank (its unwritten post-image is out, aliasing h.segBuf), and if
// the merged encoding with a neighbor stays under seg_min the two
// fold into the lower side's segment. The crash order needs no
// barrier: merged image to the low subkey first (a crash here leaves
// entries past the low entry's fence range, dead bytes point reads
// route around), then the root that drops the high entry, then the
// high subkey's delete (a crash before it leaves a bounded orphan the
// plane retire cleans). On a paged root the neighbor bounds are the
// loaded page's, so merges never cross a page boundary: the first
// entry of a page keeps its slack against the previous page's last,
// bounded v0 looseness the fill classes already tolerate.
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
		h.segBuf2, err = mergeHashSegs(h.segBuf2, lop, hip, h.enc)
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
		if r.paged {
			// The shrunken page rides the root's batch, like the
			// in-page split: the root is full-frame, so replay never
			// needs to reconcile across the pair.
			if err := h.writeFencePage(ctx); err != nil {
				return false, err
			}
		}
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
// of every point op that must preserve or edit an entry expiry. An
// entry past its expire_ms is absent (lazy expiry, doc 06 section 4);
// the bytes stay in place for the reaper, but no read path returns
// them. The value aliases internal buffers and dies on the next
// Tiered call.
func (h *Hash) getEntry(ctx context.Context, key, field []byte) ([]byte, int64, bool, error) {
	v, eExp, ok, err := h.getEntryRaw(ctx, key, field)
	if ok && eExp != 0 && eExp <= h.t.Now() {
		return nil, 0, false, nil
	}
	return v, eExp, ok, err
}

// getEntryRaw is getEntry without the expiry filter: the reap and TTL
// paths that must see a dead entry's bytes read through here.
func (h *Hash) getEntryRaw(ctx context.Context, key, field []byte) ([]byte, int64, bool, error) {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return nil, 0, false, err
	}
	switch st {
	case hashAbsent:
		return nil, 0, false, nil
	case hashSegState:
		f := hashFH(field)
		i, err := h.fenceIdx(ctx, f)
		if err != nil {
			return nil, 0, false, err
		}
		s, err := h.readSeg(ctx, h.segRoot.fence[i].segid)
		if err != nil {
			return nil, 0, false, err
		}
		return hashSegGet(s, f, field)
	}
	it := hashEntryIter{p: hi.entries, enc: h.enc}
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

// HDel removes one field and reports whether it was there; a field
// past its expiry was never there (lazy expiry), so it answers false
// and stays for the reaper. Deleting the last field deletes the key,
// which is Redis's empty-hash rule.
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
	it := hashEntryIter{p: hi.entries, enc: h.enc}
	now := h.t.Now()
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
		if bytes.Equal(f, field) && (eExp == 0 || eExp > now) {
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
		if err == nil {
			h.fireMin(key, hi.minExpMs, 0)
		}
		return true, err
	}
	putHashInlineHdr(h.rootBuf, h.subInline, hi.count-1, minExp, hi.allInt)
	if err := h.t.Set(ctx, key, h.rootBuf, h.tag|TagRoot); err != nil {
		return false, err
	}
	h.fireMin(key, hi.minExpMs, minExp)
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
// the inline tier, listpackex when the inline tier holds a field TTL,
// hashtable past it, doc 06 section 1's parity rule. The listpackex
// answer probes the live min rather than remembering a conversion, so
// unlike Redis it reverts to listpack when the last TTL is persisted
// away; the compat manifest records that corner as a standing
// divergence.
func (h *Hash) Encoding(ctx context.Context, key []byte) (string, bool, error) {
	st, hi, _, err := h.stateOf(ctx, key)
	if err != nil {
		return "", false, err
	}
	switch st {
	case hashAbsent:
		return "", false, nil
	case hashSegState:
		return "hashtable", true, nil
	}
	if hi.minExpMs != 0 {
		return "listpackex", true, nil
	}
	return "listpack", true, nil
}
