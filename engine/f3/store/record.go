package store

import "encoding/binary"

// The record frame (spec 2064/f3/04 section 3.1): a 16-byte header, an
// optional 8-byte expiry, the key bytes, then the payload at the next 8-byte
// boundary past the key. Every field is written and read with plain loads and
// stores: the owner is the only toucher, so there is no latch, no version
// bracket, and no atomic anywhere in the frame. The ver word survives as a
// committed marker for off-owner cold-path readers (snapshot cuts, the
// migrator): even means committed, and the owner's hot path only ever
// plain-stores it at publish.
const (
	hdrSize = 16

	offVer      = 0  // u32, even = committed
	offVlen     = 4  // u32, current value byte length
	offKlen     = 8  // u16, key byte length, immutable
	offVcap     = 10 // u16, reserved value capacity in 8-byte words, immutable
	offKind     = 12 // u8, record kind
	offFlags    = 13 // u8, record flags
	offKindBits = 14 // u16, kind-specific small fields

	flagHasTTL  = 1 << 0 // an 8-byte absolute unix-ms expiry follows the header
	flagSep     = 1 << 1 // payload is a value-log pointer, not the value bytes
	flagChunked = 1 << 2 // payload is a chunk-extent table
	flagDead    = 1 << 3 // superseded; counted in the segment's dead bytes

	// flagInt marks the V_INT band (doc 09 section 2): the value area holds a
	// raw 8-byte int64 cell and vlen carries the decimal digit count, so
	// STRLEN and reply presizing read vlen uniformly across bands.
	flagInt = 1 << 4

	// flagRawSticky records that APPEND or SETRANGE touched the value, the
	// F_RAWSTICKY bit OBJECT ENCODING will read once that shim lands.
	flagRawSticky = 1 << 5

	// flagVisited is the residency clock bit (resid.go): on a resident
	// separated run it is the SIEVE second-chance mark a read sets and the
	// demotion hand clears, and on a log-resident run it is the promotion
	// doorkeeper's first-touch mark. Owner-only plain stores, like every
	// other header bit.
	flagVisited = 1 << 6

	// flagMigrating marks a record the async migrator has staged into an
	// in-flight cold drain but not yet flipped (coldstage.go). It stays resident
	// and fully readable through the phase-1-to-phase-2 window; the bit exists so
	// a foreground write that reaches the record before the drain completes can
	// cancel the migration in place (findResident bumps the record's version and
	// clears this bit), which is what turns doc 06 section 3.1's stale-flip check
	// into a version compare the in-place-overwrite path cannot fool. Owner-only,
	// set nowhere unless a drain is in flight, so the L9 no-pressure path never
	// sees it.
	flagMigrating = 1 << 7

	// kindString is the plain string key record. Non-zero on purpose: a
	// zero kind byte in a reused, unscrubbed arena offset must never read as a
	// valid record kind.
	kindString = 0x01

	// The collection kinds. A collection record embeds a whole small collection's
	// packed bytes in the value area, the tiny-collection form the
	// keyspace-unification arc folds the per-type Go-heap registries into: one
	// arena record, no header struct, no separate data slice, no map entry, no
	// GC-scanned object. The kind byte discriminates the packed shape; the packed
	// bytes and the per-collection small fields carried in offKindBits are the
	// owning type's to interpret. They occupy a contiguous range so isCollKind is
	// a bounds test.
	kindSet    = 0x02
	kindHash   = 0x03
	kindList   = 0x04
	kindZSet   = 0x05
	kindStream = 0x06

	// maxKey and maxVal are the 64KiB field widths klen and the vcap word
	// count can express. Keys keep this cap for good; values at and past
	// 64KiB move to the chunked band in a later slice, and until it lands
	// this is the value's hard cap too.
	maxKey = 0xffff
	maxVal = 0xffff
)

func align8(n uint64) uint64 { return (n + 7) &^ 7 }

// keyStart is the key's offset within the record: past the header, and past
// the expiry word when the record carries one.
func (s *Store) keyStart(off uint64) uint64 {
	start := off + hdrSize
	if s.arena.buf[off+offFlags]&flagHasTTL != 0 {
		start += 8
	}
	return start
}

func (s *Store) klen(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(s.arena.buf[off+offKlen:]))
}

func (s *Store) vlen(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint32(s.arena.buf[off+offVlen:]))
}

func (s *Store) vcapBytes(off uint64) uint64 {
	return uint64(binary.LittleEndian.Uint16(s.arena.buf[off+offVcap:])) * 8
}

// valueStart is the payload's offset: the key rounded up to the next 8-byte
// boundary, so the reserved capacity is whole words and an 8-byte counter
// placed at the payload start stays aligned.
func (s *Store) valueStart(off uint64) uint64 {
	return s.keyStart(off) + align8(s.klen(off))
}

// keyAt returns the record's key bytes. They are immutable for the record's
// life, so the returned slice is stable until the record's segment is freed.
func (s *Store) keyAt(off uint64) []byte {
	start := s.keyStart(off)
	return s.arena.buf[start : start+s.klen(off)]
}

// recordMatches reports whether the record at off carries this key. Only
// immutable fields are read.
func (s *Store) recordMatches(off uint64, key []byte) bool {
	if s.klen(off) != uint64(len(key)) {
		return false
	}
	start := s.keyStart(off)
	return string(s.arena.buf[start:start+uint64(len(key))]) == string(key)
}

// recBytes is the arena bytes the allocator charged for the record at off:
// header, expiry slot if any, aligned key, and the reserved value capacity.
// Charging this back when the record leaves the index cancels the allocation
// charge exactly, so a segment's live counter reaches zero when its last
// record goes.
func (s *Store) recBytes(off uint64) uint64 {
	n := hdrSize + align8(s.klen(off)) + s.vcapBytes(off)
	if s.arena.buf[off+offFlags]&flagHasTTL != 0 {
		n += 8
	}
	return n
}
