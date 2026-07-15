package store

import (
	"encoding/binary"
	"math/bits"
)

// Hash is the wyhash-style word-at-a-time mix the whole engine keys on: it is
// computed once per command at parse time and reused for shard routing, the
// directory lookup, the in-segment bucket choice, and the entry tag. It reads
// eight key bytes per step and finishes a short key in one or two multiplies,
// so the table probe is not gated on a long scalar hash. The output is internal
// to one process and matched against nothing outside it, so the only
// requirement is that one run is self-consistent.
func Hash(b []byte) uint64 {
	const (
		s0 = 0xa0761d6478bd642f
		s1 = 0xe7037ed1a0b428db
		s2 = 0x8ebc6af09c88c6e3
	)
	h := s0 ^ uint64(len(b))
	for len(b) >= 8 {
		h = mulFold(h^binary.LittleEndian.Uint64(b), s1)
		b = b[8:]
	}
	if len(b) > 0 {
		var t uint64
		for i := 0; i < len(b); i++ {
			t |= uint64(b[i]) << (8 * uint(i))
		}
		h = mulFold(h^t, s1)
	}
	return mulFold(h, s2)
}

func mulFold(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

// An index entry packs a 48-bit logical arena address, two tier bits, two heat
// bits, and a 12-bit tag into one word (spec 2064/f3/04 section 2.1). The tag
// comes from the top of the hash, so it is disjoint from the in-segment bucket
// bits by construction.
const (
	addrBits  = 48
	addrMask  = (uint64(1) << addrBits) - 1
	tierShift = 48 // bits 48..49: 00 resident, 01 cold, 10/11 reserved
	heatShift = 50 // bits 50..51: SIEVE-shaped access signal for the demotion scan
	tagShift  = 52 // bits 52..63: the probe's fast reject
)

// The tier field (bits 48..49) says where the entry's record lives. Resident
// is the byte-identical hot path: the address is an arena offset. Cold means
// the record was demoted to the shard's cold region (cold.go) and the address
// is a cold-frame offset there, resolved by one pread. 10 and 11 stay reserved.
const (
	tierMask     = uint64(3)
	tierResident = uint64(0)
	tierCold     = uint64(1)
)

// tierOf reads an entry word's tier field.
func tierOf(w uint64) uint64 { return (w >> tierShift) & tierMask }

// slotCold reports whether an entry word names a cold-region frame rather than
// an arena record. Every path that would dereference w&addrMask as an arena
// offset tests this first: a cold word's address is meaningless in the arena.
func slotCold(w uint64) bool { return tierOf(w) == tierCold }

// The heat field (bits 50..51) is the whole-record migrator's SIEVE access
// signal (spec 2064/f3/06 section 4.2). A resident read sets the visited bit in
// the index word, one store to the line the probe already loaded; the migrator's
// demotion scan reads it and gives a visited record one second chance, clearing
// the bit so the record must be re-read to survive the next pass. The bit is
// meaningful only for a resident entry: a cold entry carries doorkeeper state
// instead, and every demote flip clears it so a bring-up re-earns its heat. The
// second heat bit stays reserved for the LFU counter a later slice may add.
const (
	heatMask    = uint64(3)
	heatVisited = uint64(1) << heatShift // bit 50: read since the last migrator pass
)

// slotVisited reports whether the entry word's visited bit is set.
func slotVisited(w uint64) bool { return w&heatVisited != 0 }

// clearHeat zeros the heat field, keeping the address, tier, and tag. Used at a
// demote flip (a cold entry carries no heat) and when the migration hand spends a
// record's second chance.
func clearHeat(w uint64) uint64 { return w &^ (heatMask << heatShift) }

// tagOf takes the high 12 bits of the hash for the entry tag, the fast reject
// that skips a slot without touching the arena. The |1 keeps it non-zero so a
// live entry word can never read as the empty-slot sentinel even at address
// zero.
func tagOf(h uint64) uint64 { return (h >> tagShift) | 1 }
