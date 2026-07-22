package set

// Inline-set arena codec (spec 2064/f3/11, keyspace-unification arc).
//
// A tiny set (intset or listpack class, set.go) is one packed blob plus a
// one-byte encoding tag and a deadline. That is little enough to live where a
// small string value already does: inline in one store arena record via
// store.PutCollBlob, discriminated by the kindSet collection byte, instead of
// as three separate Go-heap objects (the 80-byte header struct, the blob, and
// the registry map entry). This file is the codec that maps a tiny set to and
// from that record's two carriers, the packed value blob and the sixteen-bit
// per-record bits field, and back. It is pure and caller-free: the routing
// slice that homes tiny sets in the arena builds on it.
//
// The blob carried in the record is exactly the set's packed data (set.data):
// sorted little-endian int64 lanes for the intset class, length-prefixed
// entries for the listpack class. The bits field carries the one bit that the
// blob alone cannot recover, the intset-versus-listpack encoding, in its low
// bit. The listpack entry count (set.n) is NOT carried; it is recomputed by
// one bounded walk of the blob on load (countListpackEntries), which frees the
// remaining fifteen bits of the record's bits word for the per-key idle clock
// the routing slice rides there, the same free-header trick the string cell
// plays with its own record bits.

// inlineEncMask selects the encoding bit in a collection record's bits word.
// The routing slice packs the idle clock into the bits above it, so every read
// of the encoding masks rather than assuming the high bits are zero.
const inlineEncMask uint16 = 0x0001

// inlineEligible reports whether s is in a class the arena embeds inline: the
// intset and listpack bands, the two blob-backed shapes. A set that has
// escalated to the native table or the partitioned band (set.go) is no longer
// a single packed blob and stays in the Go-heap registry, so it is not
// eligible.
func inlineEligible(s *set) bool {
	return s.enc == encIntset || s.enc == encListpack
}

// inlineBits packs the carry bits for an inline set: its encoding in the low
// bit. The caller is responsible for inline eligibility; a non-inline encoding
// would not round-trip. The routing slice ORs the idle clock into the high
// bits after this, so this returns only the encoding contribution.
func inlineBits(s *set) uint16 {
	return uint16(s.enc) & inlineEncMask
}

// encFromBits recovers an inline set's encoding from a record's bits word,
// masking off whatever the routing slice rode in the high bits (the idle
// clock). It answers encIntset or encListpack, the only two the low bit spans.
func encFromBits(bits uint16) encoding {
	return encoding(bits & inlineEncMask)
}

// loadInline reconstructs the set at dst from an arena record's carriers: the
// packed blob, the record's bits word, and its deadline. It reuses dst's data
// capacity so a per-shard scratch set reloads across commands without
// allocating, and copies the blob in rather than aliasing the arena, so a
// mutation on dst never scribbles the stored record before the routing slice
// commits it back. The escalated-band pointers are cleared: an inline record
// never carries a table or a partitioned set. The listpack count is recomputed
// from the blob (the intset count derives from the blob length), so the bits
// word spends no room on it.
func loadInline(dst *set, blob []byte, bits uint16, expireAt int64) {
	dst.enc = encFromBits(bits)
	dst.data = append(dst.data[:0], blob...)
	dst.expireAt = expireAt
	dst.ht = nil
	dst.part = nil
	dst.cold = nil
	dst.acct = 0
	if dst.enc == encListpack {
		dst.n = countListpackEntries(dst.data)
	} else {
		dst.n = 0
	}
}

// countListpackEntries counts the entries in a packed listpack blob by walking
// its length-prefixed records, each [len:uint8][tag:uint8][bytes]. The walk is
// bounded by the listpack entry cap (maxListpackEntries), so it is O(members)
// over a set that is small by construction. It is how loadInline recovers
// set.n without spending a record bits field on the count.
func countListpackEntries(blob []byte) int {
	n := 0
	for i := 0; i < len(blob); {
		i += 2 + int(blob[i])
		n++
	}
	return n
}
