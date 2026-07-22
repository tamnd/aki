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

// inlineClockMask is the width of the idle clock an inline set rides in the
// fifteen bits above the encoding bit. A tiny set spends no header bytes on the
// per-key access clock the string cell keeps (store.lruClock, a full sixteen
// bits): the encoding bit claims bit 0, so the clock keeps the remaining
// fifteen. At one-second resolution fifteen bits wrap every 32768s (~9.1h)
// rather than the string clock's ~18.2h, the fidelity price of not spending a
// record byte the memory bar holds against rivals. A set idle longer than the
// wrap reports a smaller wrapped idle, the same seam OBJECT IDLETIME already
// documents for the string clock.
const inlineClockMask uint16 = 0x7fff

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

// inlineClock derives the fifteen-bit access clock an inline set stamps, the
// same second-resolution wall clock the string cell folds (store.LRUClock) but
// masked to the fifteen bits the encoding bit leaves free. It rides the now the
// command layer already threads (cx.NowMs), so a stamp costs no time call.
func inlineClock(nowMs int64) uint16 {
	return uint16(nowMs/1000) & inlineClockMask
}

// packBits composes the bits word an inline set commits to its record: the
// encoding in the low bit, the fifteen-bit access clock above it. The caller
// supplies the clock (inlineClock, or the fresh stamp a touch just took), so a
// write that must re-stamp the clock and a load that must preserve it share one
// packer. The caller owns inline eligibility; a non-inline encoding would not
// round-trip.
func packBits(s *set, clock uint16) uint16 {
	return inlineBits(s) | (clock&inlineClockMask)<<1
}

// withClock replaces the clock in an existing bits word without disturbing the
// encoding bit, the read-touch stamp the routing slice applies in place through
// store.SetCollBits: a read is an access, so it re-stamps the clock, but it
// reads the encoding out of the record rather than a materialized set, so it
// masks the old clock off and ORs the fresh one in.
func withClock(bits uint16, clock uint16) uint16 {
	return (bits & inlineEncMask) | (clock&inlineClockMask)<<1
}

// clockFromBits recovers the fifteen-bit access clock an inline set stamped,
// dropping the encoding bit below it. It is the stamp OBJECT IDLETIME reads back
// through inlineIdleSeconds without materializing the set.
func clockFromBits(bits uint16) uint16 {
	return bits >> 1
}

// inlineIdleSeconds is the OBJECT IDLETIME arithmetic for an inline set, read
// straight from its record bits word: seconds between now and the stamp, folded
// through the fifteen-bit wrap the clock lives in. It matches
// store.IdleSecondsFrom but over the narrower inline clock, so an inline set and
// a string answer OBJECT IDLETIME on the same second scale, only on different
// wrap horizons.
func inlineIdleSeconds(bits uint16, nowMs int64) int64 {
	return int64((inlineClock(nowMs) - clockFromBits(bits)) & inlineClockMask)
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
