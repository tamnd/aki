package zset

import "math"

// The score codec (spec 2064/f3/12 section 3.1, frozen by labs/f3/m2/03): the
// native band's tree keys on an order-preserving u64 form of the IEEE-754
// double so an integer compare on tree keys equals the zset score order.
// The transform flips the sign bit of a non-negative double and inverts every
// bit of a negative one, so -inf is the smallest key, +inf the largest, and
// ordinary doubles fall in numeric order between.
//
// Signed zero is normalized to +0.0 BEFORE the transform. The transform as
// written in doc 12 maps -0.0 one key below +0.0, which would order a member
// stored at -0.0 strictly before one at +0.0; Redis treats the two zeros as
// equal, ordering such members by member bytes. The one-line normalization
// collapses both zeros onto one key so the tie falls to the member compare,
// which is the lab's pinned trap. The consequence: the tree key loses the sign
// of a zero on purpose, so ZSCORE never decodes a tree key. It formats from the
// raw double bits kept in the member hash, where a stored -0.0 still prints
// "-0".
//
// NaN never reaches the encoder: parseScore rejects a literal nan and the
// update path rejects a nan-producing increment, both before any structure is
// touched.

const keySignBit = uint64(1) << 63

// scoreKey maps a non-NaN score to its sortable u64 tree key, signed zero
// normalized to +0.0 first so -0.0 and +0.0 share one key.
func scoreKey(f float64) uint64 {
	if f == 0 { // true for both +0.0 and -0.0
		f = 0 // positive zero
	}
	b := math.Float64bits(f)
	if b&keySignBit == 0 {
		return b ^ keySignBit
	}
	return ^b
}

// scoreFromKey inverts the transform. It is exact for every key scoreKey can
// produce, except that a key made from -0.0 decodes to +0.0 because scoreKey
// normalized the sign away; the raw bits in the member hash carry the sign.
// Nothing on the ZSCORE path calls this; it exists for range-bound recovery
// and for the codec tests to prove the bijection.
func scoreFromKey(k uint64) float64 {
	if k&keySignBit != 0 { // high bit set: the score was non-negative
		return math.Float64frombits(k ^ keySignBit)
	}
	return math.Float64frombits(^k)
}
