package zset

import (
	"encoding/binary"
	"math"
)

// The inline score codec (spec 2064/f3/12 section 4): a listpack-band entry
// stores its score as a one-byte class tag followed by a width-matched payload,
// so an integer-valued score (the common rank, timestamp, and counter case)
// costs 2 to 5 bytes instead of a flat 8-byte IEEE-754 double. This is the same
// trade Redis's listpack makes, where a small integer score encodes to one or
// two bytes and only a fractional or out-of-range score pays the full float
// width. On a tiny zset of integer-scored members the entry payload nearly
// halves, closing most of the inline-band memory gap against the rival listpack.
//
// A non-integer score, an infinity, or an integer outside the int32 band falls
// back to the 8-byte float payload (class scoreF64), which is lossless: the raw
// IEEE-754 bits round-trip exactly, so ZSCORE formats the stored score with no
// decode error. Signed zero is collapsed to +0.0 by the insert callers before
// it reaches the codec, matching the listpack zero-sign quirk documented in
// zset.go, so scoreF64 never carries a -0.0.
const (
	scoreF64 byte = 0 // 8-byte big-endian float64 bits (fallback)
	scoreI8  byte = 1 // 1-byte signed int
	scoreI16 byte = 2 // 2-byte big-endian signed int
	scoreI32 byte = 4 // 4-byte big-endian signed int
)

// intScore reports the integer value of f and whether f is an integer inside the
// int32 band the compact classes cover. A fractional score, an infinity, or an
// integer past int32 returns ok=false and takes the float fallback.
func intScore(f float64) (int32, bool) {
	if f < math.MinInt32 || f > math.MaxInt32 {
		return 0, false
	}
	if f != math.Trunc(f) {
		return 0, false
	}
	return int32(f), true
}

// encScoreWidth is the total encoded width (class byte plus payload) score takes
// in the inline band. It matches what putScore writes, so an insert can size the
// entry before the memmove that opens its slot.
func encScoreWidth(score float64) int {
	iv, ok := intScore(score)
	if !ok {
		return 9
	}
	switch {
	case iv >= math.MinInt8 && iv <= math.MaxInt8:
		return 2
	case iv >= math.MinInt16 && iv <= math.MaxInt16:
		return 3
	default:
		return 5
	}
}

// putScore writes score's class-tagged encoding at the front of b and returns the
// byte count written. b must hold at least encScoreWidth(score) bytes.
func putScore(b []byte, score float64) int {
	iv, ok := intScore(score)
	if !ok {
		b[0] = scoreF64
		binary.BigEndian.PutUint64(b[1:], math.Float64bits(score))
		return 9
	}
	switch {
	case iv >= math.MinInt8 && iv <= math.MaxInt8:
		b[0] = scoreI8
		b[1] = byte(int8(iv))
		return 2
	case iv >= math.MinInt16 && iv <= math.MaxInt16:
		b[0] = scoreI16
		binary.BigEndian.PutUint16(b[1:], uint16(int16(iv)))
		return 3
	default:
		b[0] = scoreI32
		binary.BigEndian.PutUint32(b[1:], uint32(iv))
		return 5
	}
}

// readScore decodes the class-tagged score at the front of b, returning the score
// and the byte count consumed so a walk advances past it.
func readScore(b []byte) (float64, int) {
	switch b[0] {
	case scoreI8:
		return float64(int8(b[1])), 2
	case scoreI16:
		return float64(int16(binary.BigEndian.Uint16(b[1:]))), 3
	case scoreI32:
		return float64(int32(binary.BigEndian.Uint32(b[1:]))), 5
	default:
		return math.Float64frombits(binary.BigEndian.Uint64(b[1:])), 9
	}
}

// scoreWidthAt returns the encoded width of the score whose class byte sits at
// b[i], the stride an index scan adds to skip an entry's score without decoding
// it.
func scoreWidthAt(b []byte, i int) int {
	switch b[i] {
	case scoreI8:
		return 2
	case scoreI16:
		return 3
	case scoreI32:
		return 5
	default:
		return 9
	}
}

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
