package sqlo1

import "math"

// Sortable score codec, doc 09 section 2.3. Zset scores are IEEE 754
// doubles made memcmp-ordered by the standard transform: flip all
// bits if the sign bit is set, else flip only the sign bit. Under it,
// (score, member) ordering is a pure byte comparison, score ranges
// are flat u64 ranges for the score fence, and -inf, +inf, and the
// full finite line order exactly as Redis's double comparison does.
//
// The one point where the raw transform and Redis disagree is the
// zero pair: -0 and +0 are distinct bit patterns (and would encode
// 2^63-1 apart) but Redis compares scores with the C < operator,
// under which -0 == +0, and even prints -0 as "0" (d2string
// special-cases value == 0). The encoder therefore canonicalizes -0
// to +0, which is invisible at every surface and keeps "equal scores
// break ties by member" true as a byte rule. NaN has no place in the
// order and is rejected at the command layer, as in Redis; the codec
// itself is total and just maps it wherever its bits land.

// zScoreSortable encodes score into the memcmp-ordered u64.
func zScoreSortable(score float64) uint64 {
	if score == 0 {
		score = 0 // -0 folds to +0, Redis's comparison semantics
	}
	b := math.Float64bits(score)
	if b&(1<<63) != 0 {
		return ^b
	}
	return b | 1<<63
}

// zScoreFromSortable inverts zScoreSortable.
func zScoreFromSortable(u uint64) float64 {
	if u&(1<<63) != 0 {
		return math.Float64frombits(u &^ (1 << 63))
	}
	return math.Float64frombits(^u)
}
