package zset

import (
	"math"
	"strconv"
)

// parseScore reads a zset score the way Redis's getDoubleFromObject does: the
// infinities (inf, +inf, -inf, infinity) and ordinary decimals parse, NaN and
// trailing garbage reject. strconv.ParseFloat matches strtod here on the shapes
// clients send, including the inf spellings and hex floats, and rejects leading
// and trailing whitespace the same way Redis does.
//
// The score format side (ZSCORE, ZADD INCR, ZINCRBY, WITHSCORES replies) is
// resp.FormatScore, a byte-for-byte port of Redis's d2string, so this package
// does not reimplement it.
func parseScore(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}
