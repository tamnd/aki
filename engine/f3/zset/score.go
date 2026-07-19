package zset

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/f3srv/resp"
)

// parseScore reads a zset score the way Redis's getDoubleFromObject does: the
// infinities (inf, +inf, -inf, infinity) and ordinary decimals parse, NaN and
// trailing garbage reject. strconv.ParseFloat matches strtod here on the shapes
// clients send, including the inf spellings and hex floats, and rejects leading
// and trailing whitespace the same way Redis does.
//
// Negative zero passes through with its sign, like strtod: what happens to it
// then depends on the band, exactly as in Redis, where the skiplist keeps the
// double (ZSCORE answers "-0") and the listpack collapses it to an integer
// zero (ZSCORE answers "0"). The live formatting test pins both behaviors
// against a real server.
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

// appendScore appends a member's score in the connection's negotiated protocol:
// a RESP3 double (,digits) when resp3 is set, or a RESP2 bulk string of the same
// FormatScore digits otherwise. It is the buffer-building twin of Reply.Double,
// for the multi-element replies (ZPOPMIN, ZMPOP, ZMSCORE, ZRANDMEMBER WITHSCORES,
// BZPOPMIN) that stream members and scores into one hand-built array rather than
// calling Reply.Double once. sc is FormatScore scratch, used only on the RESP2
// branch. Under both protocols the digits are identical, only the framing differs.
func appendScore(out []byte, s float64, resp3 bool, sc []byte) []byte {
	if resp3 {
		return resp.AppendDouble(out, s)
	}
	return resp.AppendBulk(out, resp.FormatScore(sc[:0], s))
}
