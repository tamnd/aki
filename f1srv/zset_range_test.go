package f1srv

import "testing"

// TestZrangeByScore covers the score-cursor forms: ZRANGEBYSCORE, ZREVRANGEBYSCORE, the ZRANGE
// BYSCORE routing, inclusive and exclusive bounds, the infinities, LIMIT, and WITHSCORES. Each
// member "a".."e" carries score 1..5 so the score order and member order coincide, which keeps the
// expected replies easy to read.
func TestZrangeByScore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, rw, ":5")

	// Inclusive numeric window.
	cmd(t, rw, "ZRANGEBYSCORE", "z", "2", "4")
	expect(t, rw, "*3")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	expect(t, rw, "$d")

	// Exclusive min drops score 2, exclusive max drops score 4.
	cmd(t, rw, "ZRANGEBYSCORE", "z", "(2", "(4")
	expect(t, rw, "*1")
	expect(t, rw, "$c")

	// The infinities bound the whole set.
	cmd(t, rw, "ZRANGEBYSCORE", "z", "-inf", "+inf")
	expect(t, rw, "*5")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	expect(t, rw, "$d")
	expect(t, rw, "$e")

	// WITHSCORES interleaves each member with its score.
	cmd(t, rw, "ZRANGEBYSCORE", "z", "1", "2", "WITHSCORES")
	expect(t, rw, "*4")
	expect(t, rw, "$a")
	expect(t, rw, "$1")
	expect(t, rw, "$b")
	expect(t, rw, "$2")

	// LIMIT offset count over the matched window, forward.
	cmd(t, rw, "ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2")
	expect(t, rw, "*2")
	expect(t, rw, "$b")
	expect(t, rw, "$c")

	// ZREVRANGEBYSCORE takes max then min and walks high to low.
	cmd(t, rw, "ZREVRANGEBYSCORE", "z", "4", "2")
	expect(t, rw, "*3")
	expect(t, rw, "$d")
	expect(t, rw, "$c")
	expect(t, rw, "$b")

	// Reverse LIMIT offsets from the high end.
	cmd(t, rw, "ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "1", "2")
	expect(t, rw, "*2")
	expect(t, rw, "$d")
	expect(t, rw, "$c")

	// ZRANGE BYSCORE routes to the same path; REV means max then min.
	cmd(t, rw, "ZRANGE", "z", "2", "4", "BYSCORE")
	expect(t, rw, "*3")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	expect(t, rw, "$d")
	cmd(t, rw, "ZRANGE", "z", "4", "2", "BYSCORE", "REV")
	expect(t, rw, "*3")
	expect(t, rw, "$d")
	expect(t, rw, "$c")
	expect(t, rw, "$b")

	// An empty window is an empty array.
	cmd(t, rw, "ZRANGEBYSCORE", "z", "6", "9")
	expect(t, rw, "*0")

	// A missing key is an empty array.
	cmd(t, rw, "ZRANGEBYSCORE", "nokey", "-inf", "+inf")
	expect(t, rw, "*0")
}

// TestZcount checks the score-window size is two rank lookups: inclusive, exclusive, and the
// infinities, plus the empty and missing cases.
func TestZcount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, rw, ":5")

	cmd(t, rw, "ZCOUNT", "z", "-inf", "+inf")
	expect(t, rw, ":5")
	cmd(t, rw, "ZCOUNT", "z", "2", "4")
	expect(t, rw, ":3")
	cmd(t, rw, "ZCOUNT", "z", "(2", "(4")
	expect(t, rw, ":1")
	cmd(t, rw, "ZCOUNT", "z", "6", "9")
	expect(t, rw, ":0")
	cmd(t, rw, "ZCOUNT", "nokey", "-inf", "+inf")
	expect(t, rw, ":0")
}

// TestZrangeByLex covers the lex-cursor forms over members sharing one score, where the order is
// pure member bytes: ZRANGEBYLEX, ZREVRANGEBYLEX, ZRANGE BYLEX, inclusive and exclusive bounds, the
// "-"/"+" infinities, and LIMIT.
func TestZrangeByLex(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")
	expect(t, rw, ":5")

	// Full range with the member-space infinities.
	cmd(t, rw, "ZRANGEBYLEX", "z", "-", "+")
	expect(t, rw, "*5")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	expect(t, rw, "$d")
	expect(t, rw, "$e")

	// Inclusive [b to inclusive [d.
	cmd(t, rw, "ZRANGEBYLEX", "z", "[b", "[d")
	expect(t, rw, "*3")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	expect(t, rw, "$d")

	// Exclusive (b to exclusive (d.
	cmd(t, rw, "ZRANGEBYLEX", "z", "(b", "(d")
	expect(t, rw, "*1")
	expect(t, rw, "$c")

	// Bounded above only.
	cmd(t, rw, "ZRANGEBYLEX", "z", "-", "[c")
	expect(t, rw, "*3")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
	expect(t, rw, "$c")

	// LIMIT over the matched window.
	cmd(t, rw, "ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "2")
	expect(t, rw, "*2")
	expect(t, rw, "$b")
	expect(t, rw, "$c")

	// ZREVRANGEBYLEX takes max then min and walks back to front.
	cmd(t, rw, "ZREVRANGEBYLEX", "z", "[d", "[b")
	expect(t, rw, "*3")
	expect(t, rw, "$d")
	expect(t, rw, "$c")
	expect(t, rw, "$b")
	cmd(t, rw, "ZREVRANGEBYLEX", "z", "+", "-")
	expect(t, rw, "*5")
	expect(t, rw, "$e")
	expect(t, rw, "$d")
	expect(t, rw, "$c")
	expect(t, rw, "$b")
	expect(t, rw, "$a")

	// ZRANGE BYLEX routes to the same path.
	cmd(t, rw, "ZRANGE", "z", "[b", "[d", "BYLEX")
	expect(t, rw, "*3")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	expect(t, rw, "$d")
	cmd(t, rw, "ZRANGE", "z", "[d", "[b", "BYLEX", "REV")
	expect(t, rw, "*3")
	expect(t, rw, "$d")
	expect(t, rw, "$c")
	expect(t, rw, "$b")

	// A missing key is an empty array.
	cmd(t, rw, "ZRANGEBYLEX", "nokey", "-", "+")
	expect(t, rw, "*0")
}

// TestZlexcount checks the member-window size, including the infinities and exclusive bounds.
func TestZlexcount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")
	expect(t, rw, ":5")

	cmd(t, rw, "ZLEXCOUNT", "z", "-", "+")
	expect(t, rw, ":5")
	cmd(t, rw, "ZLEXCOUNT", "z", "[b", "[d")
	expect(t, rw, ":3")
	cmd(t, rw, "ZLEXCOUNT", "z", "(b", "(d")
	expect(t, rw, ":1")
	cmd(t, rw, "ZLEXCOUNT", "nokey", "-", "+")
	expect(t, rw, ":0")
}

// TestZpop covers ZPOPMIN and ZPOPMAX: the no-count form, the count form, count past the
// cardinality, the emptied-key case, and a missing key. Members "a".."e" carry scores 1..5.
func TestZpop(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, rw, ":5")

	// No count pops the single lowest, flat member+score.
	cmd(t, rw, "ZPOPMIN", "z")
	expect(t, rw, "*2")
	expect(t, rw, "$a")
	expect(t, rw, "$1")

	// No count pops the single highest.
	cmd(t, rw, "ZPOPMAX", "z")
	expect(t, rw, "*2")
	expect(t, rw, "$e")
	expect(t, rw, "$5")

	// The set is now b,c,d. Count pops the two lowest, ascending.
	cmd(t, rw, "ZPOPMIN", "z", "2")
	expect(t, rw, "*4")
	expect(t, rw, "$b")
	expect(t, rw, "$2")
	expect(t, rw, "$c")
	expect(t, rw, "$3")

	// Only d is left; a count past the cardinality pops what remains.
	cmd(t, rw, "ZPOPMAX", "z", "9")
	expect(t, rw, "*2")
	expect(t, rw, "$d")
	expect(t, rw, "$4")

	// The zset is now empty, so it no longer exists.
	cmd(t, rw, "ZCARD", "z")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "z")
	expect(t, rw, ":0")

	// A missing key is an empty array.
	cmd(t, rw, "ZPOPMIN", "nokey")
	expect(t, rw, "*0")

	// A zero count is an empty array.
	cmd(t, rw, "ZADD", "z2", "1", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "ZPOPMIN", "z2", "0")
	expect(t, rw, "*0")
}

// TestZpopMaxOrder confirms ZPOPMAX with a count returns members highest score first.
func TestZpopMaxOrder(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZPOPMAX", "z", "3")
	expect(t, rw, "*6")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
	expect(t, rw, "$b")
	expect(t, rw, "$2")
	expect(t, rw, "$a")
	expect(t, rw, "$1")
}

// TestZrangeBySyntaxErrors confirms the option-combination guards match Redis: LIMIT needs BYSCORE
// or BYLEX, WITHSCORES is incompatible with BYLEX, and a bad float or lex item is rejected.
func TestZrangeBySyntaxErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a")
	expect(t, rw, ":1")

	cmd(t, rw, "ZRANGE", "z", "0", "1", "LIMIT", "0", "1")
	expect(t, rw, "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")

	cmd(t, rw, "ZRANGE", "z", "-", "+", "BYLEX", "WITHSCORES")
	expect(t, rw, "-ERR syntax error, WITHSCORES not supported in combination with BYLEX")

	cmd(t, rw, "ZRANGEBYSCORE", "z", "notafloat", "1")
	expect(t, rw, "-ERR min or max is not a float")

	cmd(t, rw, "ZRANGEBYLEX", "z", "b", "d")
	expect(t, rw, "-ERR min or max not valid string range item")
}
