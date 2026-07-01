package f1srv

import "testing"

// TestZsetPointPath walks the ZADD/ZSCORE/ZMSCORE/ZCARD/ZREM/ZINCRBY point path: an add, a
// re-add with a new score, ZSCORE readback, ZMSCORE with a hole, cardinality, and a removal
// that empties the key. Scores come back formatted the way Redis formats them (integers with no
// decimal point).
func TestZsetPointPath(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "board", "1", "alice", "2", "bob", "3", "carol")
	expect(t, rw, ":3")

	cmd(t, rw, "ZCARD", "board")
	expect(t, rw, ":3")

	cmd(t, rw, "ZSCORE", "board", "bob")
	expect(t, rw, "$2")

	cmd(t, rw, "ZSCORE", "board", "missing")
	expect(t, rw, "$-1")

	// Re-adding an existing member with a new score updates it and counts 0 new members.
	cmd(t, rw, "ZADD", "board", "20", "bob")
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "board", "bob")
	expect(t, rw, "$20")

	// ZMSCORE returns one slot per requested member, nil for absent.
	cmd(t, rw, "ZMSCORE", "board", "alice", "missing", "bob")
	expect(t, rw, "*3")
	expect(t, rw, "$1")
	expect(t, rw, "$-1")
	expect(t, rw, "$20")

	cmd(t, rw, "ZREM", "board", "bob", "missing")
	expect(t, rw, ":1")
	cmd(t, rw, "ZCARD", "board")
	expect(t, rw, ":2")

	// Removing the last members deletes the key.
	cmd(t, rw, "ZREM", "board", "alice", "carol")
	expect(t, rw, ":2")
	cmd(t, rw, "ZCARD", "board")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "board")
	expect(t, rw, ":0")
}

// TestZaddFlags exercises NX/XX/GT/LT/CH, including the mutually exclusive rejections and the
// GT/LT gating that still adds an absent member.
func TestZaddFlags(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// NX only adds new members.
	cmd(t, rw, "ZADD", "z", "NX", "5", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "ZADD", "z", "NX", "10", "a")
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$5")

	// XX only updates existing members; a new member under XX is not added.
	cmd(t, rw, "ZADD", "z", "XX", "7", "b")
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "z", "b")
	expect(t, rw, "$-1")

	// CH counts changed (added or updated), not just added.
	cmd(t, rw, "ZADD", "z", "CH", "5", "a", "9", "c")
	// a is unchanged (still 5), c is new -> 1 changed.
	expect(t, rw, ":1")

	// GT only updates when the new score is greater; it still adds an absent member.
	cmd(t, rw, "ZADD", "z", "GT", "3", "a") // 3 < 5, no update
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$5")
	cmd(t, rw, "ZADD", "z", "GT", "8", "a") // 8 > 5, update
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$8")
	cmd(t, rw, "ZADD", "z", "GT", "1", "newbie") // absent member still added
	expect(t, rw, ":1")

	// LT only updates when the new score is less.
	cmd(t, rw, "ZADD", "z", "LT", "10", "a") // 10 > 8, no update
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$8")
	cmd(t, rw, "ZADD", "z", "LT", "2", "a") // 2 < 8, update
	expect(t, rw, ":0")
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$2")

	// Incompatible flag combinations reject before any write.
	cmd(t, rw, "ZADD", "z", "NX", "XX", "1", "a")
	expect(t, rw, "-ERR XX and NX options at the same time are not compatible")
	cmd(t, rw, "ZADD", "z", "GT", "NX", "1", "a")
	expect(t, rw, "-ERR GT, LT, and/or NX options at the same time are not compatible")
	cmd(t, rw, "ZADD", "z", "GT", "LT", "1", "a")
	expect(t, rw, "-ERR GT, LT, and/or NX options at the same time are not compatible")
}

// TestZaddIncr covers ZADD INCR: it returns the new score, returns nil when a flag suppresses
// the change, and rejects the multi-pair form.
func TestZaddIncr(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "INCR", "5", "a")
	expect(t, rw, "$5")
	cmd(t, rw, "ZADD", "z", "INCR", "3", "a")
	expect(t, rw, "$8")

	// NX on an existing member suppresses the increment -> nil.
	cmd(t, rw, "ZADD", "z", "NX", "INCR", "1", "a")
	expect(t, rw, "$-1")
	// XX on an absent member suppresses the increment -> nil.
	cmd(t, rw, "ZADD", "z", "XX", "INCR", "1", "missing")
	expect(t, rw, "$-1")
	// GT suppresses a decrement -> nil.
	cmd(t, rw, "ZADD", "z", "GT", "INCR", "-1", "a")
	expect(t, rw, "$-1")
	cmd(t, rw, "ZSCORE", "z", "a")
	expect(t, rw, "$8")

	// INCR forbids more than one pair.
	cmd(t, rw, "ZADD", "z", "INCR", "1", "a", "2", "b")
	expect(t, rw, "-ERR INCR option supports a single increment-element pair")
}

// TestZincrby covers ZINCRBY creating and updating a member and the NaN rejection.
func TestZincrby(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZINCRBY", "z", "5", "a")
	expect(t, rw, "$5")
	cmd(t, rw, "ZINCRBY", "z", "2.5", "a")
	expect(t, rw, "$7.5")
	cmd(t, rw, "ZINCRBY", "z", "-7.5", "a")
	expect(t, rw, "$0")

	// +inf then -inf increment produces NaN, rejected, member left unchanged.
	cmd(t, rw, "ZADD", "z", "inf", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "ZINCRBY", "z", "-inf", "b")
	expect(t, rw, "-ERR resulting score is not a number (NaN)")
	cmd(t, rw, "ZSCORE", "z", "b")
	expect(t, rw, "$inf")
}

// TestZaddScoreFormatting pins score parsing and formatting: infinities, a NaN literal
// rejection, and the -0 normalization to "0".
func TestZaddScoreFormatting(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "inf", "pos", "-inf", "neg", "3.14", "pi")
	expect(t, rw, ":3")
	cmd(t, rw, "ZSCORE", "z", "pos")
	expect(t, rw, "$inf")
	cmd(t, rw, "ZSCORE", "z", "neg")
	expect(t, rw, "$-inf")
	cmd(t, rw, "ZSCORE", "z", "pi")
	expect(t, rw, "$3.14")

	// -0 normalizes to +0 on ingest, so ZSCORE reports "0".
	cmd(t, rw, "ZADD", "z", "-0", "zero")
	expect(t, rw, ":1")
	cmd(t, rw, "ZSCORE", "z", "zero")
	expect(t, rw, "$0")

	// A NaN literal is not a valid score.
	cmd(t, rw, "ZADD", "z", "nan", "bad")
	expect(t, rw, "-ERR value is not a valid float")
	// Garbage is not a valid score.
	cmd(t, rw, "ZADD", "z", "notafloat", "bad")
	expect(t, rw, "-ERR value is not a valid float")
}

// TestZsetEncodingAndType checks TYPE and OBJECT ENCODING fold from listpack to skiplist on the
// entry and value-length thresholds.
func TestZsetEncodingAndType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "small", "1", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "TYPE", "small")
	expect(t, rw, "+zset")
	cmd(t, rw, "OBJECT", "ENCODING", "small")
	expect(t, rw, "$listpack")

	// A member longer than 64 bytes upgrades the encoding to skiplist.
	long := make([]byte, 70)
	for i := range long {
		long[i] = 'x'
	}
	cmd(t, rw, "ZADD", "big", "1", string(long))
	expect(t, rw, ":1")
	cmd(t, rw, "OBJECT", "ENCODING", "big")
	expect(t, rw, "$skiplist")

	// More than 128 entries upgrades to skiplist too.
	for i := 0; i < 130; i++ {
		cmd(t, rw, "ZADD", "many", itoa(i), "m"+itoa(i))
		expect(t, rw, ":1")
	}
	cmd(t, rw, "OBJECT", "ENCODING", "many")
	expect(t, rw, "$skiplist")
}

// TestZrank walks ZRANK and ZREVRANK: forward and reverse positions, the WITHSCORE form, and
// the nil replies for an absent member (a null bulk without WITHSCORE, a null array with it).
func TestZrank(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "board", "1", "alice", "2", "bob", "3", "carol")
	expect(t, rw, ":3")

	// Forward rank is the 0-based position in ascending score order.
	cmd(t, rw, "ZRANK", "board", "alice")
	expect(t, rw, ":0")
	cmd(t, rw, "ZRANK", "board", "bob")
	expect(t, rw, ":1")
	cmd(t, rw, "ZRANK", "board", "carol")
	expect(t, rw, ":2")

	// Reverse rank counts from the high end.
	cmd(t, rw, "ZREVRANK", "board", "alice")
	expect(t, rw, ":2")
	cmd(t, rw, "ZREVRANK", "board", "carol")
	expect(t, rw, ":0")

	// WITHSCORE returns a two-element array of rank then score.
	cmd(t, rw, "ZRANK", "board", "bob", "WITHSCORE")
	expect(t, rw, "*2")
	expect(t, rw, ":1")
	expect(t, rw, "$2")
	cmd(t, rw, "ZREVRANK", "board", "bob", "WITHSCORE")
	expect(t, rw, "*2")
	expect(t, rw, ":1")
	expect(t, rw, "$2")

	// An absent member is a null bulk, or a null array under WITHSCORE.
	cmd(t, rw, "ZRANK", "board", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "ZRANK", "board", "missing", "WITHSCORE")
	expect(t, rw, "*-1")
	cmd(t, rw, "ZREVRANK", "board", "missing")
	expect(t, rw, "$-1")

	// Rank on a missing key is a null bulk, not an error.
	cmd(t, rw, "ZRANK", "nokey", "a")
	expect(t, rw, "$-1")
}

// TestZrange walks the rank-indexed ZRANGE/ZREVRANGE: a forward window, negative indices, an
// out-of-range clamp, an empty window, the REV modifier, and WITHSCORES.
func TestZrange(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, rw, ":4")

	// Forward window by rank. readReply renders a bulk as "$"+value, so a member "a"
	// comes back as "$a" and a score "1" as "$1".
	cmd(t, rw, "ZRANGE", "z", "0", "1")
	expect(t, rw, "*2")
	expect(t, rw, "$a")
	expect(t, rw, "$b")

	// Negative indices count from the end; -1 is the last element.
	cmd(t, rw, "ZRANGE", "z", "-2", "-1")
	expect(t, rw, "*2")
	expect(t, rw, "$c")
	expect(t, rw, "$d")

	// A stop past the end clamps to the last element.
	cmd(t, rw, "ZRANGE", "z", "2", "100")
	expect(t, rw, "*2")
	expect(t, rw, "$c")
	expect(t, rw, "$d")

	// start past stop is an empty array.
	cmd(t, rw, "ZRANGE", "z", "3", "1")
	expect(t, rw, "*0")

	// WITHSCORES interleaves each member with its score.
	cmd(t, rw, "ZRANGE", "z", "0", "1", "WITHSCORES")
	expect(t, rw, "*4")
	expect(t, rw, "$a")
	expect(t, rw, "$1")
	expect(t, rw, "$b")
	expect(t, rw, "$2")

	// REV walks high to low; ZREVRANGE is the same window.
	cmd(t, rw, "ZRANGE", "z", "0", "1", "REV")
	expect(t, rw, "*2")
	expect(t, rw, "$d")
	expect(t, rw, "$c")
	cmd(t, rw, "ZREVRANGE", "z", "0", "1")
	expect(t, rw, "*2")
	expect(t, rw, "$d")
	expect(t, rw, "$c")

	// ZREVRANGE WITHSCORES over the whole set, highest first.
	cmd(t, rw, "ZREVRANGE", "z", "0", "-1", "WITHSCORES")
	expect(t, rw, "*8")
	expect(t, rw, "$d")
	expect(t, rw, "$4")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
	expect(t, rw, "$b")
	expect(t, rw, "$2")
	expect(t, rw, "$a")
	expect(t, rw, "$1")

	// A missing key is an empty array.
	cmd(t, rw, "ZRANGE", "nokey", "0", "-1")
	expect(t, rw, "*0")
}

// TestZrangeTiesOrderByMember confirms members sharing a score order by member bytes, so rank
// and range are stable and match Redis's score-then-member ordering.
func TestZrangeTiesOrderByMember(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "5", "c", "5", "a", "5", "b")
	expect(t, rw, ":3")
	cmd(t, rw, "ZRANGE", "z", "0", "-1")
	expect(t, rw, "*3")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
	expect(t, rw, "$c")
	// Rank reflects the same score-then-member order.
	cmd(t, rw, "ZRANK", "z", "a")
	expect(t, rw, ":0")
	cmd(t, rw, "ZRANK", "z", "c")
	expect(t, rw, ":2")
}

// TestZsetWrongType confirms a zset command against a string key is WRONGTYPE.
func TestZsetWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "s", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "ZADD", "s", "1", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZSCORE", "s", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZINCRBY", "s", "1", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZREM", "s", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZCARD", "s")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZMSCORE", "s", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZRANK", "s", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZREVRANK", "s", "a")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZRANGE", "s", "0", "-1")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "ZREVRANGE", "s", "0", "-1")
	expect(t, rw, "-"+wrongType)
}
