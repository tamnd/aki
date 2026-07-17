package drivers

import (
	"strconv"
	"testing"
)

// The zset rank-and-range wire surface end to end (spec 2064/f3/12 M2 slice 4):
// ZRANK, ZREVRANK, ZRANGE by index, and the ZREVRANGE alias, driven through the
// real dispatch and shard so the exact RESP bytes are checked, including the
// null forms and the WITHSCORES flat array. The commands run over both bands: a
// small key stays inline, a large key crosses into the native counted tree.

// TestZsetRankRangeInline drives the whole rank-and-range surface over an inline
// zset (well under the 128-entry cap), asserting exact RESP.
func TestZsetRankRangeInline(t *testing.T) {
	_, nc, br := startServer(t)

	// a=1 b=2 c=2 d=3: b and c tie on score and order by member bytes.
	send(t, nc, "ZADD", "z", "1", "a", "2", "b", "2", "c", "3", "d")
	expect(t, br, ":4\r\n")

	// ZRANK counts members sorting before: a<b<c<d.
	send(t, nc, "ZRANK", "z", "a")
	expect(t, br, ":0\r\n")
	send(t, nc, "ZRANK", "z", "c")
	expect(t, br, ":2\r\n")
	// ZREVRANK is card-1-rank.
	send(t, nc, "ZREVRANK", "z", "a")
	expect(t, br, ":3\r\n")
	send(t, nc, "ZREVRANK", "z", "d")
	expect(t, br, ":0\r\n")

	// Absent member: nil for ZRANK, null array for ZRANK WITHSCORE.
	send(t, nc, "ZRANK", "z", "zzz")
	expect(t, br, "$-1\r\n")
	send(t, nc, "ZRANK", "z", "zzz", "WITHSCORE")
	expect(t, br, "*-1\r\n")
	send(t, nc, "ZREVRANK", "z", "zzz", "WITHSCORE")
	expect(t, br, "*-1\r\n")

	// WITHSCORE present: [rank, score].
	send(t, nc, "ZRANK", "z", "d", "WITHSCORE")
	expect(t, br, "*2\r\n:3\r\n$1\r\n3\r\n")
	send(t, nc, "ZREVRANK", "z", "a", "WITHSCORE")
	expect(t, br, "*2\r\n:3\r\n$1\r\n1\r\n")

	// ZRANGE whole set, ascending.
	send(t, nc, "ZRANGE", "z", "0", "-1")
	expect(t, br, "*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")
	// WITHSCORES flat: member, score, member, score.
	send(t, nc, "ZRANGE", "z", "0", "1", "WITHSCORES")
	expect(t, br, "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n")
	// Negative bounds: last two.
	send(t, nc, "ZRANGE", "z", "-2", "-1")
	expect(t, br, "*2\r\n$1\r\nc\r\n$1\r\nd\r\n")
	// Overflow stop clamps, start under zero clamps.
	send(t, nc, "ZRANGE", "z", "-100", "100")
	expect(t, br, "*4\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")
	// Inverted window is empty.
	send(t, nc, "ZRANGE", "z", "3", "1")
	expect(t, br, "*0\r\n")
	// Start past the end is empty.
	send(t, nc, "ZRANGE", "z", "9", "10")
	expect(t, br, "*0\r\n")

	// REV order and the ZREVRANGE alias agree.
	send(t, nc, "ZRANGE", "z", "0", "-1", "REV")
	expect(t, br, "*4\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n")
	send(t, nc, "ZREVRANGE", "z", "0", "-1")
	expect(t, br, "*4\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n")
	send(t, nc, "ZREVRANGE", "z", "0", "1", "WITHSCORES")
	expect(t, br, "*4\r\n$1\r\nd\r\n$1\r\n3\r\n$1\r\nc\r\n$1\r\n2\r\n")

	// A missing key is an empty array, and its ZRANK is nil.
	send(t, nc, "ZRANGE", "nope", "0", "-1")
	expect(t, br, "*0\r\n")
	send(t, nc, "ZRANK", "nope", "a")
	expect(t, br, "$-1\r\n")

	// Syntax errors.
	send(t, nc, "ZRANGE", "z", "x", "1")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")
	send(t, nc, "ZRANK", "z", "a", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
	send(t, nc, "ZRANGE", "z", "0", "1", "NOPE")
	expect(t, br, "-ERR syntax error\r\n")
}

// TestZsetRankRangeNative pushes past the inline cap so the same commands run on
// the native counted tree, and checks the seek-and-walk answers match a model
// computed here. Scores are i so the order is member index; the far window
// exercises the counted select, not a walk from the front.
func TestZsetRankRangeNative(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 500 // > 128, forces the skiplist band
	// Insert in a shuffled-ish order so the tree does real ordering work.
	for i := 0; i < n; i++ {
		v := (i * 137) % n // a permutation of 0..n-1
		send(t, nc, "ZADD", "big", strconv.Itoa(v), member(v))
		expect(t, br, ":1\r\n")
	}

	// The key is well past the 128-entry cap, so it runs on the native counted
	// tree, which OBJECT ENCODING now reports directly: the zset band is threaded
	// into the single OBJECT chain, so the encoding no longer routes to the string
	// store.
	send(t, nc, "OBJECT", "ENCODING", "big")
	expect(t, br, bulk("skiplist"))

	// ZRANK equals the member's own index (score == index, member sorts with it).
	send(t, nc, "ZRANK", "big", member(0))
	expect(t, br, ":0\r\n")
	send(t, nc, "ZRANK", "big", member(250))
	expect(t, br, ":250\r\n")
	send(t, nc, "ZREVRANK", "big", member(0))
	expect(t, br, ":"+strconv.Itoa(n-1)+"\r\n")
	send(t, nc, "ZRANK", "big", member(499), "WITHSCORE")
	expect(t, br, "*2\r\n:499\r\n$3\r\n499\r\n")

	// A far forward window: ranks 300..302, the counted-select seek.
	send(t, nc, "ZRANGE", "big", "300", "302")
	expect(t, br, arr(member(300), member(301), member(302)))
	// The reverse of a far window: ZREVRANGE ranks 0..2 are the top three.
	send(t, nc, "ZREVRANGE", "big", "0", "2")
	expect(t, br, arr(member(499), member(498), member(497)))
	// ZRANGE REV with WITHSCORES over a window crossing leaf boundaries.
	send(t, nc, "ZRANGE", "big", "0", "2", "REV", "WITHSCORES")
	expect(t, br, "*6\r\n"+bulk(member(499))+bulk("499")+bulk(member(498))+bulk("498")+bulk(member(497))+bulk("497"))
}

// member is the fixed-width member name the native test uses so RESP lengths are
// predictable: three digits, zero padded.
func member(i int) string {
	s := strconv.Itoa(i)
	for len(s) < 3 {
		s = "0" + s
	}
	return "m" + s
}

// arr renders a RESP array of bulk strings.
func arr(ss ...string) string {
	out := "*" + strconv.Itoa(len(ss)) + "\r\n"
	for _, s := range ss {
		out += bulk(s)
	}
	return out
}
