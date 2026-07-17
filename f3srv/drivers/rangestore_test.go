package drivers

import (
	"strconv"
	"testing"
)

// TestZrangestore drives ZRANGESTORE on co-located keys across every selection
// mode: index, BYSCORE, BYLEX, REV, and LIMIT. The destination is a sorted set of
// the selected members with their original source scores, so ZRANGE WITHSCORES on
// the copy reads them back, and an empty selection deletes the destination.
func TestZrangestore(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "ZADD", "{s}src", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, br, ":4\r\n")

	// Index range stores the window with its original scores.
	send(t, nc, "ZRANGESTORE", "{s}i", "{s}src", "0", "1")
	expect(t, br, ":2\r\n")
	send(t, nc, "ZRANGE", "{s}i", "0", "-1", "WITHSCORES")
	expect(t, br, "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n")

	// REV index selects the two highest, still stored in score order.
	send(t, nc, "ZRANGESTORE", "{s}r", "{s}src", "0", "1", "REV")
	expect(t, br, ":2\r\n")
	send(t, nc, "ZRANGE", "{s}r", "0", "-1", "WITHSCORES")
	expect(t, br, "*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n")

	// BYSCORE with a bound and a LIMIT window.
	send(t, nc, "ZRANGESTORE", "{s}bs", "{s}src", "2", "4", "BYSCORE", "LIMIT", "1", "1")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZRANGE", "{s}bs", "0", "-1", "WITHSCORES")
	expect(t, br, "*2\r\n$1\r\nc\r\n$1\r\n3\r\n")

	// BYLEX over the equal-score band keeps the original scores in the copy.
	send(t, nc, "ZADD", "{s}lex", "0", "a", "0", "b", "0", "c")
	expect(t, br, ":3\r\n")
	send(t, nc, "ZRANGESTORE", "{s}bl", "{s}lex", "[a", "[b", "BYLEX")
	expect(t, br, ":2\r\n")
	send(t, nc, "ZRANGE", "{s}bl", "0", "-1")
	expect(t, br, "*2\r\n$1\r\na\r\n$1\r\nb\r\n")

	// An empty selection deletes a pre-existing destination and replies zero.
	send(t, nc, "ZRANGESTORE", "{s}i", "{s}src", "5", "10")
	expect(t, br, ":0\r\n")
	send(t, nc, "ZCARD", "{s}i")
	expect(t, br, ":0\r\n")

	// WITHSCORES is not part of the grammar: the token is a syntax error.
	send(t, nc, "ZRANGESTORE", "{s}x", "{s}src", "0", "-1", "WITHSCORES")
	expect(t, br, "-ERR syntax error\r\n")

	// A wrong-type source is rejected before any write.
	send(t, nc, "SET", "{s}str", "x")
	expect(t, br, "+OK\r\n")
	send(t, nc, "ZRANGESTORE", "{s}i", "{s}str", "0", "-1")
	expect(t, br, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

// TestZrangestoreCrossShard drives ZRANGESTORE with the destination and source on
// different shards, so the reply comes back through the F17 intent route.
func TestZrangestoreCrossShard(t *testing.T) {
	srv, nc, br := startServer(t)

	keyOn := func(sh int, prefix string) string {
		for i := 0; ; i++ {
			k := prefix + strconv.Itoa(i)
			if srv.rt.ShardOf([]byte(k)) == sh {
				return k
			}
		}
	}
	src := keyOn(0, "zsrc")
	dst := keyOn(1, "zdst")

	send(t, nc, "ZADD", src, "1", "a", "2", "b", "3", "c")
	expect(t, br, ":3\r\n")

	// Cross-shard store: the selection runs on the source owner, the placement on
	// the destination owner, both inside the intent that holds the two keys.
	send(t, nc, "ZRANGESTORE", dst, src, "0", "-1")
	expect(t, br, ":3\r\n")
	send(t, nc, "ZRANGE", dst, "0", "-1", "WITHSCORES")
	expect(t, br, "*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n")

	// A wrong-type source is caught on the cross path before the destination is
	// touched: the earlier destination survives.
	strKey := keyOn(0, "zstr")
	send(t, nc, "SET", strKey, "x")
	expect(t, br, "+OK\r\n")
	send(t, nc, "ZRANGESTORE", dst, strKey, "0", "-1")
	expect(t, br, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send(t, nc, "ZCARD", dst)
	expect(t, br, ":3\r\n")
}
