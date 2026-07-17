package sqlo1

import (
	"fmt"
	"testing"
)

// respArr builds the RESP array reply the range tests expect: one
// bulk string per item.
func respArr(items ...string) string {
	out := fmt.Sprintf("*%d\r\n", len(items))
	for _, it := range items {
		out += fmt.Sprintf("$%d\r\n%s\r\n", len(it), it)
	}
	return out
}

func TestServerZRangeSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, r, ":5\r\n")

	// The index form: negatives from the end, out-of-range clamps,
	// crossed bounds empty, an absent key empties.
	send("ZRANGE", "z", "0", "-1")
	expect(t, r, respArr("a", "b", "c", "d", "e"))
	send("ZRANGE", "z", "1", "3")
	expect(t, r, respArr("b", "c", "d"))
	send("ZRANGE", "z", "-2", "-1")
	expect(t, r, respArr("d", "e"))
	send("ZRANGE", "z", "5", "10")
	expect(t, r, "*0\r\n")
	send("ZRANGE", "z", "3", "1")
	expect(t, r, "*0\r\n")
	send("ZRANGE", "nokey", "0", "-1")
	expect(t, r, "*0\r\n")
	send("ZRANGE", "z", "0", "2", "WITHSCORES")
	expect(t, r, respArr("a", "1", "b", "2", "c", "3"))

	// REV counts its indexes from the top.
	send("ZRANGE", "z", "0", "-1", "REV")
	expect(t, r, respArr("e", "d", "c", "b", "a"))
	send("ZRANGE", "z", "1", "2", "REV")
	expect(t, r, respArr("d", "c"))

	// BYSCORE: inclusive by default, ( for exclusive, the infinities,
	// and REV taking the bounds max-first.
	send("ZRANGE", "z", "2", "4", "BYSCORE")
	expect(t, r, respArr("b", "c", "d"))
	send("ZRANGE", "z", "(2", "4", "BYSCORE")
	expect(t, r, respArr("c", "d"))
	send("ZRANGE", "z", "(1", "(3", "BYSCORE")
	expect(t, r, respArr("b"))
	send("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "WITHSCORES")
	expect(t, r, respArr("a", "1", "b", "2", "c", "3", "d", "4", "e", "5"))
	send("ZRANGE", "z", "4", "2", "BYSCORE", "REV")
	expect(t, r, respArr("d", "c", "b"))
	send("ZRANGE", "z", "5", "2", "BYSCORE")
	expect(t, r, "*0\r\n")
	send("ZRANGE", "z", "(3", "(3", "BYSCORE")
	expect(t, r, "*0\r\n")

	// LIMIT folds into the rank window: offset from the direction's
	// near end, -1 meaning the rest, 0 nothing, a negative offset
	// nothing.
	send("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2")
	expect(t, r, respArr("b", "c"))
	send("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "2", "-1")
	expect(t, r, respArr("c", "d", "e"))
	send("ZRANGE", "z", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "1", "2")
	expect(t, r, respArr("d", "c"))
	send("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "0", "0")
	expect(t, r, "*0\r\n")
	send("ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "-1", "3")
	expect(t, r, "*0\r\n")

	// BYLEX over an equal-score key: -/+, [ and ( bounds, REV
	// max-first, LIMIT.
	send("ZADD", "zl", "0", "a", "0", "b", "0", "c", "0", "d")
	expect(t, r, ":4\r\n")
	send("ZRANGE", "zl", "-", "+", "BYLEX")
	expect(t, r, respArr("a", "b", "c", "d"))
	send("ZRANGE", "zl", "[b", "[c", "BYLEX")
	expect(t, r, respArr("b", "c"))
	send("ZRANGE", "zl", "(a", "(d", "BYLEX")
	expect(t, r, respArr("b", "c"))
	send("ZRANGE", "zl", "-", "(c", "BYLEX")
	expect(t, r, respArr("a", "b"))
	send("ZRANGE", "zl", "+", "-", "BYLEX", "REV")
	expect(t, r, respArr("d", "c", "b", "a"))
	send("ZRANGE", "zl", "[c", "-", "BYLEX", "REV")
	expect(t, r, respArr("c", "b", "a"))
	send("ZRANGE", "zl", "-", "+", "BYLEX", "LIMIT", "1", "2")
	expect(t, r, respArr("b", "c"))
	send("ZRANGE", "zl", "[b", "[a", "BYLEX")
	expect(t, r, "*0\r\n")

	// The option doors.
	send("ZRANGE", "z", "0")
	expect(t, r, "-ERR wrong number of arguments for 'zrange' command\r\n")
	send("ZRANGE", "z", "0", "-1", "LIMIT", "0", "1")
	expect(t, r, "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX\r\n")
	send("ZRANGE", "zl", "-", "+", "BYLEX", "WITHSCORES")
	expect(t, r, "-ERR syntax error, WITHSCORES not supported in combination with BYLEX\r\n")
	send("ZRANGE", "z", "notanint", "1")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("ZRANGE", "z", "bad", "1", "BYSCORE")
	expect(t, r, "-ERR min or max is not a float\r\n")
	send("ZRANGE", "zl", "b", "c", "BYLEX")
	expect(t, r, "-ERR min or max not valid string range item\r\n")
	send("ZRANGE", "z", "0", "-1", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZRANGE", "z", "0", "-1", "LIMIT", "1")
	expect(t, r, "-ERR syntax error\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("ZRANGE", "str", "0", "-1")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZRANGE", "str", "1", "2", "BYSCORE")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZRANGE", "str", "-", "+", "BYLEX")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func TestServerZRangeLegacySurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, r, ":5\r\n")
	send("ZADD", "zl", "0", "a", "0", "b", "0", "c", "0", "d")
	expect(t, r, ":4\r\n")

	// ZREVRANGE: the index form reversed.
	send("ZREVRANGE", "z", "0", "-1")
	expect(t, r, respArr("e", "d", "c", "b", "a"))
	send("ZREVRANGE", "z", "0", "1", "WITHSCORES")
	expect(t, r, respArr("e", "5", "d", "4"))
	send("ZREVRANGE", "z", "0", "1", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZREVRANGE", "z", "0", "1", "WITHSCORES", "extra")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZREVRANGE", "z", "0")
	expect(t, r, "-ERR wrong number of arguments for 'zrevrange' command\r\n")

	// The legacy BYSCORE pair: bounds arrive in the direction's order.
	send("ZRANGEBYSCORE", "z", "-inf", "+inf")
	expect(t, r, respArr("a", "b", "c", "d", "e"))
	send("ZRANGEBYSCORE", "z", "(1", "3", "WITHSCORES")
	expect(t, r, respArr("b", "2", "c", "3"))
	send("ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2")
	expect(t, r, respArr("b", "c"))
	send("ZREVRANGEBYSCORE", "z", "+inf", "-inf")
	expect(t, r, respArr("e", "d", "c", "b", "a"))
	send("ZREVRANGEBYSCORE", "z", "3", "(1")
	expect(t, r, respArr("c", "b"))
	send("ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "0", "2")
	expect(t, r, respArr("e", "d"))
	send("ZRANGEBYSCORE", "z", "bad", "1")
	expect(t, r, "-ERR min or max is not a float\r\n")
	send("ZRANGEBYSCORE", "z", "1", "2", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZRANGEBYSCORE", "z", "1")
	expect(t, r, "-ERR wrong number of arguments for 'zrangebyscore' command\r\n")

	// The legacy BYLEX pair.
	send("ZRANGEBYLEX", "zl", "-", "+")
	expect(t, r, respArr("a", "b", "c", "d"))
	send("ZRANGEBYLEX", "zl", "[b", "(d")
	expect(t, r, respArr("b", "c"))
	send("ZRANGEBYLEX", "zl", "-", "+", "LIMIT", "1", "2")
	expect(t, r, respArr("b", "c"))
	send("ZREVRANGEBYLEX", "zl", "+", "-")
	expect(t, r, respArr("d", "c", "b", "a"))
	send("ZREVRANGEBYLEX", "zl", "[c", "-")
	expect(t, r, respArr("c", "b", "a"))
	send("ZRANGEBYLEX", "zl", "bad", "+")
	expect(t, r, "-ERR min or max not valid string range item\r\n")

	// The counters: two seeks, no streaming.
	send("ZCOUNT", "z", "2", "3")
	expect(t, r, ":2\r\n")
	send("ZCOUNT", "z", "(2", "+inf")
	expect(t, r, ":3\r\n")
	send("ZCOUNT", "z", "5", "1")
	expect(t, r, ":0\r\n")
	send("ZCOUNT", "nokey", "1", "2")
	expect(t, r, ":0\r\n")
	send("ZCOUNT", "z", "bad", "1")
	expect(t, r, "-ERR min or max is not a float\r\n")
	send("ZCOUNT", "z", "1")
	expect(t, r, "-ERR wrong number of arguments for 'zcount' command\r\n")
	send("ZLEXCOUNT", "zl", "-", "+")
	expect(t, r, ":4\r\n")
	send("ZLEXCOUNT", "zl", "[b", "(d")
	expect(t, r, ":2\r\n")
	send("ZLEXCOUNT", "zl", "bad", "+")
	expect(t, r, "-ERR min or max not valid string range item\r\n")

	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	for _, cmd := range [][]string{
		{"ZREVRANGE", "str", "0", "-1"},
		{"ZRANGEBYSCORE", "str", "1", "2"},
		{"ZREVRANGEBYSCORE", "str", "2", "1"},
		{"ZRANGEBYLEX", "str", "-", "+"},
		{"ZREVRANGEBYLEX", "str", "+", "-"},
		{"ZCOUNT", "str", "1", "2"},
		{"ZLEXCOUNT", "str", "-", "+"},
	} {
		send(cmd...)
		expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	}
}

func TestServerZRangeStoreSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, r, ":5\r\n")

	// Every BY form lands the same forward window; REV only steers
	// LIMIT, the stored order is always ascending.
	send("ZRANGESTORE", "dst", "src", "0", "-1")
	expect(t, r, ":5\r\n")
	send("ZRANGE", "dst", "0", "-1", "WITHSCORES")
	expect(t, r, respArr("a", "1", "b", "2", "c", "3", "d", "4", "e", "5"))
	send("ZRANGESTORE", "dst", "src", "1", "2")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "dst", "0", "-1")
	expect(t, r, respArr("b", "c"))
	send("ZRANGESTORE", "dst", "src", "2", "4", "BYSCORE", "LIMIT", "1", "2")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "dst", "0", "-1")
	expect(t, r, respArr("c", "d"))
	send("ZRANGESTORE", "dst", "src", "0", "1", "REV")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "dst", "0", "-1")
	expect(t, r, respArr("d", "e"))
	send("ZADD", "zl", "0", "a", "0", "b", "0", "c", "0", "d")
	expect(t, r, ":4\r\n")
	send("ZRANGESTORE", "dl", "zl", "[b", "(d", "BYLEX")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "dl", "0", "-1")
	expect(t, r, respArr("b", "c"))

	// The destination doors: an old TTL drops, a wrong-type value is
	// overwritten, an empty window deletes, an absent source deletes.
	send("ZRANGESTORE", "tdst", "src", "0", "0")
	expect(t, r, ":1\r\n")
	send("EXPIRE", "tdst", "100")
	expect(t, r, ":1\r\n")
	send("TTL", "tdst")
	expect(t, r, ":100\r\n")
	send("ZRANGESTORE", "tdst", "src", "0", "-1")
	expect(t, r, ":5\r\n")
	send("TTL", "tdst")
	expect(t, r, ":-1\r\n")
	send("SET", "sdst", "plain")
	expect(t, r, "+OK\r\n")
	send("ZRANGESTORE", "sdst", "src", "0", "0")
	expect(t, r, ":1\r\n")
	send("TYPE", "sdst")
	expect(t, r, "+zset\r\n")
	send("ZRANGESTORE", "dst", "src", "20", "30")
	expect(t, r, ":0\r\n")
	send("TYPE", "dst")
	expect(t, r, "+none\r\n")
	send("ZRANGESTORE", "dl", "nokey", "0", "-1")
	expect(t, r, ":0\r\n")
	send("TYPE", "dl")
	expect(t, r, "+none\r\n")

	// The option doors, and a wrong-type source leaves dest alone.
	send("ZRANGESTORE", "dst", "src", "0")
	expect(t, r, "-ERR wrong number of arguments for 'zrangestore' command\r\n")
	send("ZRANGESTORE", "dst", "src", "0", "-1", "LIMIT", "0", "1")
	expect(t, r, "-ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX\r\n")
	send("ZRANGESTORE", "dst", "src", "0", "-1", "WITHSCORES")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZRANGESTORE", "tdst", "sdst", "0", "-1", "BYLEX")
	expect(t, r, "-ERR min or max not valid string range item\r\n")
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("ZRANGESTORE", "tdst", "str", "0", "-1")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZRANGE", "tdst", "0", "-1")
	expect(t, r, respArr("a", "b", "c", "d", "e"))
}
