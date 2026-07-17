package sqlo1

import "testing"

// The door assertions below are Redis 8.8.0's answers, probed live:
// the numkeys ladder (integer error, at-least-1 with the command's
// own name, syntax error past the argument count), the option loop's
// order-free last-wins grammar, and the family's up-front type check
// that no absent key masks.
func TestServerZAlgebraDoors(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "za", "1", "a", "2", "b", "3", "c")
	expect(t, r, ":3\r\n")
	send("ZADD", "zb", "5", "b", "1.5", "d")
	expect(t, r, ":2\r\n")
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")

	// Arity: read forms need numkeys and one key, STORE forms a dest
	// on top.
	send("ZUNION", "1")
	expect(t, r, "-ERR wrong number of arguments for 'zunion' command\r\n")
	send("ZINTERSTORE", "dest", "1")
	expect(t, r, "-ERR wrong number of arguments for 'zinterstore' command\r\n")
	send("ZINTERCARD", "1")
	expect(t, r, "-ERR wrong number of arguments for 'zintercard' command\r\n")

	// The numkeys ladder, each command's own name in the at-least-1
	// text and a plain syntax error past the argument count (not the
	// SINTERCARD texts).
	send("ZUNION", "notanint", "za")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("ZUNION", "0", "za")
	expect(t, r, "-ERR at least 1 input key is needed for 'zunion' command\r\n")
	send("ZINTERSTORE", "dest", "0", "za")
	expect(t, r, "-ERR at least 1 input key is needed for 'zinterstore' command\r\n")
	send("ZDIFF", "0", "za")
	expect(t, r, "-ERR at least 1 input key is needed for 'zdiff' command\r\n")
	send("ZINTERCARD", "0", "za")
	expect(t, r, "-ERR at least 1 input key is needed for 'zintercard' command\r\n")
	send("ZUNION", "3", "za", "zb")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZINTERCARD", "3", "za", "zb")
	expect(t, r, "-ERR syntax error\r\n")

	// WEIGHTS demands numkeys values in place, and a weight that does
	// not parse or is nan answers the weight text (inf is legal).
	send("ZUNION", "2", "za", "zb", "WEIGHTS", "1")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZUNION", "2", "za", "zb", "WEIGHTS", "1", "notafloat")
	expect(t, r, "-ERR weight value is not a float\r\n")
	send("ZUNION", "2", "za", "zb", "WEIGHTS", "1", "nan")
	expect(t, r, "-ERR weight value is not a float\r\n")

	// AGGREGATE needs a value from its set; anything else in the
	// tail, WITHSCORES on a STORE form included, is a syntax error.
	send("ZUNION", "2", "za", "zb", "AGGREGATE")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZUNION", "2", "za", "zb", "AGGREGATE", "AVG")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZUNION", "2", "za", "zb", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZUNIONSTORE", "dest", "2", "za", "zb", "WITHSCORES")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZDIFF", "2", "za", "zb", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZDIFFSTORE", "dest", "2", "za", "zb", "WITHSCORES")
	expect(t, r, "-ERR syntax error\r\n")

	// ZINTERCARD's tail is empty or LIMIT n; a limit that does not
	// parse answers the negative-limit text.
	send("ZINTERCARD", "2", "za", "zb", "LIMIT", "-1")
	expect(t, r, "-ERR LIMIT can't be negative\r\n")
	send("ZINTERCARD", "2", "za", "zb", "LIMIT", "notanint")
	expect(t, r, "-ERR LIMIT can't be negative\r\n")
	send("ZINTERCARD", "2", "za", "zb", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")

	// Every source is type-checked in argument order and absent keys
	// never mask a wrong type, unlike the set family.
	send("ZUNION", "2", "gone", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZINTER", "2", "str", "za")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZDIFF", "2", "gone", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZINTERCARD", "2", "gone", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZDIFFSTORE", "dest", "2", "gone", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}

func TestServerZAlgebraSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "za", "1", "a", "2", "b", "3", "c")
	expect(t, r, ":3\r\n")
	send("ZADD", "zb", "5", "b", "1.5", "d")
	expect(t, r, ":2\r\n")
	send("SADD", "st", "b", "x")
	expect(t, r, ":2\r\n")

	// Read replies sort by (score, member); WITHSCORES interleaves
	// through the shared formatter, integers printing bare.
	send("ZUNION", "2", "za", "zb")
	expect(t, r, respArr("a", "d", "c", "b"))
	send("ZUNION", "2", "za", "zb", "WITHSCORES")
	expect(t, r, respArr("a", "1", "d", "1.5", "c", "3", "b", "7"))
	send("ZINTER", "2", "za", "zb", "WITHSCORES")
	expect(t, r, respArr("b", "7"))
	send("ZDIFF", "2", "za", "zb", "WITHSCORES")
	expect(t, r, respArr("a", "1", "c", "3"))
	send("ZINTERCARD", "2", "za", "zb")
	expect(t, r, ":1\r\n")
	send("ZINTERCARD", "2", "za", "za", "LIMIT", "2")
	expect(t, r, ":2\r\n")
	send("ZINTERCARD", "2", "za", "za", "LIMIT", "0")
	expect(t, r, ":3\r\n")

	// The option loop: weights scale contributions, AGGREGATE is
	// case-insensitive and last-wins, options come in any order.
	send("ZUNION", "2", "za", "zb", "WEIGHTS", "2", "0.5", "WITHSCORES")
	expect(t, r, respArr("d", "0.75", "a", "2", "c", "6", "b", "6.5"))
	send("ZUNION", "2", "za", "zb", "AGGREGATE", "sum", "AGGREGATE", "max", "WITHSCORES")
	expect(t, r, respArr("a", "1", "d", "1.5", "c", "3", "b", "5"))
	send("ZINTER", "2", "za", "zb", "WITHSCORES", "AGGREGATE", "MIN")
	expect(t, r, respArr("b", "2"))

	// A plain set is a source with every member at 1, the measured
	// cross-type rule.
	send("ZUNION", "2", "za", "st", "WITHSCORES")
	expect(t, r, respArr("a", "1", "x", "1", "b", "3", "c", "3"))
	send("ZINTER", "2", "za", "st", "WITHSCORES")
	expect(t, r, respArr("b", "3"))
	send("ZDIFF", "2", "za", "st", "WITHSCORES")
	expect(t, r, respArr("a", "1", "c", "3"))

	// Absent keys are empty sources; an all-absent union is the empty
	// array, and an absent first key empties the diff.
	send("ZUNION", "2", "gone", "gone2")
	expect(t, r, "*0\r\n")
	send("ZDIFF", "2", "gone", "za")
	expect(t, r, "*0\r\n")

	// STORE forms answer the stored cardinality, land a real zset,
	// overwrite dest whatever it held, clear its TTL, and delete it
	// on an empty result.
	send("ZUNIONSTORE", "dest", "2", "za", "zb", "WEIGHTS", "2", "0.5")
	expect(t, r, ":4\r\n")
	send("ZRANGE", "dest", "0", "-1", "WITHSCORES")
	expect(t, r, respArr("d", "0.75", "a", "2", "c", "6", "b", "6.5"))
	send("SET", "strdest", "v")
	expect(t, r, "+OK\r\n")
	send("ZINTERSTORE", "strdest", "2", "za", "zb")
	expect(t, r, ":1\r\n")
	send("ZRANGE", "strdest", "0", "-1", "WITHSCORES")
	expect(t, r, respArr("b", "7"))
	send("EXPIRE", "strdest", "1000")
	expect(t, r, ":1\r\n")
	send("ZDIFFSTORE", "strdest", "2", "za", "zb")
	expect(t, r, ":2\r\n")
	send("TTL", "strdest")
	expect(t, r, ":-1\r\n")
	send("ZINTERSTORE", "dest", "2", "za", "gone")
	expect(t, r, ":0\r\n")
	send("TYPE", "dest")
	expect(t, r, "+none\r\n")
	send("ZDIFFSTORE", "strdest", "2", "za", "za")
	expect(t, r, ":0\r\n")
	send("TYPE", "strdest")
	expect(t, r, "+none\r\n")
}
