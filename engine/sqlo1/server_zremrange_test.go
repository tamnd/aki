package sqlo1

import "testing"

func TestServerZRemRangeSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "6", "f")
	expect(t, r, ":6\r\n")

	// BYRANK: negative indices count from the end, out-of-range
	// clamps, inverted windows remove nothing.
	send("ZREMRANGEBYRANK", "z", "0", "1")
	expect(t, r, ":2\r\n")
	send("ZREMRANGEBYRANK", "z", "-1", "-1")
	expect(t, r, ":1\r\n")
	send("ZRANGE", "z", "0", "-1")
	expect(t, r, respArr("c", "d", "e"))
	send("ZREMRANGEBYRANK", "z", "5", "10")
	expect(t, r, ":0\r\n")
	send("ZREMRANGEBYRANK", "z", "2", "1")
	expect(t, r, ":0\r\n")

	// BYSCORE: inclusive by default, ( makes a bound exclusive, the
	// infinities cover the ends.
	send("ZADD", "z", "1", "a", "2", "b", "6", "f")
	expect(t, r, ":3\r\n")
	send("ZREMRANGEBYSCORE", "z", "(1", "3")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "z", "0", "-1")
	expect(t, r, respArr("a", "d", "e", "f"))
	send("ZREMRANGEBYSCORE", "z", "-inf", "1")
	expect(t, r, ":1\r\n")
	send("ZREMRANGEBYSCORE", "z", "5", "+inf")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "z", "0", "-1")
	expect(t, r, respArr("d"))

	// Removing the last member deletes the key.
	send("ZREMRANGEBYSCORE", "z", "-inf", "+inf")
	expect(t, r, ":1\r\n")
	send("TYPE", "z")
	expect(t, r, "+none\r\n")

	// BYLEX over a shared-score board: member bounds, exclusive
	// members, the - and + ends.
	send("ZADD", "w", "0", "aa", "0", "bb", "0", "cc", "0", "dd", "0", "ee")
	expect(t, r, ":5\r\n")
	send("ZREMRANGEBYLEX", "w", "[bb", "(dd")
	expect(t, r, ":2\r\n")
	send("ZRANGE", "w", "0", "-1")
	expect(t, r, respArr("aa", "dd", "ee"))
	send("ZREMRANGEBYLEX", "w", "-", "(dd")
	expect(t, r, ":1\r\n")
	send("ZREMRANGEBYLEX", "w", "[x", "+")
	expect(t, r, ":0\r\n")
	send("ZREMRANGEBYLEX", "w", "-", "+")
	expect(t, r, ":2\r\n")
	send("TYPE", "w")
	expect(t, r, "+none\r\n")

	// Absent keys answer 0 through every form.
	send("ZREMRANGEBYRANK", "gone", "0", "-1")
	expect(t, r, ":0\r\n")
	send("ZREMRANGEBYSCORE", "gone", "-inf", "+inf")
	expect(t, r, ":0\r\n")
	send("ZREMRANGEBYLEX", "gone", "-", "+")
	expect(t, r, ":0\r\n")

	// The error doors: arity, the three bound grammars, WRONGTYPE.
	send("ZREMRANGEBYRANK", "z", "0")
	expect(t, r, "-ERR wrong number of arguments for 'zremrangebyrank' command\r\n")
	send("ZREMRANGEBYRANK", "z", "zero", "1")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("ZREMRANGEBYSCORE", "z", "nope", "3")
	expect(t, r, "-ERR min or max is not a float\r\n")
	send("ZREMRANGEBYLEX", "z", "bb", "[dd")
	expect(t, r, "-ERR min or max not valid string range item\r\n")
	send("SET", "s", "v")
	expect(t, r, "+OK\r\n")
	send("ZREMRANGEBYRANK", "s", "0", "-1")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZREMRANGEBYSCORE", "s", "-inf", "+inf")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("ZREMRANGEBYLEX", "s", "-", "+")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
}
