package sqlo1

import "testing"

// The doors below are Redis 8.8.0's answers to ZSCAN, probed live:
// the shared scan grammar with COUNT and MATCH, NOVALUES parsed only
// to be rejected with HSCAN's ownership text, and the flat member,
// score reply through the shared formatter. Redis leaves scan order
// inside a step unspecified, so the order pinned here is ours.
func TestServerZScan(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Arity and the cursor door: a cursor that does not parse as an
	// unsigned integer, negatives included, answers the cursor text.
	send("ZSCAN", "zk")
	expect(t, r, "-ERR wrong number of arguments for 'zscan' command\r\n")
	send("ZSCAN", "zk", "notanint")
	expect(t, r, "-ERR invalid cursor\r\n")
	send("ZSCAN", "zk", "-1")
	expect(t, r, "-ERR invalid cursor\r\n")

	// The option loop: COUNT below one is a syntax error, a COUNT
	// that does not parse is the integer error, MATCH with no argument
	// and any unknown token are syntax errors, and NOVALUES answers
	// HSCAN's ownership text.
	send("ZSCAN", "zk", "0", "COUNT", "0")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZSCAN", "zk", "0", "COUNT", "xx")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("ZSCAN", "zk", "0", "MATCH")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZSCAN", "zk", "0", "BOGUS")
	expect(t, r, "-ERR syntax error\r\n")
	send("ZSCAN", "zk", "0", "NOVALUES")
	expect(t, r, "-ERR NOVALUES option can only be used in HSCAN\r\n")

	// An absent key is cursor 0 and an empty array, not an error, and
	// a wrong type fails before the walk.
	send("ZSCAN", "ghost", "0")
	expect(t, r, "*2\r\n$1\r\n0\r\n*0\r\n")
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("ZSCAN", "str", "0")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

	// The reply is flat member, score pairs through the shared score
	// formatter, and MATCH filters members only.
	send("ZADD", "zk", "1.5", "a", "2", "b", "3", "c")
	expect(t, r, ":3\r\n")
	send("ZSCAN", "zk", "0")
	expect(t, r, "*2\r\n$1\r\n0\r\n*6\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n")
	send("ZSCAN", "zk", "0", "MATCH", "a*")
	expect(t, r, "*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$3\r\n1.5\r\n")
	send("ZSCAN", "zk", "0", "COUNT", "5", "MATCH", "b*", "COUNT", "100")
	expect(t, r, "*2\r\n$1\r\n0\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n")
}
