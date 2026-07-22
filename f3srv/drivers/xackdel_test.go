package drivers

import "testing"

// TestXackdelBasic acks a pending entry out of the group and deletes it under the
// default KEEPREF strategy: code 1. An id that was never pending reports -1 and is
// left alone.
func TestXackdelBasic(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XADD", "s", "2-1", "f", "v")
	expect(t, br, "$3\r\n2-1\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, br, "+OK\r\n")
	// Deliver only 1-1, leaving 2-1 never pending.
	send(t, nc, "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">")
	readRESP(t, br)

	// 1-1 pending -> acked+deleted (1); 2-1 not pending -> -1.
	send(t, nc, "XACKDEL", "s", "g", "IDS", "2", "1-1", "2-1")
	expect(t, br, "*2\r\n:1\r\n:-1\r\n")

	// 1-1 gone, 2-1 still there.
	send(t, nc, "XLEN", "s")
	expect(t, br, ":1\r\n")
	// The group PEL is now empty.
	send(t, nc, "XACK", "s", "g", "1-1")
	expect(t, br, ":0\r\n")
}

// TestXackdelMissingKeyGroup replies -1 per id for a missing key and for an
// existing key with an unknown group.
func TestXackdelMissingKeyGroup(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XACKDEL", "nope", "g", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:-1\r\n")

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XACKDEL", "s", "missing", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:-1\r\n")
}

// TestXackdelAcked with two groups: acking out of one group leaves the entry still
// referenced by the other (code 2), so ACKED does not delete it until it has been
// acked out of every group.
func TestXackdelAcked(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g1", "0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g2", "0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "XREADGROUP", "GROUP", "g1", "c", "STREAMS", "s", ">")
	readRESP(t, br)
	send(t, nc, "XREADGROUP", "GROUP", "g2", "c", "STREAMS", "s", ">")
	readRESP(t, br)

	// Ack out of g1: g2 still references 1-1, so ACKED keeps it (code 2).
	send(t, nc, "XACKDEL", "s", "g1", "ACKED", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:2\r\n")
	send(t, nc, "XLEN", "s")
	expect(t, br, ":1\r\n")

	// Ack out of g2: now nothing references it, so ACKED deletes it (code 1).
	send(t, nc, "XACKDEL", "s", "g2", "ACKED", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:1\r\n")
	send(t, nc, "XLEN", "s")
	expect(t, br, ":0\r\n")
}

// TestXackdelDelref acks out of one group and, with DELREF, scrubs the entry from
// the other group's PEL as it deletes it.
func TestXackdelDelref(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g1", "0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g2", "0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "XREADGROUP", "GROUP", "g1", "c", "STREAMS", "s", ">")
	readRESP(t, br)
	send(t, nc, "XREADGROUP", "GROUP", "g2", "c", "STREAMS", "s", ">")
	readRESP(t, br)

	// DELREF deletes and scrubs g2's dangling reference.
	send(t, nc, "XACKDEL", "s", "g1", "DELREF", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:1\r\n")
	send(t, nc, "XLEN", "s")
	expect(t, br, ":0\r\n")
	// g2's PEL was scrubbed: it holds nothing pending.
	send(t, nc, "XPENDING", "s", "g2")
	expect(t, br, "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n")
}

// TestXackdelErrors rejects a bad numids, a mismatched count, a malformed id, and
// an unknown option, each with its Redis error text.
func TestXackdelErrors(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XACKDEL", "s", "g", "IDS", "0", "1-1")
	expect(t, br, "-ERR Number of IDs must be a positive integer\r\n")

	send(t, nc, "XACKDEL", "s", "g", "IDS", "3", "1-1")
	expect(t, br, "-ERR The `numids` parameter must match the number of arguments\r\n")

	send(t, nc, "XACKDEL", "s", "g", "BOGUS", "IDS", "1", "1-1")
	expect(t, br, "-ERR syntax error\r\n")

	send(t, nc, "XACKDEL", "s", "g", "IDS", "1", "not-an-id")
	expect(t, br, "-ERR Invalid stream ID specified as stream command argument\r\n")
}
