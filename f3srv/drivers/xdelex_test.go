package drivers

import "testing"

// TestXdelexKeepref deletes entries with the default KEEPREF strategy and replies
// a per-id status array: 1 for a deleted entry, -1 for an id not in the stream.
func TestXdelexKeepref(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XADD", "s", "2-1", "f", "v")
	expect(t, br, "$3\r\n2-1\r\n")

	// 1-1 present (deleted -> 1), 9-9 absent (-1).
	send(t, nc, "XDELEX", "s", "IDS", "2", "1-1", "9-9")
	expect(t, br, "*2\r\n:1\r\n:-1\r\n")

	send(t, nc, "XLEN", "s")
	expect(t, br, ":1\r\n")
}

// TestXdelexMissingKey replies an array of -1, one per id, for a stream that does
// not exist.
func TestXdelexMissingKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XDELEX", "nope", "IDS", "2", "1-1", "2-2")
	expect(t, br, "*2\r\n:-1\r\n:-1\r\n")
}

// TestXdelexAcked keeps a still-pending entry (code 2) and deletes it only once it
// has been acknowledged by every group (code 1).
func TestXdelexAcked(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, br, "+OK\r\n")
	// Deliver 1-1 to c1, making it pending in group g.
	send(t, nc, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	readRESP(t, br)

	// ACKED refuses to delete a referenced entry.
	send(t, nc, "XDELEX", "s", "ACKED", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:2\r\n")
	send(t, nc, "XLEN", "s")
	expect(t, br, ":1\r\n")

	// Ack it, then ACKED deletes it.
	send(t, nc, "XACK", "s", "g", "1-1")
	expect(t, br, ":1\r\n")
	send(t, nc, "XDELEX", "s", "ACKED", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:1\r\n")
	send(t, nc, "XLEN", "s")
	expect(t, br, ":0\r\n")
}

// TestXdelexDelref deletes a still-pending entry and scrubs it from the group PEL,
// so the group is left holding no reference to the gone entry.
func TestXdelexDelref(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "1-1", "f", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "XGROUP", "CREATE", "s", "g", "0")
	expect(t, br, "+OK\r\n")
	send(t, nc, "XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">")
	readRESP(t, br)

	// One pending entry before the DELREF delete.
	send(t, nc, "XPENDING", "s", "g")
	readRESP(t, br)

	send(t, nc, "XDELEX", "s", "DELREF", "IDS", "1", "1-1")
	expect(t, br, "*1\r\n:1\r\n")

	// PEL scrubbed: the summary now reports zero pending.
	send(t, nc, "XPENDING", "s", "g")
	expect(t, br, "*4\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n")
}

// TestXdelexErrors rejects a bad numids, a missing IDS clause, a malformed id, and
// an unknown option, each with its Redis error text.
func TestXdelexErrors(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XDELEX", "s", "IDS", "0", "1-1")
	expect(t, br, "-ERR Number of IDs must be a positive integer\r\n")

	send(t, nc, "XDELEX", "s", "IDS", "3", "1-1")
	expect(t, br, "-ERR The `numids` parameter must match the number of arguments\r\n")

	send(t, nc, "XDELEX", "s", "BOGUS", "IDS", "1", "1-1")
	expect(t, br, "-ERR syntax error\r\n")

	send(t, nc, "XDELEX", "s", "IDS", "1", "not-an-id")
	expect(t, br, "-ERR Invalid stream ID specified as stream command argument\r\n")
}
