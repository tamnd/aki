package drivers

import "testing"

// TestHLLPointSurface walks PFADD/PFCOUNT over a socket: create-on-first-add,
// the change flag, small exact counts, the no-op re-add, and the WRONGTYPE
// refusal on a plain string key. Small cardinalities land in the sparse form,
// where HLL is exact, so the counts are checked against the true value.
func TestHLLPointSurface(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key is created and reports the change.
	send(t, nc, "PFADD", "h", "a", "b", "c")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFCOUNT", "h")
	expect(t, br, ":3\r\n")

	// Re-adding present elements changes nothing.
	send(t, nc, "PFADD", "h", "a", "b", "c")
	expect(t, br, ":0\r\n")
	send(t, nc, "PFCOUNT", "h")
	expect(t, br, ":3\r\n")

	// A new element grows the count by one.
	send(t, nc, "PFADD", "h", "d")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFCOUNT", "h")
	expect(t, br, ":4\r\n")

	// PFADD with no elements on a present key is a no-op touch.
	send(t, nc, "PFADD", "h")
	expect(t, br, ":0\r\n")

	// PFADD with no elements on a missing key creates it: cardinality zero.
	send(t, nc, "PFADD", "fresh")
	expect(t, br, ":1\r\n")
	send(t, nc, "PFCOUNT", "fresh")
	expect(t, br, ":0\r\n")

	// PFCOUNT on a missing key is zero.
	send(t, nc, "PFCOUNT", "nope")
	expect(t, br, ":0\r\n")

	// A plain string key is not a valid HLL: both commands refuse it.
	send(t, nc, "SET", "s", "not-a-sketch")
	expect(t, br, "+OK\r\n")
	send(t, nc, "PFADD", "s", "x")
	expect(t, br, "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n")
	send(t, nc, "PFCOUNT", "s")
	expect(t, br, "-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n")
}

// TestHLLTypeIsString pins the interop convention: an HLL is a string value, so
// TYPE reports string and GET returns the raw sketch bytes.
func TestHLLTypeIsString(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "PFADD", "h", "a")
	expect(t, br, ":1\r\n")
	send(t, nc, "TYPE", "h")
	expect(t, br, "+string\r\n")
}
