package drivers

import "testing"

// TestObjectIdleTimeEveryType drives OBJECT IDLETIME over one key of each type
// through the real dispatch verb. A missing key is the null bulk redis 8.8.0
// returns (verified live: distinct from the "no such key" error REFCOUNT gives),
// a just-touched string reads its per-key access clock at idle zero, and a
// present collection reports zero for now (collections carry no access clock this
// slice). The exact-second arithmetic is proved deterministically in the store's
// idle_test; this test pins the wire shapes and dispatch routing.
func TestObjectIdleTimeEveryType(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key is the null bulk, not the "no such key" error.
	send(t, nc, "OBJECT", "IDLETIME", "nope")
	expect(t, br, "$-1\r\n")

	// A just-set string is idle zero (the SET stamps the clock).
	send(t, nc, "SET", "s", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "s")
	expect(t, br, ":0\r\n")

	// A GET restamps: still idle zero right after the read.
	send(t, nc, "GET", "s")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "s")
	expect(t, br, ":0\r\n")

	// A just-set integer string takes the same clocked path.
	send(t, nc, "SET", "n", "100")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "n")
	expect(t, br, ":0\r\n")

	// Every collection type reports zero for a present key this slice.
	send(t, nc, "RPUSH", "l", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "l")
	expect(t, br, ":0\r\n")

	send(t, nc, "SADD", "set", "1", "2")
	expect(t, br, ":2\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "set")
	expect(t, br, ":0\r\n")

	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "h")
	expect(t, br, ":0\r\n")

	send(t, nc, "ZADD", "z", "1", "a")
	expect(t, br, ":1\r\n")
	send(t, nc, "OBJECT", "IDLETIME", "z")
	expect(t, br, ":0\r\n")

	send(t, nc, "XADD", "st", "*", "k", "v")
	readBulk(t, br) // the generated entry ID
	send(t, nc, "OBJECT", "IDLETIME", "st")
	expect(t, br, ":0\r\n")
}
