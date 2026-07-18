package drivers

import "testing"

// TestObjectFreqEveryType drives OBJECT FREQ over one key of each type through
// the real dispatch verb. Redis reports a key's LFU access counter only under an
// LFU maxmemory-policy and rejects the verb otherwise; f3 exposes no
// maxmemory-policy and keeps no per-key access counter, so a present key of any
// type always takes that rejection with the byte-for-byte redis 8.8.0 message,
// while a key present nowhere is the null bulk redis returns (distinct from the
// "no such key" error REFCOUNT gives a missing key). Both cases verified against
// redis 8.8.0 live.
func TestObjectFreqEveryType(t *testing.T) {
	_, nc, br := startServer(t)

	const lfuErr = "-ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.\r\n"

	// A missing key is the null bulk here, not the "no such key" REFCOUNT gives.
	send(t, nc, "OBJECT", "FREQ", "nope")
	expect(t, br, "$-1\r\n")

	// A present string reports the LFU-not-selected error.
	send(t, nc, "SET", "s", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "FREQ", "s")
	expect(t, br, lfuErr)

	// A present integer string takes the same branch (the shared-refcount parity
	// is a REFCOUNT concern, not FREQ).
	send(t, nc, "SET", "n", "100")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "FREQ", "n")
	expect(t, br, lfuErr)

	// Every collection type reports the error for a present key.
	send(t, nc, "RPUSH", "l", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "OBJECT", "FREQ", "l")
	expect(t, br, lfuErr)

	send(t, nc, "SADD", "set", "1", "2")
	expect(t, br, ":2\r\n")
	send(t, nc, "OBJECT", "FREQ", "set")
	expect(t, br, lfuErr)

	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "OBJECT", "FREQ", "h")
	expect(t, br, lfuErr)

	send(t, nc, "ZADD", "z", "1", "a")
	expect(t, br, ":1\r\n")
	send(t, nc, "OBJECT", "FREQ", "z")
	expect(t, br, lfuErr)

	send(t, nc, "XADD", "st", "*", "k", "v")
	readBulk(t, br) // the generated entry ID
	send(t, nc, "OBJECT", "FREQ", "st")
	expect(t, br, lfuErr)
}
