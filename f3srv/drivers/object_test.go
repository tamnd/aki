package drivers

import (
	"strconv"
	"testing"
)

// TestObjectEncodingEveryType drives OBJECT ENCODING over one key of each type
// through the real dispatch chain (stream, hash, list, set, zset, then string),
// asserting the exact encoding bytes Redis reports for each band. The chain is a
// single wired verb that delegates down the type packages, so this is the one
// place every type's encoding is checked together, including the zset band that
// used to fall through to the string store, and the null bulk Redis 8.8.0
// returns for OBJECT ENCODING on a key that exists nowhere.
func TestObjectEncodingEveryType(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key answers a null bulk, not an error, matching redis 8.8.0.
	send(t, nc, "OBJECT", "ENCODING", "nope")
	expect(t, br, "$-1\r\n")

	// String store: an integer value reports int, a short value embstr.
	send(t, nc, "SET", "s:int", "12345")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "ENCODING", "s:int")
	expect(t, br, bulk("int"))
	send(t, nc, "SET", "s:str", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "ENCODING", "s:str")
	expect(t, br, bulk("embstr"))

	// List inline band: listpack.
	send(t, nc, "RPUSH", "l", "a", "b", "c")
	expect(t, br, ":3\r\n")
	send(t, nc, "OBJECT", "ENCODING", "l")
	expect(t, br, bulk("listpack"))

	// Set of small integers: intset.
	send(t, nc, "SADD", "set:int", "1", "2", "3")
	expect(t, br, ":3\r\n")
	send(t, nc, "OBJECT", "ENCODING", "set:int")
	expect(t, br, bulk("intset"))

	// Hash inline band: listpack.
	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "OBJECT", "ENCODING", "h")
	expect(t, br, bulk("listpack"))

	// Zset inline band: listpack. This is the band that previously routed to the
	// string store, returning the wrong encoding.
	send(t, nc, "ZADD", "z:small", "1", "a", "2", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "OBJECT", "ENCODING", "z:small")
	expect(t, br, bulk("listpack"))

	// Zset native band: skiplist, once past the 128-entry inline cap.
	for i := 0; i < 200; i++ {
		send(t, nc, "ZADD", "z:big", strconv.Itoa(i), member(i))
		expect(t, br, ":1\r\n")
	}
	send(t, nc, "OBJECT", "ENCODING", "z:big")
	expect(t, br, bulk("skiplist"))

	// Stream: always reports the encoding stream.
	send(t, nc, "XADD", "st", "*", "k", "v")
	readBulk(t, br) // the generated entry ID
	send(t, nc, "OBJECT", "ENCODING", "st")
	expect(t, br, bulk("stream"))
}
