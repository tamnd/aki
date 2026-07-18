package drivers

import (
	"strconv"
	"testing"
)

// TestObjectRefcountEveryType drives OBJECT REFCOUNT over one key of each type
// through the real dispatch verb. f3 shares no allocation between keys, so a
// present key of any type reports a single reference, with the one Redis parity
// exception: a string holding a canonical integer in the shared range 0..9999
// reports the shared-refcount sentinel (INT_MAX) Redis uses for its interned
// small integers, while an integer outside that range or a non-integer string
// reports one. A key that exists nowhere is the "no such key" error, which is
// distinct from the null bulk OBJECT ENCODING gives a missing key.
func TestObjectRefcountEveryType(t *testing.T) {
	_, nc, br := startServer(t)

	// A missing key is an error here, not the null bulk ENCODING returns.
	send(t, nc, "OBJECT", "REFCOUNT", "nope")
	expect(t, br, "-ERR no such key\r\n")

	// A small integer string in the shared range reports the shared sentinel.
	send(t, nc, "SET", "s:small", "100")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "s:small")
	expect(t, br, ":2147483647\r\n")

	// The boundary: 9999 is shared, 10000 is not.
	send(t, nc, "SET", "s:edge", "9999")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "s:edge")
	expect(t, br, ":2147483647\r\n")
	send(t, nc, "SET", "s:big", "10000")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "s:big")
	expect(t, br, ":1\r\n")

	// A non-canonical integer (leading zero) is not a shared object, so one.
	send(t, nc, "SET", "s:lead", "007")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "s:lead")
	expect(t, br, ":1\r\n")

	// A plain string reports one.
	send(t, nc, "SET", "s:str", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "s:str")
	expect(t, br, ":1\r\n")

	// Every collection type reports one for a present key.
	send(t, nc, "RPUSH", "l", "a", "b", "c")
	expect(t, br, ":3\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "l")
	expect(t, br, ":1\r\n")

	send(t, nc, "SADD", "set:int", "1", "2", "3")
	expect(t, br, ":3\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "set:int")
	expect(t, br, ":1\r\n")

	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "h")
	expect(t, br, ":1\r\n")

	send(t, nc, "ZADD", "z", "1", "a", "2", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "OBJECT", "REFCOUNT", "z")
	expect(t, br, ":1\r\n")

	send(t, nc, "XADD", "st", "*", "k", "v")
	readBulk(t, br) // the generated entry ID
	send(t, nc, "OBJECT", "REFCOUNT", "st")
	expect(t, br, ":1\r\n")

	// A native-band zset of 200 members still reports one, not per-member counts.
	for i := 0; i < 200; i++ {
		send(t, nc, "ZADD", "z:big", strconv.Itoa(i), member(i))
		expect(t, br, ":1\r\n")
	}
	send(t, nc, "OBJECT", "REFCOUNT", "z:big")
	expect(t, br, ":1\r\n")
}
