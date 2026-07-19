package drivers

import (
	"testing"
)

// COPY duplicates a key of any type over the wire, RENAME's non-destructive sibling
// on the same DUMP/RESTORE serialize pair (dispatch/copy.go). These drive the real
// server so they cover both routes: a co-located pair on the point path and a
// cross-shard pair on the tier-two intent hop. COPY answers 1 on a copy and 0 when
// it declines (missing source, or an occupied destination without REPLACE), and it
// never removes the source.

// TestCopyTypes copies one key of every type and checks the value round-trips intact,
// the source stays put, and the answer is 1. COPY rides the full-fidelity RESTORE
// rebuild, so scores, fields, order, and stream ids all survive.
func TestCopyTypes(t *testing.T) {
	_, nc, br := startServer(t)

	// String.
	send(t, nc, "SET", "s1", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "COPY", "s1", "s2")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "s2")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "GET", "s1")
	expect(t, br, "$5\r\nhello\r\n")

	// Set.
	send(t, nc, "SADD", "set1", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "COPY", "set1", "set2")
	expect(t, br, ":1\r\n")
	send(t, nc, "SCARD", "set2")
	expect(t, br, ":2\r\n")
	send(t, nc, "SCARD", "set1")
	expect(t, br, ":2\r\n")

	// Sorted set: score survives.
	send(t, nc, "ZADD", "z1", "3.5", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "COPY", "z1", "z2")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZSCORE", "z2", "m")
	expect(t, br, "$3\r\n3.5\r\n")

	// Hash.
	send(t, nc, "HSET", "h1", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "COPY", "h1", "h2")
	expect(t, br, ":1\r\n")
	send(t, nc, "HGET", "h2", "f")
	expect(t, br, "$1\r\nv\r\n")

	// List: order survives.
	send(t, nc, "RPUSH", "l1", "x", "y", "z")
	expect(t, br, ":3\r\n")
	send(t, nc, "COPY", "l1", "l2")
	expect(t, br, ":1\r\n")
	send(t, nc, "LRANGE", "l2", "0", "-1")
	expect(t, br, "*3\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n")

	// Stream.
	send(t, nc, "XADD", "x1", "1-1", "k", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "COPY", "x1", "x2")
	expect(t, br, ":1\r\n")
	send(t, nc, "XLEN", "x2")
	expect(t, br, ":1\r\n")
}

// TestCopyMissingSource checks a missing source answers 0, not an error (unlike
// RENAME), and installs nothing.
func TestCopyMissingSource(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "COPY", "nope", "dst")
	expect(t, br, ":0\r\n")
	send(t, nc, "EXISTS", "dst")
	expect(t, br, ":0\r\n")
}

// TestCopyOccupiedAndReplace checks the destination guard: an occupied destination
// answers 0 and is left untouched without REPLACE, and REPLACE overwrites it (across
// types) and answers 1. The source survives either way.
func TestCopyOccupiedAndReplace(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "b", "old")
	expect(t, br, "+OK\r\n")

	// Occupied, no REPLACE: 0, destination untouched.
	send(t, nc, "COPY", "a", "b")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "b")
	expect(t, br, "$3\r\nold\r\n")

	// REPLACE overwrites, even a different type at the destination.
	send(t, nc, "SADD", "c", "x", "y", "z")
	expect(t, br, ":3\r\n")
	send(t, nc, "COPY", "a", "c", "REPLACE")
	expect(t, br, ":1\r\n")
	send(t, nc, "TYPE", "c")
	expect(t, br, "+string\r\n")
	send(t, nc, "GET", "c")
	expect(t, br, "$1\r\nv\r\n")

	// Source still present.
	send(t, nc, "GET", "a")
	expect(t, br, "$1\r\nv\r\n")
}

// TestCopyTTL checks the copy inherits the source's remaining lifetime, and a
// persistent source yields a persistent copy.
func TestCopyTTL(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v", "PX", "100000")
	expect(t, br, "+OK\r\n")
	send(t, nc, "COPY", "a", "b")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "b")
	expect(t, br, ":100\r\n")
	// Source TTL untouched.
	send(t, nc, "TTL", "a")
	expect(t, br, ":100\r\n")

	send(t, nc, "SET", "c", "w")
	expect(t, br, "+OK\r\n")
	send(t, nc, "COPY", "c", "d")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "d")
	expect(t, br, ":-1\r\n")
}

// TestCopySameKey checks COPY onto the same key is the "source and destination are
// the same" error, and the key is left intact.
func TestCopySameKey(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "COPY", "k", "k")
	expect(t, br, "-ERR source and destination objects are the same\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")
}

// TestCopyDBOption checks the single-database contract: DB 0 is accepted, any other
// index is out of range, a non-integer is the value error, and an unknown token or a
// dangling DB is a syntax error.
func TestCopyDBOption(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v")
	expect(t, br, "+OK\r\n")

	// DB 0 accepted.
	send(t, nc, "COPY", "a", "b", "DB", "0")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "b")
	expect(t, br, "$1\r\nv\r\n")

	// Non-zero DB out of range.
	send(t, nc, "COPY", "a", "e", "DB", "1")
	expect(t, br, "-ERR DB index is out of range\r\n")

	// Non-integer DB value.
	send(t, nc, "COPY", "a", "e", "DB", "xx")
	expect(t, br, "-ERR value is not an integer or out of range\r\n")

	// Dangling DB.
	send(t, nc, "COPY", "a", "e", "DB")
	expect(t, br, "-ERR syntax error\r\n")

	// Unknown token.
	send(t, nc, "COPY", "a", "e", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
}

// TestCopyCrossShard forces both routes by copying across a spread of key names. With
// two shards a large sweep of source/destination pairs necessarily hits pairs that
// split across shards (the tier-two hop) and pairs that co-locate (the point path);
// each pair must copy its value intact and leave the source present, whichever route
// it took. This is the differential check that the cross plan matches the point plan.
func TestCopyCrossShard(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 40
	for i := 0; i < n; i++ {
		src := "csrc:" + itoa(i)
		dst := "cdst:" + itoa(i)
		val := "cval:" + itoa(i)
		send(t, nc, "SET", src, val)
		expect(t, br, "+OK\r\n")
		send(t, nc, "COPY", src, dst)
		expect(t, br, ":1\r\n")
		send(t, nc, "GET", dst)
		expect(t, br, "$"+itoa(len(val))+"\r\n"+val+"\r\n")
		send(t, nc, "GET", src)
		expect(t, br, "$"+itoa(len(val))+"\r\n"+val+"\r\n")
	}

	// Second copy over an occupied destination without REPLACE declines, both routes.
	for i := 0; i < n; i++ {
		src := "csrc:" + itoa(i)
		dst := "cdst:" + itoa(i)
		send(t, nc, "COPY", src, dst)
		expect(t, br, ":0\r\n")
	}
}
