package drivers

import (
	"testing"
)

// RENAME and RENAMENX relocate a key of any type over the wire, the generic move
// built on the DUMP/RESTORE serialize pair (dispatch/rename.go). These drive the
// real server so they cover both routes: a co-located pair on the point path and a
// cross-shard pair on the tier-two intent hop. With two shards, key names are chosen
// so the tests hit each route; a rename is correct whichever shard split it lands on,
// so the suite asserts the observable move, not the internal route.

// TestRenameTypes renames one key of every type and checks the value moved intact,
// the destination overwrote whatever it held, and the source is gone. RENAME reuses
// the full-fidelity RESTORE rebuild, so scores, fields, order, and stream ids all
// survive the move.
func TestRenameTypes(t *testing.T) {
	_, nc, br := startServer(t)

	// String.
	send(t, nc, "SET", "s1", "hello")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RENAME", "s1", "s2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "s2")
	expect(t, br, "$5\r\nhello\r\n")
	send(t, nc, "EXISTS", "s1")
	expect(t, br, ":0\r\n")

	// Set.
	send(t, nc, "SADD", "set1", "a", "b")
	expect(t, br, ":2\r\n")
	send(t, nc, "RENAME", "set1", "set2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SCARD", "set2")
	expect(t, br, ":2\r\n")
	send(t, nc, "TYPE", "set1")
	expect(t, br, "+none\r\n")

	// Sorted set: score survives.
	send(t, nc, "ZADD", "z1", "3.5", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "RENAME", "z1", "z2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "ZSCORE", "z2", "m")
	expect(t, br, "$3\r\n3.5\r\n")

	// Hash.
	send(t, nc, "HSET", "h1", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "RENAME", "h1", "h2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "HGET", "h2", "f")
	expect(t, br, "$1\r\nv\r\n")

	// List: order survives.
	send(t, nc, "RPUSH", "l1", "x", "y", "z")
	expect(t, br, ":3\r\n")
	send(t, nc, "RENAME", "l1", "l2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "LRANGE", "l2", "0", "-1")
	expect(t, br, "*3\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n")

	// Stream.
	send(t, nc, "XADD", "x1", "1-1", "k", "v")
	expect(t, br, "$3\r\n1-1\r\n")
	send(t, nc, "RENAME", "x1", "x2")
	expect(t, br, "+OK\r\n")
	send(t, nc, "XLEN", "x2")
	expect(t, br, ":1\r\n")
}

// TestRenameOverwriteAcrossTypes checks the destination is cleared whatever type it
// held: a string source renamed onto a live set leaves a string, not a merge.
func TestRenameOverwriteAcrossTypes(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SADD", "b", "x", "y", "z")
	expect(t, br, ":3\r\n")
	send(t, nc, "RENAME", "a", "b")
	expect(t, br, "+OK\r\n")
	send(t, nc, "TYPE", "b")
	expect(t, br, "+string\r\n")
	send(t, nc, "GET", "b")
	expect(t, br, "$1\r\nv\r\n")
}

// TestRenameTTL checks the destination inherits the source's remaining lifetime, and
// a persistent source leaves the destination persistent.
func TestRenameTTL(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v", "PX", "100000")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RENAME", "a", "b")
	expect(t, br, "+OK\r\n")
	send(t, nc, "TTL", "b")
	expect(t, br, ":100\r\n")

	send(t, nc, "SET", "c", "w")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RENAME", "c", "d")
	expect(t, br, "+OK\r\n")
	send(t, nc, "TTL", "d")
	expect(t, br, ":-1\r\n")
}

// TestRenameMissingAndSelf checks the source-existence contract: a missing source is
// the "no such key" error, and RENAME onto the same key confirms without dropping it.
func TestRenameMissingAndSelf(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RENAME", "nope", "dst")
	expect(t, br, "-ERR no such key\r\n")

	send(t, nc, "SET", "k", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RENAME", "k", "k")
	expect(t, br, "+OK\r\n")
	send(t, nc, "GET", "k")
	expect(t, br, "$1\r\nv\r\n")
}

// TestRenamenx checks the conditional twin: it installs and answers 1 when the
// destination is free, answers 0 and touches nothing when the destination is
// occupied, errors on a missing source, and answers 0 for a self-rename (the
// destination is the still-live source).
func TestRenamenx(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SET", "a", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RENAMENX", "a", "b")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "b")
	expect(t, br, "$1\r\nv\r\n")
	send(t, nc, "EXISTS", "a")
	expect(t, br, ":0\r\n")

	// Destination occupied: 0, and both keys untouched.
	send(t, nc, "SET", "c", "first")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SET", "d", "second")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RENAMENX", "c", "d")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "c")
	expect(t, br, "$5\r\nfirst\r\n")
	send(t, nc, "GET", "d")
	expect(t, br, "$6\r\nsecond\r\n")

	// Missing source.
	send(t, nc, "RENAMENX", "gone", "x")
	expect(t, br, "-ERR no such key\r\n")

	// Self: the destination is occupied by the source, so 0.
	send(t, nc, "RENAMENX", "c", "c")
	expect(t, br, ":0\r\n")
}

// TestRenameCrossShard forces both routes by renaming across a spread of key names.
// With two shards a large sweep of source/destination pairs necessarily hits pairs
// that split across shards (the tier-two hop) and pairs that co-locate (the point
// path); each pair must move its value intact and leave the source gone, whichever
// route it took. This is the differential check that the cross plan matches the
// point plan.
func TestRenameCrossShard(t *testing.T) {
	_, nc, br := startServer(t)

	const n = 40
	for i := 0; i < n; i++ {
		src := "src:" + itoa(i)
		dst := "dst:" + itoa(i)
		val := "val:" + itoa(i)
		send(t, nc, "SET", src, val)
		expect(t, br, "+OK\r\n")
		send(t, nc, "RENAME", src, dst)
		expect(t, br, "+OK\r\n")
		send(t, nc, "GET", dst)
		expect(t, br, "$"+itoa(len(val))+"\r\n"+val+"\r\n")
		send(t, nc, "EXISTS", src)
		expect(t, br, ":0\r\n")
	}
}

// itoa is a tiny base-10 helper for building distinguishable keys without pulling
// strconv into every call site.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
