package drivers

import "testing"

// TestMsetexBasic walks MSETEX over a socket on the two-shard runtime: it writes
// every pair and replies 1, and its keys span shards so the cross-shard F17
// intent route carries the multi-key case. A single pair takes the co-located
// point path.
func TestMsetexBasic(t *testing.T) {
	_, nc, br := startServer(t)

	// Three keys over two shards force the cross-shard route.
	send(t, nc, "MSETEX", "3", "a", "1", "b", "2", "c", "3")
	expect(t, br, ":1\r\n")
	send(t, nc, "MGET", "a", "b", "c")
	expect(t, br, "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")

	// Single pair takes the co-located point path (keyAt=1).
	send(t, nc, "MSETEX", "1", "solo", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "solo")
	expect(t, br, "$1\r\nv\r\n")
}

// TestMsetexNX writes only when none of the keys exist; a single present key
// declines the whole command and leaves every key untouched.
func TestMsetexNX(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSETEX", "2", "a", "1", "b", "2", "NX")
	expect(t, br, ":1\r\n")

	// b already exists: NX declines and a2/c stay absent.
	send(t, nc, "MSETEX", "3", "a2", "9", "b", "9", "c", "9", "NX")
	expect(t, br, ":0\r\n")
	send(t, nc, "MGET", "a2", "b", "c")
	expect(t, br, "*3\r\n$-1\r\n$1\r\n2\r\n$-1\r\n")
}

// TestMsetexXX writes only when all of the keys already exist; a single missing
// key declines the whole command and writes nothing.
func TestMsetexXX(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSET", "a", "1", "b", "2")
	expect(t, br, "+OK\r\n")

	// Both present: XX updates them.
	send(t, nc, "MSETEX", "2", "a", "10", "b", "20", "XX")
	expect(t, br, ":1\r\n")
	send(t, nc, "MGET", "a", "b")
	expect(t, br, "*2\r\n$2\r\n10\r\n$2\r\n20\r\n")

	// c is missing: XX declines and a/b keep the values above.
	send(t, nc, "MSETEX", "3", "a", "99", "b", "99", "c", "99", "XX")
	expect(t, br, ":0\r\n")
	send(t, nc, "MGET", "a", "b", "c")
	expect(t, br, "*3\r\n$2\r\n10\r\n$2\r\n20\r\n$-1\r\n")
}

// TestMsetexExpiry sets a shared TTL on every key: EX installs a live deadline
// each key reports, and KEEPTTL on a later write preserves it.
func TestMsetexExpiry(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSETEX", "2", "a", "1", "b", "2", "EX", "100")
	expect(t, br, ":1\r\n")
	// Both keys carry a positive TTL near the requested 100s.
	send(t, nc, "TTL", "a")
	if ttl := readIntReply(t, br, "TTL"); ttl < 90 || ttl > 100 {
		t.Fatalf("TTL a = %d, want near 100", ttl)
	}
	send(t, nc, "TTL", "b")
	if ttl := readIntReply(t, br, "TTL"); ttl < 90 || ttl > 100 {
		t.Fatalf("TTL b = %d, want near 100", ttl)
	}

	// KEEPTTL rewrites the value but holds the deadline.
	send(t, nc, "MSETEX", "2", "a", "11", "b", "22", "KEEPTTL")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "a")
	expect(t, br, "$2\r\n11\r\n")
	send(t, nc, "TTL", "a")
	if ttl := readIntReply(t, br, "TTL"); ttl < 90 || ttl > 100 {
		t.Fatalf("TTL a after KEEPTTL = %d, want near 100", ttl)
	}

	// A plain MSETEX with no expiry option clears the TTL, like MSET.
	send(t, nc, "MSETEX", "1", "a", "x")
	expect(t, br, ":1\r\n")
	send(t, nc, "TTL", "a")
	expect(t, br, ":-1\r\n")
}

// TestMsetexBadExpiry rejects a non-positive expiry before any write, and an
// unknown trailing token is a syntax error.
func TestMsetexBadExpiry(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSETEX", "1", "a", "1", "EX", "0")
	expect(t, br, "-ERR invalid expire time in 'msetex' command\r\n")
	send(t, nc, "EXISTS", "a")
	expect(t, br, ":0\r\n")

	send(t, nc, "MSETEX", "1", "a", "1", "BOGUS")
	expect(t, br, "-ERR syntax error\r\n")
}

// TestMsetexArity rejects a bad numkeys and a short data block before any write.
func TestMsetexArity(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSETEX", "0", "a", "1")
	expect(t, br, "-ERR numkeys should be greater than 0\r\n")

	// numkeys says two pairs but only one is present.
	send(t, nc, "MSETEX", "2", "a", "1")
	expect(t, br, "-ERR wrong number of arguments for 'msetex' command\r\n")
	send(t, nc, "EXISTS", "a")
	expect(t, br, ":0\r\n")
}
