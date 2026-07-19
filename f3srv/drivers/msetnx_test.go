package drivers

import "testing"

// TestMsetnxAllOrNothing walks MSETNX over a socket on the two-shard runtime: it
// writes every pair only when all keys are absent, declines with 0 and writes
// nothing when any key is present, and its keys span shards so the cross-shard
// F17 intent route carries the multi-key case.
func TestMsetnxAllOrNothing(t *testing.T) {
	_, nc, br := startServer(t)

	// All absent: writes every pair, replies 1. Three keys over two shards force
	// the cross-shard route for at least one pair.
	send(t, nc, "MSETNX", "a", "1", "b", "2", "c", "3")
	expect(t, br, ":1\r\n")
	send(t, nc, "MGET", "a", "b", "c")
	expect(t, br, "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n")

	// One key present: declines with 0 and leaves the absent keys untouched.
	send(t, nc, "MSETNX", "d", "4", "b", "9", "e", "5")
	expect(t, br, ":0\r\n")
	send(t, nc, "MGET", "d", "e")
	expect(t, br, "*2\r\n$-1\r\n$-1\r\n")
	// The present key keeps its original value, never the declined write.
	send(t, nc, "GET", "b")
	expect(t, br, "$1\r\n2\r\n")

	// Single pair takes the co-located point path.
	send(t, nc, "MSETNX", "solo", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "GET", "solo")
	expect(t, br, "$1\r\nv\r\n")
	send(t, nc, "MSETNX", "solo", "w")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "solo")
	expect(t, br, "$1\r\nv\r\n")
}

// TestMsetnxCrossType confirms MSETNX honours existence in every keyspace, not
// just the string store: a key held as a set or a hash counts as present and
// declines the command, so MSETNX never shadows a live collection with a string.
func TestMsetnxCrossType(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "SADD", "s", "x")
	expect(t, br, ":1\r\n")
	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")

	// A set-held key blocks the write.
	send(t, nc, "MSETNX", "fresh", "1", "s", "2")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "fresh")
	expect(t, br, "$-1\r\n")
	// The set is untouched.
	send(t, nc, "SCARD", "s")
	expect(t, br, ":1\r\n")

	// A hash-held key blocks it too.
	send(t, nc, "MSETNX", "other", "1", "h", "2")
	expect(t, br, ":0\r\n")
	send(t, nc, "GET", "other")
	expect(t, br, "$-1\r\n")
}

// TestMsetnxArity rejects an odd argument count before any probe or write, the
// same malformed-command refusal Redis gives, on both the point and cross paths.
func TestMsetnxArity(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSETNX", "k1", "v1", "k2")
	expect(t, br, "-ERR wrong number of arguments for 'msetnx' command\r\n")
	// The malformed command wrote nothing.
	send(t, nc, "EXISTS", "k1", "k2")
	expect(t, br, ":0\r\n")
}
