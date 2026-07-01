package f1srv

import "testing"

// The hash point path is element-per-row on f1raw: HSET writes one field row and
// maintains the header count, HGET is a single lock-free probe, and HLEN reads the
// header count with no scan. This exercises the whole slice-1 surface end to end over
// the wire.
func TestHashPointPath(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// HSET reports the count of newly created fields, not the count written.
	cmd(t, rw, "HSET", "h", "f1", "v1", "f2", "v2")
	expect(t, rw, ":2")
	cmd(t, rw, "HSET", "h", "f1", "v1b", "f3", "v3")
	expect(t, rw, ":1") // f1 updated, f3 new

	cmd(t, rw, "HGET", "h", "f1")
	expect(t, rw, "$v1b")
	cmd(t, rw, "HGET", "h", "f3")
	expect(t, rw, "$v3")
	cmd(t, rw, "HGET", "h", "missing")
	expect(t, rw, "$-1")
	cmd(t, rw, "HGET", "missing", "f1")
	expect(t, rw, "$-1")

	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":3")
	cmd(t, rw, "HLEN", "missing")
	expect(t, rw, ":0")

	cmd(t, rw, "HEXISTS", "h", "f2")
	expect(t, rw, ":1")
	cmd(t, rw, "HEXISTS", "h", "missing")
	expect(t, rw, ":0")

	cmd(t, rw, "HSTRLEN", "h", "f1")
	expect(t, rw, ":3") // "v1b"
	cmd(t, rw, "HSTRLEN", "h", "missing")
	expect(t, rw, ":0")
}

func TestHashMGet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")

	cmd(t, rw, "HMGET", "h", "a", "x", "c")
	expect(t, rw, "*3")
	expect(t, rw, "$1")
	expect(t, rw, "$-1")
	expect(t, rw, "$3")
}

func TestHashDel(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")

	// HDEL returns the count of fields actually removed, missing ones do not count.
	cmd(t, rw, "HDEL", "h", "a", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":2")
	cmd(t, rw, "HGET", "h", "a")
	expect(t, rw, "$-1")

	// Deleting the last fields drops the header, so HLEN reports 0 and the hash key
	// stops existing.
	cmd(t, rw, "HDEL", "h", "b", "c")
	expect(t, rw, ":2")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":0")
}

func TestHashSetNX(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSETNX", "h", "f", "first")
	expect(t, rw, ":1")
	cmd(t, rw, "HSETNX", "h", "f", "second")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$first")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":1")
}

// A hash field row and a string of the same key bytes must not collide: the record
// kind byte keeps the namespaces disjoint. Here a string key equal to the composite
// field-key bytes must not be seen by HGET, and vice versa.
func TestHashStringNamespaceDisjoint(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f", "hashval")
	expect(t, rw, ":1")

	// A plain string keyed by the bare hash key is independent of the hash.
	cmd(t, rw, "SET", "sk", "strval")
	expect(t, rw, "+OK")
	cmd(t, rw, "HGET", "sk", "f") // sk is a string, no such hash field
	expect(t, rw, "$-1")
	cmd(t, rw, "GET", "sk")
	expect(t, rw, "$strval")
	// The hash is untouched by the string write.
	cmd(t, rw, "HGET", "h", "f")
	expect(t, rw, "$hashval")
}

// HSET against a key that already holds a string is WRONGTYPE, the common type clash.
func TestHashWrongTypeOnString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSET", "k", "f", "1")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "HDEL", "k", "f")
	expect(t, rw, "-"+wrongType)
	// The string is still intact.
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v")
}

// FLUSHALL clears field and header rows along with strings.
func TestHashFlush(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "FLUSHALL")
	expect(t, rw, "+OK")
	cmd(t, rw, "HLEN", "h")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "a")
	expect(t, rw, "$-1")
}
