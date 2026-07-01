package f1srv

import (
	"fmt"
	"testing"
)

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

// HGETALL, HKEYS, and HVALS enumerate one hash in field-key order off the ordered
// index, framing the RESP array from the maintained header count so the length always
// matches what is streamed.
func TestHashEnumerate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "b", "2", "a", "1", "c", "3")
	expect(t, rw, ":3")

	// HKEYS is the field names in byte order.
	cmd(t, rw, "HKEYS", "h")
	expect(t, rw, "*3")
	expect(t, rw, "$a")
	expect(t, rw, "$b")
	expect(t, rw, "$c")

	// HVALS is the values in the same order.
	cmd(t, rw, "HVALS", "h")
	expect(t, rw, "*3")
	expect(t, rw, "$1")
	expect(t, rw, "$2")
	expect(t, rw, "$3")

	// HGETALL interleaves field and value, still in field order.
	cmd(t, rw, "HGETALL", "h")
	expect(t, rw, "*6")
	expect(t, rw, "$a")
	expect(t, rw, "$1")
	expect(t, rw, "$b")
	expect(t, rw, "$2")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
}

// A missing hash enumerates as an empty array, not an error or nil.
func TestHashEnumerateMissing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HGETALL", "nope")
	expect(t, rw, "*0")
	cmd(t, rw, "HKEYS", "nope")
	expect(t, rw, "*0")
	cmd(t, rw, "HVALS", "nope")
	expect(t, rw, "*0")
}

// Enumeration tracks deletes and overwrites: a deleted field drops out, an overwritten
// field keeps its slot with the fresh value, and a field whose value outgrew its record
// still enumerates with the new value.
func TestHashEnumerateAfterMutate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":3")
	cmd(t, rw, "HDEL", "h", "b")
	expect(t, rw, ":1")
	// Overwrite a with a much longer value so the record is republished at a new offset.
	cmd(t, rw, "HSET", "h", "a", "a-much-longer-value-than-before")
	expect(t, rw, ":0")

	cmd(t, rw, "HGETALL", "h")
	expect(t, rw, "*4")
	expect(t, rw, "$a")
	expect(t, rw, "$a-much-longer-value-than-before")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
}

// Enumeration must stream more fields than one internal scan batch without dropping or
// duplicating any, so the RESP length matches across the batch boundary.
func TestHashEnumerateManyFields(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 1000
	args := []string{"HSET", "big"}
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("f%05d", i)
		args = append(args, f, "v")
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "HLEN", "big")
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "HKEYS", "big")
	expect(t, rw, fmt.Sprintf("*%d", n))
	for i := 0; i < n; i++ {
		expect(t, rw, "$"+fmt.Sprintf("f%05d", i))
	}
}

// HGETALL against a string key is WRONGTYPE, matching the point-path type guard.
func TestHashEnumerateWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "HGETALL", "k")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "HKEYS", "k")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "HVALS", "k")
	expect(t, rw, "-"+wrongType)
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
