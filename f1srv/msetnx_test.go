package f1srv

import (
	"testing"
	"time"
)

// MSETNX sets every pair only when none of the keys exist, all-or-nothing, replying 1 on a
// write and 0 when it does nothing. Every reply here was captured from live Redis 8.8.0 and
// Valkey 9.1.0, which agree byte for byte.
func TestMSetNX(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// All keys absent: the whole batch is written and the reply is 1.
	cmd(t, rw, "MSETNX", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":1")
	cmd(t, rw, "MGET", "a", "b", "c")
	got := readArray(t, rw)
	if len(got) != 3 || got[0] != "1" || got[1] != "2" || got[2] != "3" {
		t.Fatalf("MGET after MSETNX = %v, want [1 2 3]", got)
	}
}

// A single already-present key blocks the whole command: nothing is written, not even the
// pairs whose keys were absent, and the reply is 0.
func TestMSetNXAllOrNothing(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "b", "old")
	expect(t, rw, "+OK")

	// b exists, so a and c must not be written either.
	cmd(t, rw, "MSETNX", "a", "1", "b", "2", "c", "3")
	expect(t, rw, ":0")
	cmd(t, rw, "MGET", "a", "b", "c")
	got := readArray(t, rw)
	if len(got) != 3 || got[0] != "-1" || got[1] != "old" || got[2] != "-1" {
		t.Fatalf("MGET = %v, want a and c nil and b unchanged", got)
	}
	// EXISTS confirms a and c never came into being.
	cmd(t, rw, "EXISTS", "a", "c")
	expect(t, rw, ":0")
}

// Any type of value under a key counts as existing: a list at one of the keys blocks the
// batch just as a string does.
func TestMSetNXBlockedByAnyType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "x")
	expect(t, rw, ":1")
	cmd(t, rw, "MSETNX", "k", "1", "l", "2")
	expect(t, rw, ":0")
	// k was not written, l is still the list.
	cmd(t, rw, "EXISTS", "k")
	expect(t, rw, ":0")
	cmd(t, rw, "TYPE", "l")
	expect(t, rw, "+list")
}

// An expired key does not block MSETNX: once its TTL has passed it reads as absent, so the
// batch writes and replies 1, replacing it.
func TestMSetNXExpiredKeyDoesNotBlock(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "old", "PX", "1")
	expect(t, rw, "+OK")
	// Give the millisecond TTL time to pass.
	time.Sleep(30 * time.Millisecond)
	cmd(t, rw, "MSETNX", "a", "new", "b", "2")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "a")
	expect(t, rw, "$new")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$2")
}

// MSETNX carries no TTL, matching SETNX and MSET: a freshly written key has no expiry.
func TestMSetNXNoTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSETNX", "a", "1", "b", "2")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "a")
	expect(t, rw, ":-1")
}

// The argument count must be an odd number of at least three: a bare key with no value, or an
// even count, is an arity error.
func TestMSetNXArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "MSETNX", "a")
	expect(t, rw, "-ERR wrong number of arguments for 'msetnx' command")
	cmd(t, rw, "MSETNX", "a", "1", "b")
	expect(t, rw, "-ERR wrong number of arguments for 'msetnx' command")
}
