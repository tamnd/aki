package drivers

import (
	"bufio"
	"testing"
)

// keySet drains a KEYS reply of the given length into a set, so a test can
// assert the match membership without depending on the order, which KEYS leaves
// unspecified and which here also interleaves two shards' partials.
func keySet(t *testing.T, br *bufio.Reader, n int) map[string]bool {
	t.Helper()
	got := make(map[string]bool, n)
	for _, k := range readArrayBulks(t, br, n) {
		got[k] = true
	}
	return got
}

// TestKeysSpansEveryKeyspace checks KEYS walks every type f3 keeps, not just the
// string store: one key of each of the six types shows in KEYS *, and the keys
// spread across shards so the reply also exercises the gather concatenating each
// shard's matches. Before the keyspace walk landed KEYS could only have seen the
// string store, so a set, zset, hash, list, or stream key was invisible.
func TestKeysSpansEveryKeyspace(t *testing.T) {
	_, nc, br := startServer(t)

	// A fresh keyspace answers the empty array.
	send(t, nc, "KEYS", "*")
	if got := keySet(t, br, 0); len(got) != 0 {
		t.Fatalf("KEYS * on a fresh server = %v, want empty", got)
	}

	send(t, nc, "SET", "str:a", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SADD", "set:a", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZADD", "zset:a", "1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "HSET", "hash:a", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "RPUSH", "list:a", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "XADD", "stream:a", "*", "f", "v")
	if id := readBulk(t, br); len(id) == 0 {
		t.Fatal("XADD returned no id")
	}

	want := map[string]bool{
		"str:a": true, "set:a": true, "zset:a": true,
		"hash:a": true, "list:a": true, "stream:a": true,
	}
	send(t, nc, "KEYS", "*")
	got := keySet(t, br, len(want))
	for k := range want {
		if !got[k] {
			t.Fatalf("KEYS * missed %q; got %v", k, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("KEYS * = %v, want exactly %v", got, want)
	}
}

// TestKeysGlobFilters checks the pattern actually filters: a prefix glob, a
// single-character wildcard, and a character class each select their subset and
// nothing else, across keys of mixed types.
func TestKeysGlobFilters(t *testing.T) {
	_, nc, br := startServer(t)

	for _, k := range []string{"user:1", "user:2", "user:30"} {
		send(t, nc, "SET", k, "v")
		expect(t, br, "+OK\r\n")
	}
	send(t, nc, "SADD", "admin:1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "RPUSH", "user:list", "e")
	expect(t, br, ":1\r\n")

	// A prefix glob selects every user key regardless of type and skips admin.
	send(t, nc, "KEYS", "user:*")
	got := keySet(t, br, 4)
	for _, k := range []string{"user:1", "user:2", "user:30", "user:list"} {
		if !got[k] {
			t.Fatalf("KEYS user:* missed %q; got %v", k, got)
		}
	}
	if got["admin:1"] {
		t.Fatalf("KEYS user:* leaked admin:1; got %v", got)
	}

	// A single-character wildcard matches user:1 and user:2 but not user:30 or
	// the longer user:list.
	send(t, nc, "KEYS", "user:?")
	got = keySet(t, br, 2)
	if !got["user:1"] || !got["user:2"] {
		t.Fatalf("KEYS user:? = %v, want user:1 and user:2", got)
	}

	// A character class narrows the wildcard to a single value.
	send(t, nc, "KEYS", "user:[12]")
	got = keySet(t, br, 2)
	if !got["user:1"] || !got["user:2"] || got["user:30"] {
		t.Fatalf("KEYS user:[12] = %v, want user:1 and user:2 only", got)
	}
}

// TestKeysReflectsDeletionAndFlush checks the walk tracks the live keyspace: a
// deleted key stops matching, and after FLUSHALL the pattern answers empty.
func TestKeysReflectsDeletionAndFlush(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSET", "k:1", "a", "k:2", "b", "k:3", "c")
	expect(t, br, "+OK\r\n")

	send(t, nc, "DEL", "k:2")
	expect(t, br, ":1\r\n")

	send(t, nc, "KEYS", "k:*")
	got := keySet(t, br, 2)
	if !got["k:1"] || !got["k:3"] || got["k:2"] {
		t.Fatalf("KEYS k:* after delete = %v, want k:1 and k:3", got)
	}

	send(t, nc, "FLUSHALL")
	expect(t, br, "+OK\r\n")
	send(t, nc, "KEYS", "*")
	if got := keySet(t, br, 0); len(got) != 0 {
		t.Fatalf("KEYS * after FLUSHALL = %v, want empty", got)
	}
}
