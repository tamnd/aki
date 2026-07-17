package drivers

import "testing"

// TestDbsizeCountsEveryKeyspace checks DBSIZE counts keys of every type f3
// keeps, not only the string store: before the fix a set, zset, hash, list, or
// stream key was invisible to DBSIZE, so a database of pure collections reported
// zero. The keys spread across shards, so this also exercises the gather summing
// each shard's partial count.
func TestDbsizeCountsEveryKeyspace(t *testing.T) {
	_, nc, br := startServer(t)

	if n := readInt(t, nc, br, "DBSIZE"); n != 0 {
		t.Fatalf("DBSIZE on a fresh server = %d, want 0", n)
	}

	send(t, nc, "SET", "s", "v")
	expect(t, br, "+OK\r\n")
	send(t, nc, "SADD", "st", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "ZADD", "zs", "1", "m")
	expect(t, br, ":1\r\n")
	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")
	send(t, nc, "RPUSH", "l", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "XADD", "str", "*", "f", "v")
	streamID := readBulk(t, br)
	if len(streamID) == 0 {
		t.Fatalf("XADD id = %q, want an entry id", streamID)
	}

	if n := readInt(t, nc, br, "DBSIZE"); n != 6 {
		t.Fatalf("DBSIZE with one key of every type = %d, want 6", n)
	}

	// Deleting two keys of collection types drops the count by two.
	if n := readInt(t, nc, br, "DEL", "l", "zs"); n != 2 {
		t.Fatalf("DEL two collection keys = %d, want 2", n)
	}
	if n := readInt(t, nc, br, "DBSIZE"); n != 4 {
		t.Fatalf("DBSIZE after deleting two = %d, want 4", n)
	}

	// An emptied-but-kept stream still counts as a key: XDEL of its only entry
	// leaves the stream in place, so DBSIZE holds.
	send(t, nc, "XDEL", "str", streamID)
	expect(t, br, ":1\r\n")
	if n := readInt(t, nc, br, "DBSIZE"); n != 4 {
		t.Fatalf("DBSIZE after emptying a kept stream = %d, want 4", n)
	}

	// A flush zeroes the count across every keyspace.
	send(t, nc, "FLUSHALL")
	expect(t, br, "+OK\r\n")
	if n := readInt(t, nc, br, "DBSIZE"); n != 0 {
		t.Fatalf("DBSIZE after FLUSHALL = %d, want 0", n)
	}
}
