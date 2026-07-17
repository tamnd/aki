package drivers

import "testing"

// TestMultiKeyExistsDelAllKeyspaces checks the multi-key EXISTS and DEL fan path
// spans every keyspace f3 keeps, the way the single-key point path does. Before
// the fan-threading fix these forms fanned through a string-only sub-handler, so
// a hash, list, zset, or stream key was invisible to an EXISTS or DEL over two or
// more keys. The keys spread across shards, so this also exercises the gather
// summing each shard's partial count.
func TestMultiKeyExistsDelAllKeyspaces(t *testing.T) {
	_, nc, br := startServer(t)

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
	if id := readBulk(t, br); len(id) == 0 {
		t.Fatalf("XADD id = %q, want an entry id", id)
	}

	// EXISTS over one key of every type plus two absent keys counts the six that
	// live, in any keyspace, and skips the two that do not.
	if n := readInt(t, nc, br, "EXISTS", "s", "st", "zs", "h", "l", "str", "gone1", "gone2"); n != 6 {
		t.Fatalf("multi-key EXISTS over all types = %d, want 6", n)
	}

	// EXISTS counts duplicates, the Redis contract: a live key named twice adds
	// two to the total. A duplicate key hashes to one shard, so the per-shard
	// count composes exactly.
	if n := readInt(t, nc, br, "EXISTS", "h", "h", "l"); n != 3 {
		t.Fatalf("multi-key EXISTS with a duplicate = %d, want 3", n)
	}

	// DEL over the mix removes the six live keys of every type and reports six,
	// skipping the absent one, where the string-only fan left the five collection
	// keys in place and counted only the string.
	if n := readInt(t, nc, br, "DEL", "s", "st", "zs", "h", "l", "str", "gone1"); n != 6 {
		t.Fatalf("multi-key DEL over all types = %d, want 6", n)
	}
	// Every key is gone now, across every keyspace.
	if n := readInt(t, nc, br, "EXISTS", "s", "st", "zs", "h", "l", "str"); n != 0 {
		t.Fatalf("EXISTS after multi-key DEL = %d, want 0", n)
	}
}

// TestMultiKeyUnlinkAllKeyspaces checks UNLINK shares the multi-key fan path and
// removes collection keys the same as DEL, since reclamation is owner-local and
// immediate.
func TestMultiKeyUnlinkAllKeyspaces(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "RPUSH", "l", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "HSET", "h", "f", "v")
	expect(t, br, ":1\r\n")

	if n := readInt(t, nc, br, "UNLINK", "l", "h", "absent"); n != 2 {
		t.Fatalf("multi-key UNLINK over collections = %d, want 2", n)
	}
	if n := readInt(t, nc, br, "EXISTS", "l", "h"); n != 0 {
		t.Fatalf("EXISTS after multi-key UNLINK = %d, want 0", n)
	}
}
