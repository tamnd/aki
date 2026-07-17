package drivers

import "testing"

// TestRandomkeyEmptyIsNull checks RANDOMKEY answers the null bulk when no key
// exists on any shard, rather than an error or an empty string.
func TestRandomkeyEmptyIsNull(t *testing.T) {
	_, nc, br := startServer(t)
	send(t, nc, "RANDOMKEY")
	expect(t, br, "$-1\r\n")
}

// TestRandomkeyReturnsALiveKey checks RANDOMKEY draws a key that actually
// exists, across keys of every type and both shards, and that every draw lands
// on a live key. It repeats enough to catch a draw that reaches an empty shard
// or a keyspace the walk cannot see.
func TestRandomkeyReturnsALiveKey(t *testing.T) {
	_, nc, br := startServer(t)

	live := map[string]bool{
		"str:a": true, "set:a": true, "zset:a": true,
		"hash:a": true, "list:a": true, "stream:a": true,
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

	seen := make(map[string]bool)
	for i := 0; i < 200; i++ {
		send(t, nc, "RANDOMKEY")
		k := readBulk(t, br)
		if !live[k] {
			t.Fatalf("RANDOMKEY returned %q, not a live key", k)
		}
		seen[k] = true
	}
	// Over 200 draws the reservoir should have surfaced more than one distinct
	// key: a draw stuck on a single key or shard would fail this.
	if len(seen) < 2 {
		t.Fatalf("RANDOMKEY only ever returned %v across 200 draws", seen)
	}
}

// TestRandomkeyAfterFlush checks the draw tracks the live keyspace: once every
// key is gone the draw is null again.
func TestRandomkeyAfterFlush(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "MSET", "k:1", "a", "k:2", "b")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RANDOMKEY")
	if k := readBulk(t, br); k != "k:1" && k != "k:2" {
		t.Fatalf("RANDOMKEY = %q, want k:1 or k:2", k)
	}

	send(t, nc, "FLUSHALL")
	expect(t, br, "+OK\r\n")
	send(t, nc, "RANDOMKEY")
	expect(t, br, "$-1\r\n")
}
