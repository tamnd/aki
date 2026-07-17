package drivers

import "testing"

// TestTouchAllKeyspaces checks TOUCH counts how many of its keys exist, spanning
// every keyspace f3 keeps, exactly as EXISTS does. A single present key of each
// type counts, an absent key does not, a repeated key counts each time, and the
// multi-key form sums across shards. Keys are placed so the multi-key call
// gathers from more than one shard.
func TestTouchAllKeyspaces(t *testing.T) {
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

	// Single-key point path: one of each type counts, an absent key does not.
	for _, key := range []string{"s", "st", "zs", "h", "l", "str"} {
		send(t, nc, "TOUCH", key)
		expect(t, br, ":1\r\n")
	}
	send(t, nc, "TOUCH", "absent")
	expect(t, br, ":0\r\n")

	// Multi-key fan: all six present across shards, plus two absent, is 6.
	send(t, nc, "TOUCH", "s", "st", "zs", "h", "l", "str", "absent", "gone")
	expect(t, br, ":6\r\n")

	// A repeated present key counts each occurrence, matching EXISTS.
	send(t, nc, "TOUCH", "s", "s", "s")
	expect(t, br, ":3\r\n")
}
