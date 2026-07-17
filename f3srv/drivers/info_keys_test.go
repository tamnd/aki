package drivers

import "testing"

// TestInfoKeysAllKeyspaces checks the INFO keys counter spans every keyspace f3
// keeps, not just the string store. One key of each of the six types is placed
// across shards; INFO must report keys:6. Before infoShardAll wrapped the string
// InfoShard, only the string key counted, so this would have read 1. The count
// is the denominator the memory-bar accounting divides resident bytes by, so an
// undercount there overstates bytes-per-key.
func TestInfoKeysAllKeyspaces(t *testing.T) {
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

	if got := readInfo(t, nc, br)["keys"]; got != 6 {
		t.Fatalf("INFO keys = %d, want 6 (one of every keyspace)", got)
	}

	// Deleting the lone string key drops the count by exactly one: the string
	// store is one of the six summed arms, not the whole total.
	send(t, nc, "DEL", "s")
	expect(t, br, ":1\r\n")
	if got := readInfo(t, nc, br)["keys"]; got != 5 {
		t.Fatalf("INFO keys after DEL s = %d, want 5", got)
	}
}
