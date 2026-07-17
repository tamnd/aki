package drivers

import "testing"

// TestExistsSingleKeyAllKeyspaces checks the single-key EXISTS point path counts
// a key in any keyspace f3 keeps, not only the string store and the set
// registry. Before the keyspace-unification fix a lone hash, list, zset, or
// stream key answered 0. The multi-key fan form still spans string plus set
// only, so this covers just the single-key path.
func TestExistsSingleKeyAllKeyspaces(t *testing.T) {
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

	for _, key := range []string{"s", "st", "zs", "h", "l", "str"} {
		send(t, nc, "EXISTS", key)
		expect(t, br, ":1\r\n")
	}
	send(t, nc, "EXISTS", "absent")
	expect(t, br, ":0\r\n")
}
