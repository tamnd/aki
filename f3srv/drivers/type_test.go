package drivers

import "testing"

// TestTypeAllKeyspaces checks TYPE names every keyspace f3 keeps: a string
// value, all five collection types, and "none" for an absent key. This is the
// keyspace-unification fix: TYPE used to see only the string store and the set
// registry, so a hash, list, zset, or stream key answered "none".
func TestTypeAllKeyspaces(t *testing.T) {
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

	for _, tc := range []struct{ key, want string }{
		{"s", "+string\r\n"},
		{"st", "+set\r\n"},
		{"zs", "+zset\r\n"},
		{"h", "+hash\r\n"},
		{"l", "+list\r\n"},
		{"str", "+stream\r\n"},
		{"absent", "+none\r\n"},
	} {
		send(t, nc, "TYPE", tc.key)
		expect(t, br, tc.want)
	}
}
