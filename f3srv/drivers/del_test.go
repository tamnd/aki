package drivers

import "testing"

// TestDelSingleKeyAllKeyspaces checks the single-key DEL point path removes a
// key of any type and reports 1, where before the keyspace-unification fix a
// lone hash, list, zset, or stream key answered 0 and stayed in place. It
// deletes each key, confirms the reply, then confirms the key is gone. UNLINK
// shares the handler, so the last case checks it too.
func TestDelSingleKeyAllKeyspaces(t *testing.T) {
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
		send(t, nc, "DEL", key)
		expect(t, br, ":1\r\n")
		send(t, nc, "EXISTS", key)
		expect(t, br, ":0\r\n")
		send(t, nc, "TYPE", key)
		expect(t, br, "+none\r\n")
	}

	// DEL of an absent key reports 0.
	send(t, nc, "DEL", "gone")
	expect(t, br, ":0\r\n")

	// UNLINK shares the point path and removes a collection key just the same.
	send(t, nc, "RPUSH", "u", "e")
	expect(t, br, ":1\r\n")
	send(t, nc, "UNLINK", "u")
	expect(t, br, ":1\r\n")
	send(t, nc, "EXISTS", "u")
	expect(t, br, ":0\r\n")
}

// TestDelStreamThenRecreate checks a deleted stream leaves nothing behind and a
// following XADD builds a fresh stream at the key, the invariant the registry's
// keep-empty-streams rule must not violate for a truly deleted key.
func TestDelStreamThenRecreate(t *testing.T) {
	_, nc, br := startServer(t)

	send(t, nc, "XADD", "s", "*", "f", "v")
	if id := readBulk(t, br); len(id) == 0 {
		t.Fatalf("XADD id = %q", id)
	}
	send(t, nc, "DEL", "s")
	expect(t, br, ":1\r\n")
	// The key is gone, so XLEN reports 0 for a missing stream, not a kept-empty one.
	if n := readInt(t, nc, br, "XLEN", "s"); n != 0 {
		t.Fatalf("XLEN after DEL = %d, want 0", n)
	}
	send(t, nc, "XADD", "s", "*", "f2", "v2")
	if id := readBulk(t, br); len(id) == 0 {
		t.Fatalf("XADD after recreate = %q", id)
	}
	if n := readInt(t, nc, br, "XLEN", "s"); n != 1 {
		t.Fatalf("XLEN after recreate = %d, want 1", n)
	}
}
