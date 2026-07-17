package drivers

import "testing"

// TestExpiryReadAllKeyspaces checks the read-only expiry queries and PERSIST
// span every keyspace. A collection key of any type carries no key-level
// deadline, so TTL and PTTL report -1 (live, no deadline) and PERSIST reports 0
// (nothing to remove). Before the keyspace-unification fix these resolved
// through a set-only helper, so a hash, list, zset, or stream key answered -2
// (missing key) for the reads.
func TestExpiryReadAllKeyspaces(t *testing.T) {
	_, nc, br := startServer(t)

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

	for _, key := range []string{"st", "zs", "h", "l", "str"} {
		if ttl := readInt(t, nc, br, "TTL", key); ttl != -1 {
			t.Fatalf("TTL %s = %d, want -1 (live, no deadline)", key, ttl)
		}
		if pttl := readInt(t, nc, br, "PTTL", key); pttl != -1 {
			t.Fatalf("PTTL %s = %d, want -1", key, pttl)
		}
		if et := readInt(t, nc, br, "EXPIRETIME", key); et != -1 {
			t.Fatalf("EXPIRETIME %s = %d, want -1", key, et)
		}
		if pet := readInt(t, nc, br, "PEXPIRETIME", key); pet != -1 {
			t.Fatalf("PEXPIRETIME %s = %d, want -1", key, pet)
		}
		if p := readInt(t, nc, br, "PERSIST", key); p != 0 {
			t.Fatalf("PERSIST %s = %d, want 0 (no deadline to remove)", key, p)
		}
	}

	// An absent key still reports the missing-key sentinel.
	if ttl := readInt(t, nc, br, "TTL", "absent"); ttl != -2 {
		t.Fatalf("TTL absent = %d, want -2", ttl)
	}
}
