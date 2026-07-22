package drivers

import "testing"

// TestArenaSetLifecycleWire drives a tiny set through the real dispatch verbs to
// pin the keyspace-unification flip at the wire (spec 2064/f3/11): a tiny set homes
// inline in a store arena record, not the Go-heap registry, so every generic
// introspection verb that once mapped "a store record exists" to "a string" must now
// see the set. It checks TYPE, OBJECT ENCODING, MEMORY USAGE, and the key counters
// (DBSIZE and INFO keys) for a tiny arena set, that escalation past the inline bands
// keeps the count at one rather than double-counting the moved key, and that DEL
// clears it from both homes.
func TestArenaSetLifecycleWire(t *testing.T) {
	_, nc, br := startServer(t)

	// A tiny set: three small members stay in the listpack band, homed in the arena.
	send(t, nc, "SADD", "k", "a", "b", "c")
	expect(t, br, ":3\r\n")

	// TYPE must read "set", not "string": the store holds a record for the arena set,
	// but the type routing consults HasString so the record does not read as a string.
	send(t, nc, "TYPE", "k")
	expect(t, br, "+set\r\n")

	// OBJECT ENCODING reports the set band from the record bits word, not a string
	// encoding.
	send(t, nc, "OBJECT", "ENCODING", "k")
	expect(t, br, "$8\r\nlistpack\r\n")

	// MEMORY USAGE routes to the set's own footprint, a positive figure.
	if n := readInt(t, nc, br, "MEMORY", "USAGE", "k"); n <= 0 {
		t.Fatalf("MEMORY USAGE arena set = %d, want positive", n)
	}

	// The key counters count the arena set exactly once: the store's own keys stat
	// excludes the coll subset (Mem().Keys = count - collCount), and the INFO/DBSIZE
	// handlers fold the set registry's count back in, so the sum is one, not two.
	if n := readInt(t, nc, br, "DBSIZE"); n != 1 {
		t.Fatalf("DBSIZE with one arena set = %d, want 1", n)
	}
	if got := readInfo(t, nc, br)["keys"]; got != 1 {
		t.Fatalf("INFO keys with one arena set = %d, want 1", got)
	}

	// Escalate the set past the listpack entry cap so it evacuates to the Go-heap
	// registry. The key moved home, it was not added: the count stays one.
	for i := 0; i < 200; i++ {
		send(t, nc, "SADD", "k", "member-"+itoa(i))
		readIntReply(t, br, "SADD escalate")
	}
	send(t, nc, "OBJECT", "ENCODING", "k")
	expect(t, br, "$9\r\nhashtable\r\n")
	if n := readInt(t, nc, br, "DBSIZE"); n != 1 {
		t.Fatalf("DBSIZE after escalation = %d, want 1 (moved home, not a new key)", n)
	}

	// DEL clears the escalated set from its one home.
	send(t, nc, "DEL", "k")
	expect(t, br, ":1\r\n")
	if n := readInt(t, nc, br, "DBSIZE"); n != 0 {
		t.Fatalf("DBSIZE after DEL = %d, want 0", n)
	}
	send(t, nc, "TYPE", "k")
	expect(t, br, "+none\r\n")
}
