package f1srv

import (
	"bufio"
	"testing"
)

// TestRenameString moves a plain string and checks the source is gone and the value lands intact.
func TestRenameString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "RENAME", "a", "b")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXISTS", "a")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$hello")
}

// TestRenameMissingSource returns the exact Redis error when the source is absent, for both verbs.
func TestRenameMissingSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RENAME", "nope", "dst")
	expect(t, rw, "-ERR no such key")
	cmd(t, rw, "RENAMENX", "nope", "dst")
	expect(t, rw, "-ERR no such key")
	// Source equal to a missing destination is still an absent source.
	cmd(t, rw, "RENAME", "same", "same")
	expect(t, rw, "-ERR no such key")
	cmd(t, rw, "RENAMENX", "same", "same")
	expect(t, rw, "-ERR no such key")
}

// TestRenameOverwrite overwrites an existing destination of a different type and drops its rows.
func TestRenameOverwrite(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v1")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSET", "b", "f1", "x", "f2", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "RENAME", "a", "b")
	expect(t, rw, "+OK")
	// b is now the string, not the old hash.
	cmd(t, rw, "TYPE", "b")
	expect(t, rw, "+string")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$v1")
	// No orphan hash field rows survive: HLEN sees a string as WRONGTYPE, and the keyspace has
	// exactly one key.
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":1")
}

// TestRenameTTLCarries moves the source TTL to the destination and discards the destination's own.
func TestRenameTTLCarries(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Source has a TTL, destination does not: destination inherits it.
	cmd(t, rw, "SET", "a", "v1", "EX", "500")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "b", "v2")
	expect(t, rw, "+OK")
	cmd(t, rw, "RENAME", "a", "b")
	expect(t, rw, "+OK")
	cmd(t, rw, "TTL", "b")
	expect(t, rw, ":500")

	// Source has no TTL, destination does: the destination's TTL is discarded.
	cmd(t, rw, "SET", "c", "v3")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "d", "v4", "EX", "500")
	expect(t, rw, "+OK")
	cmd(t, rw, "RENAME", "c", "d")
	expect(t, rw, "+OK")
	cmd(t, rw, "TTL", "d")
	expect(t, rw, ":-1")
}

// TestRenameNx reports 1 only when the destination is free, and never overwrites an existing one.
func TestRenameNx(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v1")
	expect(t, rw, "+OK")
	// Destination free: rename happens, reports 1.
	cmd(t, rw, "RENAMENX", "a", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$v1")
	cmd(t, rw, "EXISTS", "a")
	expect(t, rw, ":0")

	// Destination taken: no rename, reports 0, both keys keep their values.
	cmd(t, rw, "SET", "c", "v3")
	expect(t, rw, "+OK")
	cmd(t, rw, "RENAMENX", "c", "b")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "c")
	expect(t, rw, "$v3")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$v1")
}

// TestRenameSelf renames a key onto itself: OK for RENAME, 0 for RENAMENX, value untouched.
func TestRenameSelf(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v1", "EX", "500")
	expect(t, rw, "+OK")
	cmd(t, rw, "RENAME", "a", "a")
	expect(t, rw, "+OK")
	cmd(t, rw, "GET", "a")
	expect(t, rw, "$v1")
	cmd(t, rw, "TTL", "a")
	expect(t, rw, ":500")
	cmd(t, rw, "RENAMENX", "a", "a")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "a")
	expect(t, rw, "$v1")
}

// TestRenameHash moves every field row and the header, leaving no orphan rows behind.
func TestRenameHash(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f1", "a", "f2", "b", "f3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "RENAME", "h", "g")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXISTS", "h")
	expect(t, rw, ":0")
	cmd(t, rw, "HLEN", "g")
	expect(t, rw, ":3")
	cmd(t, rw, "HGET", "g", "f2")
	expect(t, rw, "$b")
	assertHashEqual(t, rw, "g", map[string]string{"f1": "a", "f2": "b", "f3": "c"})
}

// TestRenameSet moves every member row and the header.
func TestRenameSet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "x", "y", "z")
	expect(t, rw, ":3")
	cmd(t, rw, "RENAME", "s", "t")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXISTS", "s")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "t")
	expect(t, rw, ":3")
	cmd(t, rw, "SMEMBERS", "t")
	got := readArray(t, rw)
	if !sortedEqual(got, []string{"x", "y", "z"}) {
		t.Fatalf("SMEMBERS = %v", got)
	}
}

// TestRenameZset moves both the member and score families so ranks and scores survive.
func TestRenameZset(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "RENAME", "z", "w")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXISTS", "z")
	expect(t, rw, ":0")
	cmd(t, rw, "ZCARD", "w")
	expect(t, rw, ":3")
	cmd(t, rw, "ZSCORE", "w", "b")
	expect(t, rw, "$2")
	// Score-family order survives: ZRANGE by rank returns the members in score order.
	cmd(t, rw, "ZRANGE", "w", "0", "-1")
	got := readArray(t, rw)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("ZRANGE = %v", got)
	}
	cmd(t, rw, "ZRANK", "w", "c")
	expect(t, rw, ":2")
}

// TestRenameList moves the element window and header, preserving order.
func TestRenameList(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "a", "b", "c", "d")
	expect(t, rw, ":4")
	cmd(t, rw, "RENAME", "l", "m")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXISTS", "l")
	expect(t, rw, ":0")
	cmd(t, rw, "LLEN", "m")
	expect(t, rw, ":4")
	cmd(t, rw, "LRANGE", "m", "0", "-1")
	got := readArray(t, rw)
	if len(got) != 4 || got[0] != "a" || got[3] != "d" {
		t.Fatalf("LRANGE = %v", got)
	}
	// The moved list still pushes and pops correctly under the new name (window intact).
	cmd(t, rw, "LPOP", "m")
	expect(t, rw, "$a")
	cmd(t, rw, "RPOP", "m")
	expect(t, rw, "$d")
	cmd(t, rw, "LRANGE", "m", "0", "-1")
	got = readArray(t, rw)
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("LRANGE after pops = %v", got)
	}
}

// TestRenameStream moves the entry rows plus the group, consumer, and PEL families, so a consumer
// group keeps its pending entries under the new name.
func TestRenameStream(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "st", "1-1", "f", "v1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "st", "2-1", "f", "v2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XGROUP", "CREATE", "st", "g1", "0")
	expect(t, rw, "+OK")
	// Read both entries into the group so a PEL and a consumer exist.
	cmd(t, rw, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "10", "STREAMS", "st", ">")
	drainReply(t, rw)

	cmd(t, rw, "RENAME", "st", "su")
	expect(t, rw, "+OK")
	cmd(t, rw, "EXISTS", "st")
	expect(t, rw, ":0")
	cmd(t, rw, "XLEN", "su")
	expect(t, rw, ":2")
	// The group and its two pending entries survived the move. XPENDING's summary form is a
	// four-element array whose first element is the pending count as an integer, so it is read
	// element by element rather than with the bulk-only readArray helper.
	cmd(t, rw, "XPENDING", "su", "g1")
	if h := readReply(t, rw); h != "*4" {
		t.Fatalf("XPENDING summary header = %q, want *4", h)
	}
	if got := readReply(t, rw); got != ":2" {
		t.Fatalf("XPENDING summary count = %q, want :2", got)
	}
	drainReply(t, rw) // min-id bulk
	drainReply(t, rw) // max-id bulk
	drainReply(t, rw) // per-consumer array
	// XACK drops one pending entry, proving the PEL rows are addressable under the new name.
	cmd(t, rw, "XACK", "su", "g1", "1-1")
	expect(t, rw, ":1")
}

// drainReply reads one complete RESP reply, recursing into arrays so a nested reply (such as
// XREADGROUP's stream-of-entries) is fully consumed and the stream stays aligned for the next
// command. readReply consumes a bulk body itself and returns only the header for an array, so
// only arrays need the recursion.
func drainReply(t *testing.T, rw *bufio.ReadWriter) {
	t.Helper()
	line := readReply(t, rw)
	if len(line) == 0 || line[0] != '*' {
		return
	}
	n := 0
	neg := false
	for _, ch := range line[1:] {
		if ch == '-' {
			neg = true
			continue
		}
		n = n*10 + int(ch-'0')
	}
	if neg {
		return // a null array has no elements
	}
	for i := 0; i < n; i++ {
		drainReply(t, rw)
	}
}

// assertHashEqual checks HGETALL of key matches want exactly.
func assertHashEqual(t *testing.T, rw *bufio.ReadWriter, key string, want map[string]string) {
	t.Helper()
	cmd(t, rw, "HGETALL", key)
	flat := readArray(t, rw)
	if len(flat) != len(want)*2 {
		t.Fatalf("HGETALL %s len = %d, want %d", key, len(flat), len(want)*2)
	}
	got := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		got[flat[i]] = flat[i+1]
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("HGETALL %s[%q] = %q, want %q", key, k, got[k], v)
		}
	}
}
