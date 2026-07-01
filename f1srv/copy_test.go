package f1srv

import "testing"

// TestCopyString copies a plain string and checks the source survives and the value lands intact.
func TestCopyString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "COPY", "a", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "a")
	expect(t, rw, "$hello")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$hello")
}

// TestCopyMissingSource returns 0 when the source is absent, not an error.
func TestCopyMissingSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "COPY", "nope", "dst")
	expect(t, rw, ":0")
}

// TestCopySameObject errors when source and destination name the same key, checked before the
// source is even looked up, and DB 0 or REPLACE do not change that.
func TestCopySameObject(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "COPY", "a", "a")
	expect(t, rw, "-ERR source and destination objects are the same")
	cmd(t, rw, "COPY", "a", "a", "DB", "0")
	expect(t, rw, "-ERR source and destination objects are the same")
	cmd(t, rw, "COPY", "a", "a", "REPLACE")
	expect(t, rw, "-ERR source and destination objects are the same")
	// The same-object check precedes the existence check: a missing key copied onto itself is
	// still the same-object error, not a 0.
	cmd(t, rw, "COPY", "none", "none")
	expect(t, rw, "-ERR source and destination objects are the same")
}

// TestCopyDestExists returns 0 without REPLACE and overwrites with REPLACE.
func TestCopyDestExists(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v1")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "b", "old")
	expect(t, rw, "+OK")
	cmd(t, rw, "COPY", "a", "b")
	expect(t, rw, ":0")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$old")
	cmd(t, rw, "COPY", "a", "b", "REPLACE")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$v1")
}

// TestCopyReplaceDropsHash proves REPLACE drops the destination's old rows, not just its header:
// a hash overwritten by a string leaves no orphan field rows and one key in the space.
func TestCopyReplaceDropsHash(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v1")
	expect(t, rw, "+OK")
	cmd(t, rw, "HSET", "b", "f1", "x", "f2", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "COPY", "a", "b", "REPLACE")
	expect(t, rw, ":1")
	cmd(t, rw, "TYPE", "b")
	expect(t, rw, "+string")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$v1")
	cmd(t, rw, "DBSIZE")
	expect(t, rw, ":2")
}

// TestCopyTTL carries the source TTL to the destination and drops a replaced destination's TTL.
func TestCopyTTL(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v", "EX", "500")
	expect(t, rw, "+OK")
	cmd(t, rw, "COPY", "a", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "b")
	expect(t, rw, ":500")

	// Source without a TTL, replacing a destination that has one: the destination loses its TTL.
	cmd(t, rw, "SET", "c", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SET", "d", "v", "EX", "500")
	expect(t, rw, "+OK")
	cmd(t, rw, "COPY", "c", "d", "REPLACE")
	expect(t, rw, ":1")
	cmd(t, rw, "TTL", "d")
	expect(t, rw, ":-1")
}

// TestCopyDBIndex accepts DB 0 (the only database) and rejects any other index the way Redis
// rejects an out-of-range one, plus the parse errors.
func TestCopyDBIndex(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "a", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "COPY", "a", "b", "DB", "0")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "b")
	expect(t, rw, "$v")
	cmd(t, rw, "COPY", "a", "c", "DB", "-1")
	expect(t, rw, "-ERR DB index is out of range")
	cmd(t, rw, "COPY", "a", "c", "DB", "foo")
	expect(t, rw, "-ERR value is not an integer or out of range")
	cmd(t, rw, "COPY", "a", "c", "DB")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "COPY", "a", "c", "FOO")
	expect(t, rw, "-ERR syntax error")
	cmd(t, rw, "COPY", "a")
	expect(t, rw, "-ERR wrong number of arguments for 'copy' command")
}

// TestCopyHash copies every field row and the header into an independent hash, and mutating the
// copy does not touch the source.
func TestCopyHash(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "HSET", "h", "f1", "a", "f2", "b", "f3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "COPY", "h", "g")
	expect(t, rw, ":1")
	cmd(t, rw, "HLEN", "g")
	expect(t, rw, ":3")
	assertHashEqual(t, rw, "g", map[string]string{"f1": "a", "f2": "b", "f3": "c"})
	// The copy is independent: editing it leaves the source intact.
	cmd(t, rw, "HSET", "g", "f1", "changed")
	expect(t, rw, ":0")
	cmd(t, rw, "HGET", "h", "f1")
	expect(t, rw, "$a")
}

// TestCopySet copies every member row and the header.
func TestCopySet(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "x", "y", "z")
	expect(t, rw, ":3")
	cmd(t, rw, "COPY", "s", "t")
	expect(t, rw, ":1")
	cmd(t, rw, "SCARD", "t")
	expect(t, rw, ":3")
	cmd(t, rw, "SMEMBERS", "t")
	got := readArray(t, rw)
	if !sortedEqual(got, []string{"x", "y", "z"}) {
		t.Fatalf("SMEMBERS = %v", got)
	}
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":3")
}

// TestCopyZset copies both the member and score families so ranks and scores survive.
func TestCopyZset(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "COPY", "z", "w")
	expect(t, rw, ":1")
	cmd(t, rw, "ZCARD", "w")
	expect(t, rw, ":3")
	cmd(t, rw, "ZSCORE", "w", "b")
	expect(t, rw, "$2")
	cmd(t, rw, "ZRANGE", "w", "0", "-1")
	got := readArray(t, rw)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("ZRANGE = %v", got)
	}
	cmd(t, rw, "ZRANK", "w", "c")
	expect(t, rw, ":2")
}

// TestCopyList copies the element window and header, preserving order, and the copy pops
// independently of the source.
func TestCopyList(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "RPUSH", "l", "a", "b", "c", "d")
	expect(t, rw, ":4")
	cmd(t, rw, "COPY", "l", "m")
	expect(t, rw, ":1")
	cmd(t, rw, "LRANGE", "m", "0", "-1")
	got := readArray(t, rw)
	if len(got) != 4 || got[0] != "a" || got[3] != "d" {
		t.Fatalf("LRANGE = %v", got)
	}
	cmd(t, rw, "LPOP", "m")
	expect(t, rw, "$a")
	// The source still has all four.
	cmd(t, rw, "LLEN", "l")
	expect(t, rw, ":4")
}

// TestCopyStream copies the entry rows plus the group, consumer, and PEL families, so a consumer
// group keeps its pending entries in the copy.
func TestCopyStream(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "XADD", "st", "1-1", "f", "v1")
	expect(t, rw, "$1-1")
	cmd(t, rw, "XADD", "st", "2-1", "f", "v2")
	expect(t, rw, "$2-1")
	cmd(t, rw, "XGROUP", "CREATE", "st", "g1", "0")
	expect(t, rw, "+OK")
	cmd(t, rw, "XREADGROUP", "GROUP", "g1", "c1", "COUNT", "10", "STREAMS", "st", ">")
	drainReply(t, rw)

	cmd(t, rw, "COPY", "st", "su")
	expect(t, rw, ":1")
	cmd(t, rw, "XLEN", "su")
	expect(t, rw, ":2")
	// The group and its two pending entries came across.
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
	// Acking in the copy proves the PEL rows are addressable under the new name, and it does not
	// touch the source's own PEL.
	cmd(t, rw, "XACK", "su", "g1", "1-1")
	expect(t, rw, ":1")
	cmd(t, rw, "XPENDING", "st", "g1")
	if h := readReply(t, rw); h != "*4" {
		t.Fatalf("source XPENDING header = %q, want *4", h)
	}
	if got := readReply(t, rw); got != ":2" {
		t.Fatalf("source XPENDING count = %q, want :2 (unchanged)", got)
	}
	drainReply(t, rw)
	drainReply(t, rw)
	drainReply(t, rw)
}
