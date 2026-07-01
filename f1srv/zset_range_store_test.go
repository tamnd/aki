package f1srv

import "testing"

// ZRANGESTORE stores the rank-indexed window of the source into the destination and replies with the
// stored cardinality. The destination reads back in score order with the source scores.
func TestZRangeStoreByIndex(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, rw, ":4")

	// Store the middle two by rank.
	cmd(t, rw, "ZRANGESTORE", "dst", "src", "1", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "2", "c", "3"}
	assertArray(t, "byindex", got, want)
}

// The BYSCORE form stores members whose scores fall in the range.
func TestZRangeStoreByScore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, rw, ":4")

	cmd(t, rw, "ZRANGESTORE", "dst", "src", "2", "3", "BYSCORE")
	expect(t, rw, ":2")
	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "2", "c", "3"}
	assertArray(t, "byscore", got, want)
}

// The BYLEX form stores members in a lexical range, reading each member's score from the source.
func TestZRangeStoreByLex(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// Equal scores so lexical order is well defined.
	cmd(t, rw, "ZADD", "src", "0", "a", "0", "b", "0", "c", "0", "d")
	expect(t, rw, ":4")

	cmd(t, rw, "ZRANGESTORE", "dst", "src", "[b", "[c", "BYLEX")
	expect(t, rw, ":2")
	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "0", "c", "0"}
	assertArray(t, "bylex", got, want)
}

// REV and LIMIT carry through the BYSCORE form.
func TestZRangeStoreRevLimit(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "src", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, rw, ":4")

	// Reverse over the whole score range, skip one from the top, keep two: c(3), b(2).
	cmd(t, rw, "ZRANGESTORE", "dst", "src", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "1", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "2", "c", "3"}
	assertArray(t, "revlimit", got, want)
}

// An empty window deletes the destination and replies with 0, replacing whatever was there.
func TestZRangeStoreEmptyDeletes(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "src", "1", "a", "2", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "ZADD", "dst", "9", "stale")
	expect(t, rw, ":1")

	// A range that matches nothing.
	cmd(t, rw, "ZRANGESTORE", "dst", "src", "100", "200", "BYSCORE")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "dst")
	expect(t, rw, ":0")
}

// A string destination is dropped, not a WRONGTYPE, and the source is stored over it.
func TestZRangeStoreReplacesString(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "src", "1", "a", "2", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SET", "dst", "hello")
	expect(t, rw, "+OK")

	cmd(t, rw, "ZRANGESTORE", "dst", "src", "0", "-1")
	expect(t, rw, ":2")
	cmd(t, rw, "TYPE", "dst")
	expect(t, rw, "+zset")
}

// The destination aliasing the source keeps only the windowed members, filtering in place.
func TestZRangeStoreAliasedDest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "k", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, rw, ":4")

	// Keep ranks [1,2] of k into k itself: b, c survive.
	cmd(t, rw, "ZRANGESTORE", "k", "k", "1", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "ZRANGE", "k", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "2", "c", "3"}
	assertArray(t, "aliased", got, want)
}

// A source held by a plain string is a WRONGTYPE.
func TestZRangeStoreSourceWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "src", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "ZRANGESTORE", "dst", "src", "0", "-1")
	expect(t, rw, "-"+wrongType)
}

// assertArray fails the test if got does not match want element for element.
func assertArray(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %q, want %q", label, i, got[i], want[i])
		}
	}
}
