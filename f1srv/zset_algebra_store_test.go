package f1srv

import "testing"

// ZUNIONSTORE writes the scored union into the destination and replies with its cardinality. The
// stored zset reads back in score order with the aggregated scores.
func TestZUnionStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z2", "10", "b", "20", "d")
	expect(t, rw, ":2")

	// a=1, c=3, b=12, d=20 -> 4 members.
	cmd(t, rw, "ZUNIONSTORE", "dst", "2", "z1", "z2")
	expect(t, rw, ":4")

	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"a", "1", "c", "3", "b", "12", "d", "20"}
	if len(got) != len(want) {
		t.Fatalf("stored union = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stored union[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// WEIGHTS and AGGREGATE carry through the STORE form.
func TestZInterStoreWeightsAggregate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z2", "10", "b", "20", "c", "30", "d")
	expect(t, rw, ":3")

	// Shared b, c. AGGREGATE MAX with WEIGHTS 1 2: b=max(2,20)=20, c=max(3,40)=40.
	cmd(t, rw, "ZINTERSTORE", "dst", "2", "z1", "z2", "WEIGHTS", "1", "2", "AGGREGATE", "MAX")
	expect(t, rw, ":2")

	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "20", "c", "40"}
	if len(got) != len(want) {
		t.Fatalf("stored inter = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stored inter[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// ZDIFFSTORE writes the first-minus-rest difference and replies with its cardinality.
func TestZDiffStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z2", "10", "b")
	expect(t, rw, ":1")

	cmd(t, rw, "ZDIFFSTORE", "dst", "2", "z1", "z2")
	expect(t, rw, ":2")

	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"a", "1", "c", "3"}
	if len(got) != len(want) {
		t.Fatalf("stored diff = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stored diff[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// An empty result deletes the destination, and STORE overwrites whatever the destination held,
// including a plain string.
func TestZStoreReplaceAndEmpty(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "ZADD", "z2", "1", "b")
	expect(t, rw, ":1")

	// A non-empty destination is fully replaced.
	cmd(t, rw, "ZADD", "dst", "99", "stale", "98", "old")
	expect(t, rw, ":2")
	cmd(t, rw, "ZUNIONSTORE", "dst", "2", "z1", "z2")
	expect(t, rw, ":2")
	cmd(t, rw, "ZSCORE", "dst", "stale")
	expect(t, rw, "$-1")
	cmd(t, rw, "ZCARD", "dst")
	expect(t, rw, ":2")

	// A string destination is dropped, not a WRONGTYPE.
	cmd(t, rw, "SET", "strdst", "hello")
	expect(t, rw, "+OK")
	cmd(t, rw, "ZUNIONSTORE", "strdst", "2", "z1", "z2")
	expect(t, rw, ":2")
	cmd(t, rw, "TYPE", "strdst")
	expect(t, rw, "+zset")

	// An empty intersection deletes the destination.
	cmd(t, rw, "ZINTERSTORE", "dst", "2", "z1", "z2")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "dst")
	expect(t, rw, ":0")
}

// STORE handles the destination aliasing a source: the merge is computed before the destination is
// cleared, so aliasing does not corrupt the result.
func TestZStoreAliasedDest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "ZADD", "other", "10", "b", "20", "c")
	expect(t, rw, ":2")

	// ZUNIONSTORE z z other: z becomes a=1, b=2+10=12, c=20.
	cmd(t, rw, "ZUNIONSTORE", "z", "2", "z", "other")
	expect(t, rw, ":3")
	cmd(t, rw, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"a", "1", "b", "12", "c", "20"}
	if len(got) != len(want) {
		t.Fatalf("aliased store = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("aliased store[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// A set source stores with the implicit score of 1.
func TestZStoreSetSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "5", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "s1", "a", "b")
	expect(t, rw, ":2")

	// Union: a=5+1=6, b=1. Order: b(1) a(6).
	cmd(t, rw, "ZUNIONSTORE", "dst", "2", "z1", "s1")
	expect(t, rw, ":2")
	cmd(t, rw, "ZRANGE", "dst", "0", "-1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "1", "a", "6"}
	if len(got) != len(want) {
		t.Fatalf("set-source store = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("set-source store[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// STORE argument guards: bad numkeys, and ZDIFFSTORE takes no WEIGHTS.
func TestZStoreErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZUNIONSTORE", "dst", "0", "z1")
	expect(t, rw, "-ERR at least 1 input key is needed for 'zunionstore' command")

	cmd(t, rw, "ZINTERSTORE", "dst", "x", "z1")
	expect(t, rw, "-ERR value is not an integer or out of range")

	cmd(t, rw, "ZDIFFSTORE", "dst", "2", "z1", "z2", "WEIGHTS", "1", "1")
	expect(t, rw, "-ERR syntax error")
}
