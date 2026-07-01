package f1srv

import "testing"

// ZUNION sums scores of shared members and returns every member in score order. With WITHSCORES the
// array interleaves member and score.
func TestZUnion(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z2", "10", "b", "20", "d")
	expect(t, rw, ":2")

	// a=1, c=3, b=2+10=12, d=20, sorted by score: a(1) c(3) b(12) d(20).
	cmd(t, rw, "ZUNION", "2", "z1", "z2", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"a", "1", "c", "3", "b", "12", "d", "20"}
	if len(got) != len(want) {
		t.Fatalf("ZUNION returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZUNION[%d] = %q, want %q (full %v)", i, got[i], want[i], got)
		}
	}

	// Without WITHSCORES it is just the members in score order.
	cmd(t, rw, "ZUNION", "2", "z1", "z2")
	got = readArray(t, rw)
	wantM := []string{"a", "c", "b", "d"}
	if len(got) != len(wantM) {
		t.Fatalf("ZUNION members = %v, want %v", got, wantM)
	}
	for i := range wantM {
		if got[i] != wantM[i] {
			t.Fatalf("ZUNION member[%d] = %q, want %q", i, got[i], wantM[i])
		}
	}
}

// WEIGHTS scale each source before the sum and AGGREGATE picks how shared scores combine.
func TestZUnionWeightsAggregate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "ZADD", "z2", "3", "b", "4", "c")
	expect(t, rw, ":2")

	// WEIGHTS 2 3: a=1*2=2, b=2*2 + 3*3 = 13, c=4*3=12. Order: a(2) c(12) b(13).
	cmd(t, rw, "ZUNION", "2", "z1", "z2", "WEIGHTS", "2", "3", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"a", "2", "c", "12", "b", "13"}
	if len(got) != len(want) {
		t.Fatalf("weighted ZUNION = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("weighted ZUNION[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// AGGREGATE MAX keeps the larger of the two scores for b.
	cmd(t, rw, "ZUNION", "2", "z1", "z2", "AGGREGATE", "MAX", "WITHSCORES")
	got = readArray(t, rw)
	// a=1, b=max(2,3)=3, c=4. Order: a(1) b(3) c(4).
	want = []string{"a", "1", "b", "3", "c", "4"}
	if len(got) != len(want) {
		t.Fatalf("MAX ZUNION = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("MAX ZUNION[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// ZINTER keeps only members present in every source and aggregates their scores.
func TestZInter(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z2", "10", "b", "20", "c", "30", "d")
	expect(t, rw, ":3")

	// Shared: b=2+10=12, c=3+20=23. Order: b(12) c(23).
	cmd(t, rw, "ZINTER", "2", "z1", "z2", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"b", "12", "c", "23"}
	if len(got) != len(want) {
		t.Fatalf("ZINTER = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZINTER[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// A missing source empties the intersection.
	cmd(t, rw, "ZINTER", "2", "z1", "nope")
	expect(t, rw, "*0")
}

// ZDIFF returns members of the first set that no other set holds, keeping the first set's scores.
func TestZDiff(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "ZADD", "z2", "10", "b")
	expect(t, rw, ":1")

	// z1 minus z2: a(1), c(3). b is removed. Order: a(1) c(3).
	cmd(t, rw, "ZDIFF", "2", "z1", "z2", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"a", "1", "c", "3"}
	if len(got) != len(want) {
		t.Fatalf("ZDIFF = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ZDIFF[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// ZDIFF takes no WEIGHTS.
	cmd(t, rw, "ZDIFF", "2", "z1", "z2", "WEIGHTS", "1", "1")
	expect(t, rw, "-ERR syntax error")
}

// A plain set participates in the algebra with an implicit score of 1 per member.
func TestZAlgebraSetSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "5", "a", "6", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "s1", "b", "c")
	expect(t, rw, ":2")

	// Union: a=5, b=6+1=7, c=1. Order: c(1) a(5) b(7).
	cmd(t, rw, "ZUNION", "2", "z1", "s1", "WITHSCORES")
	got := readArray(t, rw)
	want := []string{"c", "1", "a", "5", "b", "7"}
	if len(got) != len(want) {
		t.Fatalf("set-source ZUNION = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("set-source ZUNION[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Intersection with the set: only b is shared, score 6+1=7.
	cmd(t, rw, "ZINTER", "2", "z1", "s1", "WITHSCORES")
	got = readArray(t, rw)
	want = []string{"b", "7"}
	if len(got) != len(want) {
		t.Fatalf("set-source ZINTER = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("set-source ZINTER[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// ZINTERCARD counts the intersection and honors a positive LIMIT as an early stop.
func TestZInterCard(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z1", "1", "a", "2", "b", "3", "c", "4", "d")
	expect(t, rw, ":4")
	cmd(t, rw, "ZADD", "z2", "1", "b", "1", "c", "1", "d", "1", "e")
	expect(t, rw, ":4")

	// Shared: b, c, d -> 3.
	cmd(t, rw, "ZINTERCARD", "2", "z1", "z2")
	expect(t, rw, ":3")

	// LIMIT 2 stops the count early.
	cmd(t, rw, "ZINTERCARD", "2", "z1", "z2", "LIMIT", "2")
	expect(t, rw, ":2")

	// LIMIT 0 means no limit.
	cmd(t, rw, "ZINTERCARD", "2", "z1", "z2", "LIMIT", "0")
	expect(t, rw, ":3")

	// A missing source gives an empty intersection.
	cmd(t, rw, "ZINTERCARD", "2", "z1", "nope")
	expect(t, rw, ":0")
}

// The algebra guards: bad numkeys, too few keys, a string source, and unknown options.
func TestZAlgebraErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZUNION", "0", "z1")
	expect(t, rw, "-ERR at least 1 input key is needed for 'zunion' command")

	cmd(t, rw, "ZINTER", "x", "z1")
	expect(t, rw, "-ERR value is not an integer or out of range")

	// numkeys promises more keys than are present.
	cmd(t, rw, "ZUNION", "3", "z1")
	expect(t, rw, "-ERR syntax error")

	// A string source is a type error.
	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "ZADD", "z1", "1", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "ZUNION", "2", "z1", "str")
	expect(t, rw, "-WRONGTYPE Operation against a key holding the wrong kind of value")

	// An unknown trailing token is a syntax error.
	cmd(t, rw, "ZUNION", "1", "z1", "NONSENSE")
	expect(t, rw, "-ERR syntax error")

	cmd(t, rw, "ZINTERCARD", "2", "z1", "z2", "LIMIT", "-1")
	expect(t, rw, "-ERR LIMIT can't be negative")
}
