package f1srv

import (
	"bufio"
	"fmt"
	"testing"
)

// readArray reads an array reply of bulk strings and returns the members. A nil bulk
// inside the array reads back as the empty string, which none of the random-selection
// paths emit, so an empty string in the result flags a bug rather than a valid member.
func readArray(t *testing.T, rw *bufio.ReadWriter) []string {
	t.Helper()
	h := readReply(t, rw)
	if len(h) == 0 || h[0] != '*' {
		t.Fatalf("array header = %q, want an array", h)
	}
	n := 0
	for _, ch := range h[1:] {
		n = n*10 + int(ch-'0')
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		b := readReply(t, rw)
		if len(b) == 0 || b[0] != '$' {
			t.Fatalf("array item %d = %q, want a bulk string", i, b)
		}
		out[i] = b[1:]
	}
	return out
}

// SRANDMEMBER's no-count form returns one member drawn uniformly from the set, and nil
// for a key that does not exist, and it never removes anything.
func TestSRandMemberNoCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// A missing key is a nil bulk, not an error or an empty string.
	cmd(t, rw, "SRANDMEMBER", "nope")
	expect(t, rw, "$-1")

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")

	members := map[string]bool{"a": true, "b": true, "c": true}
	// Many draws must all land inside the set and must not shrink it.
	for i := 0; i < 50; i++ {
		cmd(t, rw, "SRANDMEMBER", "s")
		got := readReply(t, rw)
		if got[0] != '$' || !members[got[1:]] {
			t.Fatalf("SRANDMEMBER returned %q, not a member of the set", got)
		}
	}
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":3")
}

// A positive count returns up to that many distinct members with no duplicates, capped at
// the cardinality, and does not remove anything.
func TestSRandMemberPositiveCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c", "d", "e")
	expect(t, rw, ":5")

	// A count below the cardinality returns exactly that many distinct members.
	cmd(t, rw, "SRANDMEMBER", "s", "3")
	got := readArray(t, rw)
	if len(got) != 3 {
		t.Fatalf("got %d members, want 3", len(got))
	}
	assertDistinctSubset(t, got, []string{"a", "b", "c", "d", "e"})

	// A count at or above the cardinality returns the whole set once, never padded.
	cmd(t, rw, "SRANDMEMBER", "s", "100")
	got = readArray(t, rw)
	if len(got) != 5 {
		t.Fatalf("got %d members, want the whole set of 5", len(got))
	}
	assertDistinctSubset(t, got, []string{"a", "b", "c", "d", "e"})

	// Non-destructive: the set is untouched.
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":5")
}

// A negative count returns exactly abs(count) members with replacement, so the length is
// the requested magnitude even when it exceeds the cardinality and duplicates are allowed.
func TestSRandMemberNegativeCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b")
	expect(t, rw, ":2")

	// abs(count) past the cardinality: length is 6, every element a valid member,
	// duplicates expected.
	cmd(t, rw, "SRANDMEMBER", "s", "-6")
	got := readArray(t, rw)
	if len(got) != 6 {
		t.Fatalf("got %d members, want exactly 6 with replacement", len(got))
	}
	for _, m := range got {
		if m != "a" && m != "b" {
			t.Fatalf("SRANDMEMBER returned %q, not a member", m)
		}
	}
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":2")
}

// A zero count is an empty array, and a count on a missing key is an empty array too, not
// a nil.
func TestSRandMemberCountEdges(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SRANDMEMBER", "s", "0")
	expect(t, rw, "*0")

	cmd(t, rw, "SRANDMEMBER", "missing", "5")
	expect(t, rw, "*0")
	cmd(t, rw, "SRANDMEMBER", "missing", "-5")
	expect(t, rw, "*0")
}

// SRANDMEMBER against a string key is WRONGTYPE in both the no-count and the count form.
func TestSRandMemberWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SRANDMEMBER", "k")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "SRANDMEMBER", "k", "3")
	expect(t, rw, "-"+wrongType)
}

// The distinct-sample path crosses over to a whole-set walk plus a partial shuffle once
// the requested count is a big fraction of the cardinality. A count just over half a
// larger set exercises that crossover and must still return distinct members.
func TestSRandMemberCrossover(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 100
	all := make([]string, n)
	args := []string{"SADD", "s"}
	for i := 0; i < n; i++ {
		all[i] = fmt.Sprintf("m%03d", i)
		args = append(args, all[i])
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	// 60 of 100 is above the half-cardinality crossover into the walk-and-shuffle path.
	cmd(t, rw, "SRANDMEMBER", "s", "60")
	got := readArray(t, rw)
	if len(got) != 60 {
		t.Fatalf("got %d members, want 60", len(got))
	}
	assertDistinctSubset(t, got, all)
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, fmt.Sprintf(":%d", n))
}

// SPOP's no-count form returns one member as a bulk string and removes it, and a missing
// key is a nil bulk.
func TestSPopNoCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SPOP", "nope")
	expect(t, rw, "$-1")

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")

	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		cmd(t, rw, "SPOP", "s")
		got := readReply(t, rw)
		if got[0] != '$' {
			t.Fatalf("SPOP returned %q, want a bulk member", got)
		}
		m := got[1:]
		if seen[m] {
			t.Fatalf("SPOP returned %q twice", m)
		}
		seen[m] = true
	}
	// Every member popped exactly once, and the set is gone.
	if len(seen) != 3 {
		t.Fatalf("popped %d distinct members, want 3", len(seen))
	}
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":0")
	cmd(t, rw, "SPOP", "s")
	expect(t, rw, "$-1")
}

// SPOP's count form returns an array of distinct members and removes exactly them, leaving
// the rest of the set intact.
func TestSPopCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c", "d", "e")
	expect(t, rw, ":5")

	cmd(t, rw, "SPOP", "s", "2")
	got := readArray(t, rw)
	if len(got) != 2 {
		t.Fatalf("popped %d members, want 2", len(got))
	}
	assertDistinctSubset(t, got, []string{"a", "b", "c", "d", "e"})

	// The two popped members are gone, three remain.
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":3")
	for _, m := range got {
		cmd(t, rw, "SISMEMBER", "s", m)
		expect(t, rw, ":0")
	}
}

// A count at or past the cardinality pops the whole set and deletes it, and a zero count
// is an empty array that removes nothing.
func TestSPopCountWholeSetAndZero(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")

	// Zero count removes nothing.
	cmd(t, rw, "SPOP", "s", "0")
	expect(t, rw, "*0")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":3")

	// A count over the cardinality returns the whole set and drops the key.
	cmd(t, rw, "SPOP", "s", "100")
	got := readArray(t, rw)
	if len(got) != 3 {
		t.Fatalf("popped %d members, want the whole set of 3", len(got))
	}
	assertDistinctSubset(t, got, []string{"a", "b", "c"})
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":0")

	// A count pop on a missing key is an empty array, not a nil.
	cmd(t, rw, "SPOP", "missing", "3")
	expect(t, rw, "*0")
}

// SPOP rejects a negative count, unlike SRANDMEMBER which reads it as with-replacement.
func TestSPopNegativeCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SPOP", "s", "-1")
	expect(t, rw, "-ERR value is out of range, must be positive")
	// The set is untouched by the rejected pop.
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":2")
}

// SPOP against a string key is WRONGTYPE in both forms.
func TestSPopWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "k", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SPOP", "k")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "SPOP", "k", "2")
	expect(t, rw, "-"+wrongType)
	cmd(t, rw, "GET", "k")
	expect(t, rw, "$v")
}

// A large-count SPOP crosses the same half-cardinality boundary the sampler does, and it
// must remove exactly the distinct members it returns, updating the header count to match.
func TestSPopCrossover(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	const n = 100
	all := make([]string, n)
	args := []string{"SADD", "s"}
	for i := 0; i < n; i++ {
		all[i] = fmt.Sprintf("m%03d", i)
		args = append(args, all[i])
	}
	cmd(t, rw, args...)
	expect(t, rw, fmt.Sprintf(":%d", n))

	cmd(t, rw, "SPOP", "s", "70")
	got := readArray(t, rw)
	if len(got) != 70 {
		t.Fatalf("popped %d members, want 70", len(got))
	}
	assertDistinctSubset(t, got, all)
	// Exactly the popped members are gone, and the header count reflects the remainder.
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, fmt.Sprintf(":%d", n-70))
	for _, m := range got {
		cmd(t, rw, "SISMEMBER", "s", m)
		expect(t, rw, ":0")
	}
}

// assertDistinctSubset fails if got has a duplicate or a member outside the universe, the
// property both the distinct-sample paths must hold.
func assertDistinctSubset(t *testing.T, got, universe []string) {
	t.Helper()
	valid := make(map[string]bool, len(universe))
	for _, u := range universe {
		valid[u] = true
	}
	seen := make(map[string]bool, len(got))
	for _, m := range got {
		if !valid[m] {
			t.Fatalf("member %q is not in the set", m)
		}
		if seen[m] {
			t.Fatalf("member %q returned more than once", m)
		}
		seen[m] = true
	}
}
