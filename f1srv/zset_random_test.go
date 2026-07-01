package f1srv

import "testing"

// ZRANDMEMBER's no-count form returns one member drawn from the zset, and nil for a missing key,
// and never removes anything. Members "a".."e" carry scores 1..5.
func TestZRandMemberNoCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	// A missing key is a nil bulk, not an error or an empty string.
	cmd(t, rw, "ZRANDMEMBER", "nope")
	expect(t, rw, "$-1")

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, rw, ":5")

	members := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	for i := 0; i < 50; i++ {
		cmd(t, rw, "ZRANDMEMBER", "z")
		got := readReply(t, rw)
		if len(got) == 0 || got[0] != '$' {
			t.Fatalf("no-count reply = %q, want a bulk string", got)
		}
		if !members[got[1:]] {
			t.Fatalf("drew %q, not a member of the zset", got[1:])
		}
	}
	cmd(t, rw, "ZCARD", "z")
	expect(t, rw, ":5")
}

// A positive count returns distinct members, capped at the cardinality, and never repeats one.
func TestZRandMemberPositiveCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, rw, ":5")

	members := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}

	// A count below the cardinality returns exactly that many distinct members.
	for trial := 0; trial < 20; trial++ {
		cmd(t, rw, "ZRANDMEMBER", "z", "3")
		got := readArray(t, rw)
		if len(got) != 3 {
			t.Fatalf("count 3 returned %d members, want 3", len(got))
		}
		seen := map[string]bool{}
		for _, m := range got {
			if !members[m] {
				t.Fatalf("returned %q, not a member", m)
			}
			if seen[m] {
				t.Fatalf("returned %q twice, positive count must be distinct", m)
			}
			seen[m] = true
		}
	}

	// A count at or above the cardinality caps at the whole set, still distinct.
	cmd(t, rw, "ZRANDMEMBER", "z", "10")
	got := readArray(t, rw)
	if len(got) != 5 {
		t.Fatalf("count 10 over a 5-member set returned %d, want 5", len(got))
	}
	seen := map[string]bool{}
	for _, m := range got {
		if seen[m] {
			t.Fatalf("returned %q twice", m)
		}
		seen[m] = true
	}

	// A zero count is an empty array; a missing key is an empty array.
	cmd(t, rw, "ZRANDMEMBER", "z", "0")
	expect(t, rw, "*0")
	cmd(t, rw, "ZRANDMEMBER", "nope", "3")
	expect(t, rw, "*0")
}

// A negative count returns exactly abs(count) members with replacement, so it can exceed the
// cardinality and may repeat members.
func TestZRandMemberNegativeCount(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")

	members := map[string]bool{"a": true, "b": true, "c": true}
	cmd(t, rw, "ZRANDMEMBER", "z", "-8")
	got := readArray(t, rw)
	if len(got) != 8 {
		t.Fatalf("count -8 returned %d, want 8 (with replacement)", len(got))
	}
	for _, m := range got {
		if !members[m] {
			t.Fatalf("returned %q, not a member", m)
		}
	}
}

// WITHSCORES interleaves each drawn member with its score, so the array is member, score pairs and
// each member's score matches what ZADD stored.
func TestZRandMemberWithScores(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	expect(t, rw, ":5")

	want := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"}

	// Positive count with scores: distinct members, each followed by its score.
	cmd(t, rw, "ZRANDMEMBER", "z", "3", "WITHSCORES")
	got := readArray(t, rw)
	if len(got) != 6 {
		t.Fatalf("count 3 WITHSCORES returned %d entries, want 6", len(got))
	}
	seen := map[string]bool{}
	for i := 0; i < len(got); i += 2 {
		m, s := got[i], got[i+1]
		if want[m] != s {
			t.Fatalf("member %q paired with score %q, want %q", m, s, want[m])
		}
		if seen[m] {
			t.Fatalf("member %q repeated under a positive count", m)
		}
		seen[m] = true
	}

	// Negative count with scores keeps the pairing under replacement.
	cmd(t, rw, "ZRANDMEMBER", "z", "-4", "WITHSCORES")
	got = readArray(t, rw)
	if len(got) != 8 {
		t.Fatalf("count -4 WITHSCORES returned %d entries, want 8", len(got))
	}
	for i := 0; i < len(got); i += 2 {
		if want[got[i]] != got[i+1] {
			t.Fatalf("member %q paired with score %q, want %q", got[i], got[i+1], want[got[i]])
		}
	}

	// A bad option token is a syntax error.
	cmd(t, rw, "ZRANDMEMBER", "z", "3", "WITHVALUES")
	expect(t, rw, "-ERR syntax error")
}
