package sqlo1

import (
	"fmt"
	"testing"
)

func TestServerSetSurface(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Point writes: variadic SADD counts created members only.
	send("SADD", "s", "a", "b", "c")
	expect(t, r, ":3\r\n")
	send("SADD", "s", "a", "d")
	expect(t, r, ":1\r\n")
	send("sadd", "s")
	expect(t, r, "-ERR wrong number of arguments for 'sadd' command\r\n")
	send("SCARD", "s")
	expect(t, r, ":4\r\n")
	send("SCARD", "ghost")
	expect(t, r, ":0\r\n")
	send("SCARD", "s", "stray")
	expect(t, r, "-ERR wrong number of arguments for 'scard' command\r\n")

	// Membership probes.
	send("SISMEMBER", "s", "a")
	expect(t, r, ":1\r\n")
	send("SISMEMBER", "s", "zz")
	expect(t, r, ":0\r\n")
	send("SISMEMBER", "ghost", "a")
	expect(t, r, ":0\r\n")
	send("SISMEMBER", "s")
	expect(t, r, "-ERR wrong number of arguments for 'sismember' command\r\n")
	send("SMISMEMBER", "s", "a", "zz", "d")
	expect(t, r, "*3\r\n:1\r\n:0\r\n:1\r\n")
	send("SMISMEMBER", "ghost", "x", "y")
	expect(t, r, "*2\r\n:0\r\n:0\r\n")
	send("SMISMEMBER", "s")
	expect(t, r, "-ERR wrong number of arguments for 'smismember' command\r\n")

	// Removal counts hits only.
	send("SREM", "s", "d", "zz")
	expect(t, r, ":1\r\n")
	send("SREM", "s")
	expect(t, r, "-ERR wrong number of arguments for 'srem' command\r\n")

	// SMOVE relocates a held member and answers 0 for an absent one.
	send("SMOVE", "s", "moved", "a")
	expect(t, r, ":1\r\n")
	send("SISMEMBER", "moved", "a")
	expect(t, r, ":1\r\n")
	send("SISMEMBER", "s", "a")
	expect(t, r, ":0\r\n")
	send("SMOVE", "s", "moved", "nope")
	expect(t, r, ":0\r\n")
	send("SMOVE", "s", "moved")
	expect(t, r, "-ERR wrong number of arguments for 'smove' command\r\n")

	// TYPE and OBJECT ENCODING route through the root sniff.
	send("TYPE", "s")
	expect(t, r, "+set\r\n")
	send("OBJECT", "ENCODING", "s")
	expect(t, r, "$8\r\nlistpack\r\n")
	send("SADD", "ints", "1", "2", "3")
	expect(t, r, ":3\r\n")
	send("OBJECT", "ENCODING", "ints")
	expect(t, r, "$6\r\nintset\r\n")

	// Past the inline count threshold the encoding flips to hashtable
	// and the point surface keeps answering over segments.
	args := []string{"SADD", "big"}
	for i := range 140 {
		args = append(args, fmt.Sprintf("w%03d", i))
	}
	send(args...)
	expect(t, r, ":140\r\n")
	send("OBJECT", "ENCODING", "big")
	expect(t, r, "$9\r\nhashtable\r\n")
	send("TYPE", "big")
	expect(t, r, "+set\r\n")
	send("SISMEMBER", "big", "w077")
	expect(t, r, ":1\r\n")
	send("SMOVE", "big", "s", "w007")
	expect(t, r, ":1\r\n")
	send("SCARD", "big")
	expect(t, r, ":139\r\n")

	// Cross-type doors, both directions.
	wrong := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("SADD", "str", "x")
	expect(t, r, wrong)
	send("SCARD", "str")
	expect(t, r, wrong)
	send("SMOVE", "s", "str", "b")
	expect(t, r, wrong)
	send("SMOVE", "str", "s", "x")
	expect(t, r, wrong)
	send("GET", "s")
	expect(t, r, wrong)
}

func TestServerSetIteration(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	send("SADD", "s", "cherry", "apple", "banana")
	expect(t, r, ":3\r\n")

	// SMEMBERS streams an inline set in insertion order.
	send("SMEMBERS", "s")
	expect(t, r, "*3\r\n$6\r\ncherry\r\n$5\r\napple\r\n$6\r\nbanana\r\n")
	send("SMEMBERS", "ghost")
	expect(t, r, "*0\r\n")
	send("SMEMBERS", "s", "stray")
	expect(t, r, "-ERR wrong number of arguments for 'smembers' command\r\n")

	// SSCAN on an inline set answers any cursor with everything and a
	// zero next cursor, the listpack behavior.
	send("SSCAN", "s", "0")
	expect(t, r, "*2\r\n$1\r\n0\r\n*3\r\n$6\r\ncherry\r\n$5\r\napple\r\n$6\r\nbanana\r\n")
	send("SSCAN", "s", "99999")
	expect(t, r, "*2\r\n$1\r\n0\r\n*3\r\n$6\r\ncherry\r\n$5\r\napple\r\n$6\r\nbanana\r\n")
	send("SSCAN", "s", "0", "MATCH", "*an*")
	expect(t, r, "*2\r\n$1\r\n0\r\n*1\r\n$6\r\nbanana\r\n")
	send("SSCAN", "s", "0", "COUNT", "100")
	expect(t, r, "*2\r\n$1\r\n0\r\n*3\r\n$6\r\ncherry\r\n$5\r\napple\r\n$6\r\nbanana\r\n")
	send("SSCAN", "ghost", "0")
	expect(t, r, "*2\r\n$1\r\n0\r\n*0\r\n")

	// Option grammar: bad cursor, bad count, unknown token.
	send("SSCAN", "s", "notacursor")
	expect(t, r, "-ERR invalid cursor\r\n")
	send("SSCAN", "s", "0", "COUNT", "0")
	expect(t, r, "-ERR syntax error\r\n")
	send("SSCAN", "s", "0", "NOVALUES")
	expect(t, r, "-ERR syntax error\r\n")
	send("SSCAN", "s")
	expect(t, r, "-ERR wrong number of arguments for 'sscan' command\r\n")

	// Cross-type doors.
	wrong := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("SMEMBERS", "str")
	expect(t, r, wrong)
	send("SSCAN", "str", "0")
	expect(t, r, wrong)
}

func TestServerSetSampling(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}

	// Absent keys: null bulk without a count, empty arrays with one.
	send("SRANDMEMBER")
	expect(t, r, "-ERR wrong number of arguments for 'srandmember' command\r\n")
	send("SRANDMEMBER", "ghost")
	expect(t, r, "$-1\r\n")
	send("SRANDMEMBER", "ghost", "5")
	expect(t, r, "*0\r\n")
	send("SRANDMEMBER", "ghost", "-5")
	expect(t, r, "*0\r\n")
	send("SPOP")
	expect(t, r, "-ERR wrong number of arguments for 'spop' command\r\n")
	send("SPOP", "ghost")
	expect(t, r, "$-1\r\n")
	send("SPOP", "ghost", "3")
	expect(t, r, "*0\r\n")

	// The count grammar, Redis's doors and texts.
	send("SRANDMEMBER", "ghost", "abc")
	expect(t, r, "-ERR value is not an integer or out of range\r\n")
	send("SRANDMEMBER", "ghost", "-9223372036854775808")
	expect(t, r, "-ERR value is out of range\r\n")
	send("SRANDMEMBER", "ghost", "3", "extra")
	expect(t, r, "-ERR syntax error\r\n")
	send("SPOP", "ghost", "-1")
	expect(t, r, "-ERR value is out of range, must be positive\r\n")
	send("SPOP", "ghost", "abc")
	expect(t, r, "-ERR value is out of range, must be positive\r\n")
	send("SPOP", "ghost", "3", "extra")
	expect(t, r, "-ERR syntax error\r\n")

	// Cross-type doors, both forms.
	wrong := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")
	send("SRANDMEMBER", "str")
	expect(t, r, wrong)
	send("SRANDMEMBER", "str", "3")
	expect(t, r, wrong)
	send("SPOP", "str")
	expect(t, r, wrong)
	send("SPOP", "str", "3")
	expect(t, r, wrong)

	// Draws off a live set, checked by membership since the picks are
	// random.
	universe := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	send("SADD", "s", "a", "b", "c", "d", "e")
	expect(t, r, ":5\r\n")
	send("SRANDMEMBER", "s")
	if m := readBulk(t, r); !universe[m] {
		t.Fatalf("SRANDMEMBER drew %q", m)
	}
	send("SRANDMEMBER", "s", "3")
	if n := readArrayLen(t, r); n != 3 {
		t.Fatalf("count 3 answered %d members", n)
	}
	seen := map[string]bool{}
	for range 3 {
		m := readBulk(t, r)
		if !universe[m] || seen[m] {
			t.Fatalf("distinct draw repeated or invented %q", m)
		}
		seen[m] = true
	}
	send("SRANDMEMBER", "s", "10")
	if n := readArrayLen(t, r); n != 5 {
		t.Fatalf("overcount answered %d members, want the whole set", n)
	}
	for range 5 {
		readBulk(t, r)
	}
	send("SRANDMEMBER", "s", "-8")
	if n := readArrayLen(t, r); n != 8 {
		t.Fatalf("negative count answered %d members, want exactly 8", n)
	}
	for range 8 {
		if m := readBulk(t, r); !universe[m] {
			t.Fatalf("with-replacement draw invented %q", m)
		}
	}
	send("SRANDMEMBER", "s", "0")
	expect(t, r, "*0\r\n")
	send("SCARD", "s")
	expect(t, r, ":5\r\n")

	// Pops: the single form removes what it answers, the count form
	// drains, count 0 touches nothing, an overpop deletes the key.
	send("SPOP", "s")
	popped := readBulk(t, r)
	if !universe[popped] {
		t.Fatalf("SPOP drew %q", popped)
	}
	send("SISMEMBER", "s", popped)
	expect(t, r, ":0\r\n")
	send("SCARD", "s")
	expect(t, r, ":4\r\n")
	send("SPOP", "s", "0")
	expect(t, r, "*0\r\n")
	send("SCARD", "s")
	expect(t, r, ":4\r\n")
	send("SPOP", "s", "2")
	if n := readArrayLen(t, r); n != 2 {
		t.Fatalf("SPOP count 2 answered %d members", n)
	}
	for range 2 {
		m := readBulk(t, r)
		if !universe[m] || m == popped {
			t.Fatalf("SPOP count drew %q", m)
		}
	}
	send("SCARD", "s")
	expect(t, r, ":2\r\n")
	send("SPOP", "s", "99")
	if n := readArrayLen(t, r); n != 2 {
		t.Fatalf("overpop answered %d members, want the 2 left", n)
	}
	for range 2 {
		readBulk(t, r)
	}
	send("TYPE", "s")
	expect(t, r, "+none\r\n")
}

func TestServerSetAlgebra(t *testing.T) {
	c, r := startServer(t)
	send := func(args ...string) {
		t.Helper()
		if _, err := c.Write([]byte(respCmd(args...))); err != nil {
			t.Fatal(err)
		}
	}
	readSet := func() map[string]bool {
		t.Helper()
		got := map[string]bool{}
		n := readArrayLen(t, r)
		for range n {
			m := readBulk(t, r)
			if got[m] {
				t.Fatalf("member %q answered twice", m)
			}
			got[m] = true
		}
		return got
	}
	wantMembers := func(got map[string]bool, want ...string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %d members, want %d", len(got), len(want))
		}
		for _, m := range want {
			if !got[m] {
				t.Fatalf("member %q missing", m)
			}
		}
	}

	send("SADD", "s1", "a", "b", "c")
	expect(t, r, ":3\r\n")
	send("SADD", "s2", "b", "c", "d")
	expect(t, r, ":3\r\n")
	send("SADD", "s3", "c", "d", "e")
	expect(t, r, ":3\r\n")
	send("SET", "str", "v")
	expect(t, r, "+OK\r\n")

	// SINTER: order-free member sets, the absent-key short circuit,
	// and its wrong-type masking.
	send("SINTER", "s1", "s2", "s3")
	wantMembers(readSet(), "c")
	send("SINTER", "s1", "s2")
	wantMembers(readSet(), "b", "c")
	send("SINTER", "s1")
	wantMembers(readSet(), "a", "b", "c")
	send("SINTER", "s1", "ghost", "s2")
	expect(t, r, "*0\r\n")
	send("SINTER", "ghost", "str")
	expect(t, r, "*0\r\n")
	send("SINTER", "str", "s1")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("SINTER")
	expect(t, r, "-ERR wrong number of arguments for 'sinter' command\r\n")

	// SUNION: dedupe across sources, absent keys as empty sets, no
	// wrong-type masking.
	send("SUNION", "s1", "s2")
	wantMembers(readSet(), "a", "b", "c", "d")
	send("SUNION", "ghost", "ghost2")
	expect(t, r, "*0\r\n")
	send("SUNION", "ghost", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("SUNION")
	expect(t, r, "-ERR wrong number of arguments for 'sunion' command\r\n")

	// SDIFF: first set drives, absent first set is empty, absent rest
	// sets remove nothing.
	send("SDIFF", "s1", "s2")
	wantMembers(readSet(), "a")
	send("SDIFF", "s1", "ghost")
	wantMembers(readSet(), "a", "b", "c")
	send("SDIFF", "ghost", "s1")
	expect(t, r, "*0\r\n")
	send("SDIFF", "ghost", "str")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("SDIFF")
	expect(t, r, "-ERR wrong number of arguments for 'sdiff' command\r\n")

	// SINTERCARD: values, the LIMIT clamp, and Redis's exact doors.
	send("SINTERCARD", "2", "s1", "s2")
	expect(t, r, ":2\r\n")
	send("SINTERCARD", "2", "s1", "s2", "LIMIT", "1")
	expect(t, r, ":1\r\n")
	send("SINTERCARD", "2", "s1", "s2", "LIMIT", "0")
	expect(t, r, ":2\r\n")
	send("SINTERCARD", "2", "s1", "ghost")
	expect(t, r, ":0\r\n")
	send("SINTERCARD", "2", "ghost", "str")
	expect(t, r, ":0\r\n")
	send("SINTERCARD", "2", "str", "s1")
	expect(t, r, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	send("SINTERCARD", "0", "s1")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("SINTERCARD", "-1", "s1")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("SINTERCARD", "abc", "s1")
	expect(t, r, "-ERR numkeys should be greater than 0\r\n")
	send("SINTERCARD", "3", "s1", "s2")
	expect(t, r, "-ERR Number of keys can't be greater than number of args\r\n")
	send("SINTERCARD", "2", "s1", "s2", "LIMIT", "-1")
	expect(t, r, "-ERR LIMIT can't be negative\r\n")
	send("SINTERCARD", "2", "s1", "s2", "LIMIT", "abc")
	expect(t, r, "-ERR LIMIT can't be negative\r\n")
	send("SINTERCARD", "2", "s1", "s2", "EXTRA", "1")
	expect(t, r, "-ERR syntax error\r\n")
	send("SINTERCARD", "2", "s1", "s2", "LIMIT", "1", "x")
	expect(t, r, "-ERR syntax error\r\n")
	send("SINTERCARD", "2")
	expect(t, r, "-ERR wrong number of arguments for 'sintercard' command\r\n")
}
