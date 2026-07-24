package sqlo1

import (
	"context"
	"testing"
)

// TestServerExistsTouch pins the wire surface: variadic counting with
// duplicates counted per mention, misses at zero, TOUCH as the same
// count, and the arity doors.
func TestServerExistsTouch(t *testing.T) {
	_, do := scanServerRig(t)

	if got := do("SET", "a", "v"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("SET", "b", "v"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if got := do("EXISTS", "a"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("EXISTS", "nosuch"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("EXISTS", "a", "nosuch", "b", "a"); got != ":3\r\n" {
		t.Fatalf("EXISTS with a duplicate mention = %q, want 3", got)
	}
	if got := do("TOUCH", "a", "nosuch", "b"); got != ":2\r\n" {
		t.Fatal(got)
	}
	if got := do("EXISTS"); got != "-ERR wrong number of arguments for 'exists' command\r\n" {
		t.Fatal(got)
	}
	if got := do("TOUCH"); got != "-ERR wrong number of arguments for 'touch' command\r\n" {
		t.Fatal(got)
	}
}

// TestServerDelUnlink pins UNLINK as a DEL alias and the variadic
// count with duplicates, misses, and both tiers in one call.
func TestServerDelUnlink(t *testing.T) {
	s, do := scanServerRig(t)
	ctx := context.Background()

	for _, k := range []string{"cold1", "cold2", "hot1", "dup"} {
		if got := do("SET", k, "v"); got != "+OK\r\n" {
			t.Fatal(got)
		}
	}
	if do("HSET", "coldh", "f", "v") != ":1\r\n" {
		t.Fatal("HSET")
	}
	if err := s.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s.t.EvictAllForTest()
	if got := do("SET", "hot1", "v2"); got != "+OK\r\n" {
		t.Fatal(got)
	}

	// Two cold keys, a hot key, a collection, and a miss in one call.
	if got := do("UNLINK", "cold1", "nosuch", "hot1", "coldh"); got != ":3\r\n" {
		t.Fatalf("UNLINK across tiers = %q, want 3", got)
	}
	for _, k := range []string{"cold1", "hot1"} {
		if got := do("EXISTS", k); got != ":0\r\n" {
			t.Fatalf("%q survived UNLINK: %q", k, got)
		}
	}
	if got := do("TYPE", "coldh"); got != "+none\r\n" {
		t.Fatalf("coldh survived UNLINK: %q", got)
	}

	// A duplicate mention deletes once.
	if got := do("DEL", "dup", "dup"); got != ":1\r\n" {
		t.Fatalf("DEL dup dup = %q, want 1", got)
	}
	if got := do("DEL", "cold2"); got != ":1\r\n" {
		t.Fatal(got)
	}
	if got := do("DEL", "cold2"); got != ":0\r\n" {
		t.Fatal(got)
	}
	if got := do("DEL"); got != "-ERR wrong number of arguments for 'del' command\r\n" {
		t.Fatal(got)
	}
	if got := do("UNLINK"); got != "-ERR wrong number of arguments for 'unlink' command\r\n" {
		t.Fatal(got)
	}
}

// TestServerTypeTiers pins TYPE for all six types on both sides of
// eviction, so the header tag and the sniffed cold root agree.
func TestServerTypeTiers(t *testing.T) {
	s, do := scanServerRig(t)
	ctx := context.Background()

	if do("SET", "str1", "v") != "+OK\r\n" {
		t.Fatal("SET")
	}
	if do("HSET", "h1", "f", "v") != ":1\r\n" {
		t.Fatal("HSET")
	}
	if do("RPUSH", "l1", "v") != ":1\r\n" {
		t.Fatal("RPUSH")
	}
	if do("SADD", "s1", "m") != ":1\r\n" {
		t.Fatal("SADD")
	}
	if do("ZADD", "z1", "1", "m") != ":1\r\n" {
		t.Fatal("ZADD")
	}
	if got := do("XADD", "x1", "1-1", "f", "v"); got != "$3\r\n1-1\r\n" {
		t.Fatal(got)
	}

	want := map[string]string{
		"str1": "string", "h1": "hash", "l1": "list",
		"s1": "set", "z1": "zset", "x1": "stream",
	}
	check := func(when string) {
		t.Helper()
		for k, name := range want {
			if got := do("TYPE", k); got != "+"+name+"\r\n" {
				t.Fatalf("%s: TYPE %s = %q, want %s", when, k, got, name)
			}
		}
	}
	check("hot")
	if err := s.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s.t.EvictAllForTest()
	check("cold")

	if got := do("TYPE", "nosuch"); got != "+none\r\n" {
		t.Fatal(got)
	}
	if got := do("TYPE"); got != "-ERR wrong number of arguments for 'type' command\r\n" {
		t.Fatal(got)
	}
	if got := do("TYPE", "a", "b"); got != "-ERR wrong number of arguments for 'type' command\r\n" {
		t.Fatal(got)
	}
}

// TestServerRename pins the wire surface of RENAME and RENAMENX: the
// no-such-key error, OK and the integer answers, the same-key edge,
// cross-tier moves, TTL travel, and the arity doors.
func TestServerRename(t *testing.T) {
	s, do := scanServerRig(t)
	ctx := context.Background()

	if got := do("RENAME", "nosuch", "b"); got != "-ERR no such key\r\n" {
		t.Fatalf("RENAME missing src = %q", got)
	}
	if got := do("RENAMENX", "nosuch", "b"); got != "-ERR no such key\r\n" {
		t.Fatalf("RENAMENX missing src = %q", got)
	}

	if do("SET", "a", "v1") != "+OK\r\n" {
		t.Fatal("SET")
	}
	if got := do("RENAME", "a", "b"); got != "+OK\r\n" {
		t.Fatalf("RENAME = %q", got)
	}
	if got := do("GET", "b"); got != "$2\r\nv1\r\n" {
		t.Fatalf("GET b = %q", got)
	}
	if got := do("EXISTS", "a"); got != ":0\r\n" {
		t.Fatal("src survived")
	}

	// Same key both ways.
	if got := do("RENAME", "b", "b"); got != "+OK\r\n" {
		t.Fatalf("same-key RENAME = %q", got)
	}
	if got := do("RENAMENX", "b", "b"); got != ":0\r\n" {
		t.Fatalf("same-key RENAMENX = %q", got)
	}

	// NX refusal and success.
	if do("SET", "c", "v2") != "+OK\r\n" {
		t.Fatal("SET")
	}
	if got := do("RENAMENX", "c", "b"); got != ":0\r\n" {
		t.Fatalf("RENAMENX onto live dst = %q", got)
	}
	if got := do("RENAMENX", "c", "d"); got != ":1\r\n" {
		t.Fatalf("RENAMENX onto free name = %q", got)
	}

	// A cold collection moves and keeps its type; the TTL travels.
	if do("HSET", "coldh", "f", "v") != ":1\r\n" {
		t.Fatal("HSET")
	}
	if do("EXPIRE", "coldh", "100") != ":1\r\n" {
		t.Fatal("EXPIRE")
	}
	if err := s.t.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s.t.EvictAllForTest()
	if got := do("RENAME", "coldh", "warmh"); got != "+OK\r\n" {
		t.Fatalf("RENAME cold collection = %q", got)
	}
	if got := do("TYPE", "warmh"); got != "+hash\r\n" {
		t.Fatalf("TYPE after cold rename = %q", got)
	}
	if got := do("HGET", "warmh", "f"); got != "$1\r\nv\r\n" {
		t.Fatalf("HGET after cold rename = %q", got)
	}
	if got := do("TTL", "warmh"); got != ":100\r\n" {
		t.Fatalf("TTL after rename = %q", got)
	}
	if got := do("TYPE", "coldh"); got != "+none\r\n" {
		t.Fatalf("src type after rename = %q", got)
	}

	if got := do("RENAME", "b"); got != "-ERR wrong number of arguments for 'rename' command\r\n" {
		t.Fatal(got)
	}
	if got := do("RENAMENX", "b"); got != "-ERR wrong number of arguments for 'renamenx' command\r\n" {
		t.Fatal(got)
	}
}
