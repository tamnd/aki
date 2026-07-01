package f1srv

import "testing"

// SINTERSTORE writes the intersection into the destination and returns its cardinality, and
// the destination reads back as exactly that set.
func TestSInterStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y", "z", "w")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "y", "z", "q")
	expect(t, rw, ":3")

	// Intersection {y, z} is stored, so the reply is 2 and the destination holds {y, z}.
	cmd(t, rw, "SINTERSTORE", "dst", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SMEMBERS", "dst")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"y", "z"}) {
		t.Fatalf("SINTERSTORE dst = %v, want [y z]", got)
	}
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":2")

	// An empty intersection deletes the destination and returns 0.
	cmd(t, rw, "SADD", "disjoint", "1", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "SINTERSTORE", "dst", "a", "disjoint")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "dst")
	expect(t, rw, ":0")
}

// SUNIONSTORE writes the deduplicated union into the destination and returns its size.
func TestSUnionStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "b", "y", "z")
	expect(t, rw, ":2")

	cmd(t, rw, "SUNIONSTORE", "dst", "a", "b")
	expect(t, rw, ":3")
	cmd(t, rw, "SMEMBERS", "dst")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"x", "y", "z"}) {
		t.Fatalf("SUNIONSTORE dst = %v, want [x y z]", got)
	}
}

// SDIFFSTORE writes the first-set-minus-the-rest difference into the destination.
func TestSDiffStore(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y", "z", "w")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "y")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "c", "z")
	expect(t, rw, ":1")

	cmd(t, rw, "SDIFFSTORE", "dst", "a", "b", "c")
	expect(t, rw, ":2")
	cmd(t, rw, "SMEMBERS", "dst")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"w", "x"}) {
		t.Fatalf("SDIFFSTORE dst = %v, want [w x]", got)
	}
}

// A STORE form overwrites whatever the destination held before, including a plain string,
// which is dropped rather than raising WRONGTYPE (only the sources are type-checked).
func TestStoreOverwritesDestination(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "b", "y", "z")
	expect(t, rw, ":2")

	// A string at the destination is replaced, not a WRONGTYPE.
	cmd(t, rw, "SET", "dst", "old-string-value")
	expect(t, rw, "+OK")
	cmd(t, rw, "SUNIONSTORE", "dst", "a", "b")
	expect(t, rw, ":3")
	// The old string record is dropped: GET no longer returns it. The key is a set now,
	// which SMEMBERS and SCARD confirm.
	cmd(t, rw, "GET", "dst")
	expect(t, rw, "$-1")
	cmd(t, rw, "SMEMBERS", "dst")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"x", "y", "z"}) {
		t.Fatalf("dst after overwriting a string = %v, want [x y z]", got)
	}
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":3")

	// A prior set at the destination is fully replaced, not merged: old members are gone.
	cmd(t, rw, "SADD", "dst2", "keep-me", "and-me")
	expect(t, rw, ":2")
	cmd(t, rw, "SINTERSTORE", "dst2", "a", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "SMEMBERS", "dst2")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"y"}) {
		t.Fatalf("dst2 after overwriting a set = %v, want [y] (old members dropped)", got)
	}
}

// The aliased case: the destination is also one of the sources. The result is buffered
// before the destination is cleared, so the store computes against the pre-clear source and
// the reply and contents are correct even though dest and source are the same key.
func TestStoreAliasedDestination(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "x", "y", "z", "w")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "y", "z", "q")
	expect(t, rw, ":3")

	// SINTERSTORE a a b: a becomes a ∩ b = {y, z}.
	cmd(t, rw, "SINTERSTORE", "a", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SMEMBERS", "a")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"y", "z"}) {
		t.Fatalf("SINTERSTORE a a b -> a = %v, want [y z]", got)
	}

	// SUNIONSTORE onto a source: c becomes c ∪ d, dest aliases the first source.
	cmd(t, rw, "SADD", "c", "1", "2")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "d", "2", "3")
	expect(t, rw, ":2")
	cmd(t, rw, "SUNIONSTORE", "c", "c", "d")
	expect(t, rw, ":3")
	cmd(t, rw, "SMEMBERS", "c")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"1", "2", "3"}) {
		t.Fatalf("SUNIONSTORE c c d -> c = %v, want [1 2 3]", got)
	}

	// SDIFFSTORE onto its own base: e becomes e - f.
	cmd(t, rw, "SADD", "e", "p", "q", "r")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "f", "q")
	expect(t, rw, ":1")
	cmd(t, rw, "SDIFFSTORE", "e", "e", "f")
	expect(t, rw, ":2")
	cmd(t, rw, "SMEMBERS", "e")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"p", "r"}) {
		t.Fatalf("SDIFFSTORE e e f -> e = %v, want [p r]", got)
	}

	// An aliased store whose result is empty deletes the destination even though it was a
	// source: SINTERSTORE g g h with disjoint h leaves g gone.
	cmd(t, rw, "SADD", "g", "m", "n")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "h", "x", "y")
	expect(t, rw, ":2")
	cmd(t, rw, "SINTERSTORE", "g", "g", "h")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "g")
	expect(t, rw, ":0")
}

// A STORE form against a source holding a plain string is WRONGTYPE, and the destination is
// left untouched by the rejected command.
func TestStoreWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SADD", "s", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "dst", "keep")
	expect(t, rw, ":1")

	for _, op := range []string{"SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"} {
		cmd(t, rw, op, "dst", "str", "s")
		expect(t, rw, "-"+wrongType)
		cmd(t, rw, op, "dst", "s", "str")
		expect(t, rw, "-"+wrongType)
	}

	// The destination survived every rejected command with its original member.
	cmd(t, rw, "SMEMBERS", "dst")
	if got := sortedArray(t, rw); !eqStrings(got, []string{"keep"}) {
		t.Fatalf("dst after rejected stores = %v, want [keep]", got)
	}
}

// The STORE forms reject the wrong argument count (they need a destination plus one source).
func TestStoreArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SINTERSTORE", "dst")
	expect(t, rw, "-ERR wrong number of arguments for 'sinterstore' command")
	cmd(t, rw, "SUNIONSTORE", "dst")
	expect(t, rw, "-ERR wrong number of arguments for 'sunionstore' command")
	cmd(t, rw, "SDIFFSTORE", "dst")
	expect(t, rw, "-ERR wrong number of arguments for 'sdiffstore' command")
}
