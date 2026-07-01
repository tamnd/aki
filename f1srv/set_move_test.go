package f1srv

import "testing"

// SMOVE moves a member out of the source set and into the destination, returns 1, and
// leaves both cardinalities correct: the source loses the member and the destination
// gains it.
func TestSMoveBasic(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "src", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "dst", "x")
	expect(t, rw, ":1")

	cmd(t, rw, "SMOVE", "src", "dst", "b")
	expect(t, rw, ":1")

	// The member left the source and joined the destination.
	cmd(t, rw, "SISMEMBER", "src", "b")
	expect(t, rw, ":0")
	cmd(t, rw, "SISMEMBER", "dst", "b")
	expect(t, rw, ":1")
	cmd(t, rw, "SCARD", "src")
	expect(t, rw, ":2")
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":2")
}

// SMOVE of a member the source does not hold returns 0 and touches neither set, whether
// the destination already has the member or not.
func TestSMoveMemberNotInSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "src", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SADD", "dst", "z")
	expect(t, rw, ":1")

	cmd(t, rw, "SMOVE", "src", "dst", "nope")
	expect(t, rw, ":0")

	// Nothing changed on either side.
	cmd(t, rw, "SCARD", "src")
	expect(t, rw, ":1")
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":1")
	cmd(t, rw, "SISMEMBER", "dst", "nope")
	expect(t, rw, ":0")
}

// When the destination already holds the member, SMOVE only removes it from the source
// and does not double-count the destination.
func TestSMoveMemberAlreadyInDest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "src", "a", "b")
	expect(t, rw, ":2")
	cmd(t, rw, "SADD", "dst", "b", "c")
	expect(t, rw, ":2")

	cmd(t, rw, "SMOVE", "src", "dst", "b")
	expect(t, rw, ":1")

	// Source lost b; destination is unchanged since it already had b.
	cmd(t, rw, "SISMEMBER", "src", "b")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "src")
	expect(t, rw, ":1")
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":2")
}

// Moving the last member of the source deletes the source key entirely.
func TestSMoveEmptiesSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "src", "only")
	expect(t, rw, ":1")

	cmd(t, rw, "SMOVE", "src", "dst", "only")
	expect(t, rw, ":1")

	// The source header is gone, so SCARD reports 0 and the key does not exist.
	cmd(t, rw, "SCARD", "src")
	expect(t, rw, ":0")
	cmd(t, rw, "EXISTS", "src")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":1")
}

// SMOVE into a brand-new destination creates the destination set with the member.
func TestSMoveCreatesDest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "src", "a", "b")
	expect(t, rw, ":2")

	cmd(t, rw, "SMOVE", "src", "fresh", "a")
	expect(t, rw, ":1")

	cmd(t, rw, "SCARD", "fresh")
	expect(t, rw, ":1")
	cmd(t, rw, "SISMEMBER", "fresh", "a")
	expect(t, rw, ":1")
}

// SMOVE with the source equal to the destination is a no-op that reports whether the
// member is present: 1 when it is, 0 when it is not, and the set is unchanged either way.
func TestSMoveSourceEqualsDest(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b")
	expect(t, rw, ":2")

	// Present member: reports 1, set untouched.
	cmd(t, rw, "SMOVE", "s", "s", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":2")
	cmd(t, rw, "SISMEMBER", "s", "a")
	expect(t, rw, ":1")

	// Absent member: reports 0, set untouched.
	cmd(t, rw, "SMOVE", "s", "s", "nope")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "s")
	expect(t, rw, ":2")
}

// A missing source is a member-not-present move: 0, and the destination stays untouched.
func TestSMoveMissingSource(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "dst", "x")
	expect(t, rw, ":1")

	cmd(t, rw, "SMOVE", "gone", "dst", "a")
	expect(t, rw, ":0")
	cmd(t, rw, "SCARD", "dst")
	expect(t, rw, ":1")
}

// SMOVE is WRONGTYPE when either the source or the destination holds a plain string, and
// it does not mutate either key in that case.
func TestSMoveWrongType(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SET", "str", "v")
	expect(t, rw, "+OK")
	cmd(t, rw, "SADD", "set", "a")
	expect(t, rw, ":1")

	// Source is a string.
	cmd(t, rw, "SMOVE", "str", "set", "a")
	expect(t, rw, "-"+wrongType)
	// Destination is a string.
	cmd(t, rw, "SMOVE", "set", "str", "a")
	expect(t, rw, "-"+wrongType)

	// Neither side changed: the set still has its member, the string is intact.
	cmd(t, rw, "SCARD", "set")
	expect(t, rw, ":1")
	cmd(t, rw, "GET", "str")
	expect(t, rw, "$v")
}

// SMOVE rejects the wrong argument count.
func TestSMoveArity(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SMOVE", "a", "b")
	expect(t, rw, "-ERR wrong number of arguments for 'smove' command")
}
