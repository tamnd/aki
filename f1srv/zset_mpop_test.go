package f1srv

import "testing"

// TestZMPop covers the multi-key pop: MIN and MAX direction, the COUNT form, skipping empty keys to
// the first non-empty one, the all-empty null-array reply, and the argument guards. The reply nests
// as [key, [[member, score], ...]].
func TestZMPop(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	expect(t, rw, ":3")

	// MIN pops the single lowest from the first (only) non-empty key.
	cmd(t, rw, "ZMPOP", "1", "z", "MIN")
	expect(t, rw, "*2")
	expect(t, rw, "$z")
	expect(t, rw, "*1")
	expect(t, rw, "*2")
	expect(t, rw, "$a")
	expect(t, rw, "$1")

	// MAX with COUNT pops the two highest, highest first.
	cmd(t, rw, "ZMPOP", "1", "z", "MAX", "COUNT", "2")
	expect(t, rw, "*2")
	expect(t, rw, "$z")
	expect(t, rw, "*2")
	expect(t, rw, "*2")
	expect(t, rw, "$c")
	expect(t, rw, "$3")
	expect(t, rw, "*2")
	expect(t, rw, "$b")
	expect(t, rw, "$2")

	// The key emptied out, so it no longer exists.
	cmd(t, rw, "EXISTS", "z")
	expect(t, rw, ":0")
}

// TestZMPopSkipsEmpty confirms ZMPOP walks past missing and empty keys to the first with members.
func TestZMPopSkipsEmpty(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZADD", "z2", "5", "x", "6", "y")
	expect(t, rw, ":2")

	// nokey is missing, z1 was never created, z2 has members: pop lands on z2.
	cmd(t, rw, "ZMPOP", "3", "nokey", "z1", "z2", "MIN")
	expect(t, rw, "*2")
	expect(t, rw, "$z2")
	expect(t, rw, "*1")
	expect(t, rw, "*2")
	expect(t, rw, "$x")
	expect(t, rw, "$5")
}

// TestZMPopAllEmpty confirms the null-array reply when no key has members.
func TestZMPopAllEmpty(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZMPOP", "2", "nokey", "alsonokey", "MIN")
	expect(t, rw, "*-1")
}

// TestZMPopErrors covers the argument guards: bad numkeys, a missing direction, an unknown option,
// and a non-positive count.
func TestZMPopErrors(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "ZMPOP", "0", "MIN")
	expect(t, rw, "-ERR numkeys should be greater than 0")

	cmd(t, rw, "ZMPOP", "x", "z", "MIN")
	expect(t, rw, "-ERR numkeys should be greater than 0")

	// Direction token is missing (only the key follows numkeys).
	cmd(t, rw, "ZMPOP", "1", "z")
	expect(t, rw, "-ERR syntax error")

	// A direction that is neither MIN nor MAX.
	cmd(t, rw, "ZMPOP", "1", "z", "SIDEWAYS")
	expect(t, rw, "-ERR syntax error")

	// COUNT must be greater than zero.
	cmd(t, rw, "ZADD", "z", "1", "a")
	expect(t, rw, ":1")
	cmd(t, rw, "ZMPOP", "1", "z", "MIN", "COUNT", "0")
	expect(t, rw, "-ERR count should be greater than 0")
}
