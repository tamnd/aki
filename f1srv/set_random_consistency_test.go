package f1srv

import (
	"bufio"
	"fmt"
	"testing"
)

// drawNoCount issues one no-count SRANDMEMBER and returns the drawn member, failing on a
// nil or a wrong-type reply. It is the read that consults the dense member vector, so
// every test here uses it to prove the vector tracks the current membership.
func drawNoCount(t *testing.T, rw *bufio.ReadWriter, key string) string {
	t.Helper()
	cmd(t, rw, "SRANDMEMBER", key)
	got := readReply(t, rw)
	if len(got) == 0 || got[0] != '$' {
		t.Fatalf("SRANDMEMBER %s = %q, want a bulk member", key, got)
	}
	return got[1:]
}

// assertDrawsWithin draws many times and fails if any draw lands outside want, so a stale
// vector slot pointing at a removed member is caught. It also fails if want is empty and a
// draw returns a member, the missing-key contract.
func assertDrawsWithin(t *testing.T, rw *bufio.ReadWriter, key string, want map[string]bool) {
	t.Helper()
	if len(want) == 0 {
		cmd(t, rw, "SRANDMEMBER", key)
		expect(t, rw, "$-1")
		return
	}
	for i := 0; i < 60; i++ {
		m := drawNoCount(t, rw, key)
		if !want[m] {
			t.Fatalf("SRANDMEMBER %s drew %q, not one of the live members", key, m)
		}
	}
}

// After a first draw builds the vector, an SREM must swap-remove the gone members so no
// later draw ever returns one. This exercises the SREM routing site with a live vector.
func TestRandVecConsistencyAfterSRem(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c", "d", "e")
	expect(t, rw, ":5")
	// First draw builds the vector from all five members.
	drawNoCount(t, rw, "s")

	cmd(t, rw, "SREM", "s", "a", "c", "e")
	expect(t, rw, ":3")
	// Only b and d remain; a stale slot for a, c, or e would surface here.
	assertDrawsWithin(t, rw, "s", map[string]bool{"b": true, "d": true})

	cmd(t, rw, "SREM", "s", "b", "d")
	expect(t, rw, ":2")
	// Drained to empty: the set is gone and a draw is nil.
	assertDrawsWithin(t, rw, "s", nil)
}

// A DEL drops the whole vector, and a fresh set under the same key rebuilds a correct one
// on its next first draw. This exercises the DEL CollRandDrop plus the lazy rebuild.
func TestRandVecConsistencyAfterDelAndRecreate(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "s", "a", "b", "c")
	expect(t, rw, ":3")
	drawNoCount(t, rw, "s") // build the vector

	cmd(t, rw, "DEL", "s")
	expect(t, rw, ":1")
	assertDrawsWithin(t, rw, "s", nil)

	// A new set under the same key must draw only its own members, never a ghost of the old.
	cmd(t, rw, "SADD", "s", "x", "y")
	expect(t, rw, ":2")
	assertDrawsWithin(t, rw, "s", map[string]bool{"x": true, "y": true})
}

// SMOVE removes from the source and adds to the destination, so after both vectors are
// built a move must leave neither drawing the moved member on the wrong side.
func TestRandVecConsistencyAfterSMove(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "src", "a", "b", "c")
	expect(t, rw, ":3")
	cmd(t, rw, "SADD", "dst", "x", "y")
	expect(t, rw, ":2")
	// Build both vectors before the move.
	drawNoCount(t, rw, "src")
	drawNoCount(t, rw, "dst")

	cmd(t, rw, "SMOVE", "src", "dst", "b")
	expect(t, rw, ":1")

	// b left src and joined dst; a stale src slot or a missing dst slot would show here.
	assertDrawsWithin(t, rw, "src", map[string]bool{"a": true, "c": true})
	assertDrawsWithin(t, rw, "dst", map[string]bool{"x": true, "y": true, "b": true})
}

// RENAME drops the source vector and lets the destination rebuild on first draw, since the
// moved rows land at fresh arena offsets. A draw on the new name must see exactly the moved
// members, and the old name must be gone.
func TestRandVecConsistencyAfterRename(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "old", "a", "b", "c")
	expect(t, rw, ":3")
	drawNoCount(t, rw, "old") // build the source vector

	cmd(t, rw, "RENAME", "old", "new")
	expect(t, rw, "+OK")

	assertDrawsWithin(t, rw, "old", nil)
	assertDrawsWithin(t, rw, "new", map[string]bool{"a": true, "b": true, "c": true})
}

// A SINTERSTORE into an existing set overwrites it, so the destination's old vector must be
// dropped and a draw must see only the freshly stored intersection.
func TestRandVecConsistencyAfterStoreOverwrite(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	cmd(t, rw, "SADD", "a", "1", "2", "3", "4")
	expect(t, rw, ":4")
	cmd(t, rw, "SADD", "b", "2", "3", "5")
	expect(t, rw, ":3")
	// Give the destination a prior set and a built vector.
	cmd(t, rw, "SADD", "dst", "99")
	expect(t, rw, ":1")
	drawNoCount(t, rw, "dst")

	cmd(t, rw, "SINTERSTORE", "dst", "a", "b")
	expect(t, rw, ":2") // {2,3}
	// The old member 99 must never surface; only the intersection is live.
	assertDrawsWithin(t, rw, "dst", map[string]bool{"2": true, "3": true})
}

// A steady interleave of adds, removes, and draws must never draw a removed member. This is
// the routing invariant under churn rather than a single mutation.
func TestRandVecConsistencyUnderChurn(t *testing.T) {
	rw, cleanup := dialTestServer(t)
	defer cleanup()

	live := map[string]bool{}
	for i := 0; i < 40; i++ {
		m := fmt.Sprintf("m%02d", i)
		cmd(t, rw, "SADD", "s", m)
		readReply(t, rw)
		live[m] = true
	}
	drawNoCount(t, rw, "s") // build

	// Remove every third member, then confirm no draw returns one of them.
	for i := 0; i < 40; i += 3 {
		m := fmt.Sprintf("m%02d", i)
		cmd(t, rw, "SREM", "s", m)
		readReply(t, rw)
		delete(live, m)
	}
	assertDrawsWithin(t, rw, "s", live)

	// Add a fresh batch and confirm the new members are drawable and the removed ones stay out.
	for i := 40; i < 50; i++ {
		m := fmt.Sprintf("m%02d", i)
		cmd(t, rw, "SADD", "s", m)
		readReply(t, rw)
		live[m] = true
	}
	assertDrawsWithin(t, rw, "s", live)
}
