package f1srv

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Slice 5c routes SMOVE over the P partitions of both the source and the destination set (spec
// 2064/f1_rewrite_ltm/19 section 6.10). The member routes to one partition in the source and,
// independently, one partition in the destination, and the command locks exactly those two
// partition stripes in ascending stripe-index order so it can never deadlock against another SMOVE
// from the opposite argument order or against a concurrent algebra or SMEMBERS holding a superset of
// stripes. These tests pin that a routed SMOVE returns and leaves behind exactly what the
// unpartitioned path does across P=1, 2, 4, 8, that the same-key no-op and the WRONGTYPE guard
// survive routing, and that heavy two-key traffic under both lock orders never deadlocks.

// smoveArgs builds an SMOVE argument list.
func smoveArgs(src, dst, member string) []string {
	return []string{"SMOVE", src, dst, member}
}

// TestSetMovePartitionIdentical replays a scripted sequence of SMOVEs between two overlapping sets
// at P=1, 2, 4, 8 and asserts every reply plus the final contents and cardinalities of both sets are
// identical across all P. Matching the P=1 unpartitioned path proves the routed remove-from-source
// and add-to-destination land under the right partition keys and keep both header counts in step.
// The members are spread over many byte values so they scatter across every partition of P=8, and
// the script covers a member in the source only, a member already in the destination, a member in
// neither, and a move back so the present, duplicate, and absent branches are all exercised.
func TestSetMovePartitionIdentical(t *testing.T) {
	type snapshot struct {
		replies  []string
		aMembers string
		bMembers string
		aCard    string
		bCard    string
	}
	run := func(p int) snapshot {
		srv := newPartServer(t, p)
		defer srv.Close()
		c := bareConn(srv)

		// A holds shared:* and aonly:*, B holds shared:* and bonly:*, so the two overlap on shared:*.
		var amem, bmem []string
		for i := 0; i < 40; i++ {
			amem = append(amem, fmt.Sprintf("shared:%04d", i), fmt.Sprintf("aonly:%04d", i))
			bmem = append(bmem, fmt.Sprintf("shared:%04d", i), fmt.Sprintf("bonly:%04d", i))
		}
		loadSet(t, c, "A", amem)
		loadSet(t, c, "B", bmem)

		var replies []string
		move := func(src, dst, m string) {
			replies = append(replies, call(c, func(c *connState, a [][]byte) { c.cmdSMove(a) }, smoveArgs(src, dst, m)...))
		}
		// A-only member moves to B (a genuine move onto a fresh destination partition).
		for i := 0; i < 20; i++ {
			move("A", "B", fmt.Sprintf("aonly:%04d", i))
		}
		// Shared member moves to B where it already lives (removed from A, not duplicated in B).
		for i := 0; i < 20; i++ {
			move("A", "B", fmt.Sprintf("shared:%04d", i))
		}
		// Member in neither set (already moved, or never present): no-op reporting 0.
		for i := 0; i < 10; i++ {
			move("A", "B", fmt.Sprintf("aonly:%04d", i))
			move("A", "B", fmt.Sprintf("ghost:%04d", i))
		}
		// Move some back B->A to exercise the reverse key order under the same partition stripes.
		for i := 0; i < 15; i++ {
			move("B", "A", fmt.Sprintf("bonly:%04d", i))
		}
		return snapshot{
			replies:  replies,
			aMembers: strings.Join(smembersSorted(t, c, "A"), "\x00"),
			bMembers: strings.Join(smembersSorted(t, c, "B"), "\x00"),
			aCard:    call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "A"),
			bCard:    call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "B"),
		}
	}

	ref := run(1)
	for _, p := range []int{2, 4, 8} {
		got := run(p)
		if len(got.replies) != len(ref.replies) {
			t.Fatalf("P=%d produced %d replies, want %d", p, len(got.replies), len(ref.replies))
		}
		for i := range ref.replies {
			if got.replies[i] != ref.replies[i] {
				t.Fatalf("P=%d SMOVE reply %d = %q, want %q (P=1)", p, i, got.replies[i], ref.replies[i])
			}
		}
		if got.aMembers != ref.aMembers {
			t.Fatalf("P=%d final A contents differ from P=1", p)
		}
		if got.bMembers != ref.bMembers {
			t.Fatalf("P=%d final B contents differ from P=1", p)
		}
		if got.aCard != ref.aCard || got.bCard != ref.bCard {
			t.Fatalf("P=%d cards (A=%q B=%q) differ from P=1 (A=%q B=%q)", p, got.aCard, got.bCard, ref.aCard, ref.bCard)
		}
	}
}

// TestSetMoveSameKeyPartition asserts SMOVE k k m is a no-op reporting whether m is present, at every
// P. Source equal to destination routes both locks to the same partition of the same key, so the
// command must report presence and touch nothing, never double-lock or drop the member.
func TestSetMoveSameKeyPartition(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		loadSet(t, c, "s", []string{"alpha", "beta", "gamma"})
		present := call(c, func(c *connState, a [][]byte) { c.cmdSMove(a) }, smoveArgs("s", "s", "beta")...)
		if present != ":1\r\n" {
			t.Fatalf("P=%d SMOVE s s beta (present) = %q, want :1", p, present)
		}
		absent := call(c, func(c *connState, a [][]byte) { c.cmdSMove(a) }, smoveArgs("s", "s", "missing")...)
		if absent != ":0\r\n" {
			t.Fatalf("P=%d SMOVE s s missing (absent) = %q, want :0", p, absent)
		}
		// The set is untouched: all three members still present.
		if card := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "s"); card != ":3\r\n" {
			t.Fatalf("P=%d SCARD after same-key SMOVE = %q, want :3", p, card)
		}
		srv.Close()
	}
}

// TestSetMoveWrongTypePartition asserts a plain string at either the source or the destination makes
// SMOVE WRONGTYPE at every P, and leaves both keys untouched. The type guard covers both keys, and
// routing must not bypass it.
func TestSetMoveWrongTypePartition(t *testing.T) {
	for _, p := range []int{1, 2, 4, 8} {
		srv := newPartServer(t, p)
		c := bareConn(srv)
		loadSet(t, c, "set", []string{"x", "y", "z"})
		call(c, func(c *connState, a [][]byte) { c.cmdSet(a) }, "SET", "str", "plain")

		if r := call(c, func(c *connState, a [][]byte) { c.cmdSMove(a) }, smoveArgs("str", "set", "x")...); !strings.HasPrefix(r, "-WRONGTYPE") {
			t.Fatalf("P=%d SMOVE from string source = %q, want WRONGTYPE", p, r)
		}
		if r := call(c, func(c *connState, a [][]byte) { c.cmdSMove(a) }, smoveArgs("set", "str", "x")...); !strings.HasPrefix(r, "-WRONGTYPE") {
			t.Fatalf("P=%d SMOVE to string destination = %q, want WRONGTYPE", p, r)
		}
		if card := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "set"); card != ":3\r\n" {
			t.Fatalf("P=%d set changed after WRONGTYPE SMOVE, SCARD = %q, want :3", p, card)
		}
		srv.Close()
	}
}

// TestSetMoveConcurrentNoDeadlock hammers two partitioned hot keys with SMOVEs in both directions
// while other goroutines run SINTER and SMEMBERS over the same pair, and asserts the run completes
// (a lock-order cycle would hang it) with a conserved total membership. SMOVE locks two partition
// stripes and the algebra locks a superset of both keys' stripes; both take stripes in ascending
// index order, so the mixed traffic must never deadlock, and every member stays in exactly one of
// the two sets because a move is atomic under both locks.
func TestSetMoveConcurrentNoDeadlock(t *testing.T) {
	srv := newPartServer(t, 8)
	defer srv.Close()

	// Seed A with all members; B empty. Members shuffle between the two under concurrent SMOVEs.
	loader := bareConn(srv)
	var all []string
	for i := 0; i < 400; i++ {
		all = append(all, fmt.Sprintf("m:%05d", i))
	}
	loadSet(t, loader, "A", all)

	var wg sync.WaitGroup
	// Movers: half push A->B, half push B->A, so both key orders are taken concurrently.
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := bareConn(srv)
			src, dst := "A", "B"
			if w%2 == 1 {
				src, dst = "B", "A"
			}
			for r := 0; r < 200; r++ {
				m := fmt.Sprintf("m:%05d", (w*200+r)%400)
				call(c, func(c *connState, a [][]byte) { c.cmdSMove(a) }, smoveArgs(src, dst, m)...)
			}
		}(w)
	}
	// Readers: algebra and enumeration over the same two keys, holding the superset of stripes.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := bareConn(srv)
			for i := 0; i < 200; i++ {
				call(c, func(c *connState, a [][]byte) { c.cmdSInter(a) }, "SINTER", "A", "B")
				call(c, func(c *connState, a [][]byte) { c.cmdSMembers(a) }, "SMEMBERS", "A")
			}
		}()
	}
	wg.Wait()

	// Every member still lives in exactly one of the two sets: no member lost or duplicated across
	// the two partition stripes as it moved back and forth.
	union := map[string]bool{}
	for _, m := range smembersSorted(t, loader, "A") {
		union[m] = true
	}
	for _, m := range smembersSorted(t, loader, "B") {
		if union[m] {
			t.Fatalf("member %q is in both A and B after concurrent SMOVEs", m)
		}
		union[m] = true
	}
	if len(union) != len(all) {
		t.Fatalf("conserved membership broke: %d distinct members across A and B, want %d", len(union), len(all))
	}
}
