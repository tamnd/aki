package f1srv

import (
	"fmt"
	"strings"
	"testing"
)

// Slice 6a-1 replaces the whole-server forceP hook as the source of a set's partition count with a
// per-key registry: partitionsFor consults forceP first (the test and microbench override) and then
// the registry, so a set that has engaged the adaptive transition routes through the P recorded for
// its own key while every other set stays P=1 (spec 2064/f1_rewrite_ltm/19 slice 6). These tests pin
// the registry primitives and prove that a partitioned set routes identically whether its P comes
// from forceP or from the registry, which is the invariant the migration in slice 6a-2 relies on.

// TestPartitionRegistryPrimitives exercises engageP, unengageP, and the lock-free partitionP read
// directly: an empty registry answers P=1 for every key, an engaged key answers its recorded P while
// its neighbors stay P=1, a second engagement leaves the first intact, and a drop resets exactly the
// dropped key to P=1 and leaves the rest. A drop of a never-engaged key is a no-op.
func TestPartitionRegistryPrimitives(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0", IndexBuckets: 1 << 12, ArenaBytes: 1 << 20, ReadBufSize: 4 << 10, IncrStripes: 64})

	if p := srv.partitionP([]byte("nothing")); p != 1 {
		t.Fatalf("empty registry partitionP = %d, want 1", p)
	}
	// A drop against an empty registry must not panic and must stay a no-op.
	srv.unengageP([]byte("nothing"))

	srv.engageP([]byte("hot"), 4)
	if p := srv.partitionP([]byte("hot")); p != 4 {
		t.Fatalf("engaged hot partitionP = %d, want 4", p)
	}
	if p := srv.partitionP([]byte("cold")); p != 1 {
		t.Fatalf("unengaged neighbor partitionP = %d, want 1", p)
	}

	srv.engageP([]byte("hot2"), 8)
	if p := srv.partitionP([]byte("hot")); p != 4 {
		t.Fatalf("hot partitionP after second engage = %d, want 4", p)
	}
	if p := srv.partitionP([]byte("hot2")); p != 8 {
		t.Fatalf("hot2 partitionP = %d, want 8", p)
	}

	// Re-engaging one key at a higher P overwrites just its entry (the grow case).
	srv.engageP([]byte("hot"), 16)
	if p := srv.partitionP([]byte("hot")); p != 16 {
		t.Fatalf("hot partitionP after re-engage = %d, want 16", p)
	}
	if p := srv.partitionP([]byte("hot2")); p != 8 {
		t.Fatalf("hot2 partitionP after hot re-engage = %d, want 8", p)
	}

	srv.unengageP([]byte("hot"))
	if p := srv.partitionP([]byte("hot")); p != 1 {
		t.Fatalf("dropped hot partitionP = %d, want 1", p)
	}
	if p := srv.partitionP([]byte("hot2")); p != 8 {
		t.Fatalf("hot2 partitionP after hot drop = %d, want 8", p)
	}
	// A second drop of the same key is a no-op.
	srv.unengageP([]byte("hot"))
	if p := srv.partitionP([]byte("hot2")); p != 8 {
		t.Fatalf("hot2 partitionP after redundant hot drop = %d, want 8", p)
	}
}

// TestPartitionRegistryDrivesRouting builds a partitioned set under forceP so its member rows and
// header carry the P>1 layout, then clears forceP and records the same P in the registry, and asserts
// every read routes identically off the registry as it did off forceP. This is the crossover the
// migration will perform: a set laid out at P>1 must serve correctly when its P comes from the
// registry, not just from the whole-server hook. The forceP-off, registry-off case (an unregistered
// key) is checked to still take the P=1 path.
func TestPartitionRegistryDrivesRouting(t *testing.T) {
	const p = 4
	srv := newPartServer(t, p)
	defer srv.Close()
	c := bareConn(srv)

	var members []string
	for i := 0; i < 200; i++ {
		members = append(members, fmt.Sprintf("m:%05d", i))
	}
	loadSet(t, c, "hot", members)

	// Reference replies with the whole-server forceP hook still driving the P>1 routing.
	refIsm := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", "m:00042")
	refMiss := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", "absent")
	refCard := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot")
	refMembers := strings.Join(smembersSorted(t, c, "hot"), "\x00")

	// Retire the whole-server hook and drive the same P from the per-key registry instead.
	srv.forceP.Store(0)
	if got := c.partitionsFor([]byte("hot")); got != 1 {
		t.Fatalf("with forceP off and no registry entry, partitionsFor(hot) = %d, want 1", got)
	}
	srv.engageP([]byte("hot"), p)
	if got := c.partitionsFor([]byte("hot")); got != p {
		t.Fatalf("registry-driven partitionsFor(hot) = %d, want %d", got, p)
	}

	if got := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", "m:00042"); got != refIsm {
		t.Fatalf("registry SISMEMBER present = %q, want %q", got, refIsm)
	}
	if got := call(c, func(c *connState, a [][]byte) { c.cmdSIsMember(a) }, "SISMEMBER", "hot", "absent"); got != refMiss {
		t.Fatalf("registry SISMEMBER absent = %q, want %q", got, refMiss)
	}
	if got := call(c, func(c *connState, a [][]byte) { c.cmdSCard(a) }, "SCARD", "hot"); got != refCard {
		t.Fatalf("registry SCARD = %q, want %q", got, refCard)
	}
	if got := strings.Join(smembersSorted(t, c, "hot"), "\x00"); got != refMembers {
		t.Fatalf("registry SMEMBERS differ from forceP reference")
	}
}
