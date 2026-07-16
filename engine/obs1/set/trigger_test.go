package set

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The set demotion trigger (spec 2064/f3/06 section 6, trigger.go). The worker's
// demote loop drives one collection quantum per boundary through DemoteQuantum, and
// these tests hold the trigger's contract: it self-gates so a store under its
// resident budget or with no cold tier demotes nothing (the L9 zero-delta path),
// it sheds a real quantum once the combined footprint runs past the cap, and it
// picks the largest resident set as the victim.

// coldCtxCap builds a cold-configured store like coldCtx but with an explicit
// resident cap, so a test can push the arena plus the registry heap past the budget
// and watch the trigger fire.
func coldCtxCap(t *testing.T, cap uint64) (*shard.Ctx, *reg) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         1 << 20,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: cap,
	})
	if err != nil {
		t.Fatalf("open cold store: %v", err)
	}
	if !st.ColdConfigured() {
		t.Fatal("store with a cold region and a resident cap should be cold-configured")
	}
	cx := &shard.Ctx{St: st, NowMs: 1}
	g := registry(cx)
	if !g.acctOn {
		t.Fatal("registry on a cold-configured store should account")
	}
	return cx, g
}

// TestDemoteQuantumUnderBudgetNoOp holds the steady path: with the roomy cap the
// registry accounts but the combined footprint sits well under budget, so a demote
// quantum is a no-op that leaves the native slab whole. The trigger reads one
// predicate and returns.
func TestDemoteQuantumUnderBudgetNoOp(t *testing.T) {
	cx, g := coldCtx(t)

	addKey(g, "k", gen("m", 0, 1000, 12)...)
	if enc := g.m["k"].enc; enc != encHashtable {
		t.Fatalf("1000-member set enc %v, want hashtable", enc)
	}
	before := residentOf(g, "k")

	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("under-budget demote moved %d members, want 0", n)
	}
	if g.m["k"].ht.slab == nil {
		t.Fatal("under-budget demote freed the slab")
	}
	if after := residentOf(g, "k"); after != before {
		t.Fatalf("under-budget footprint changed %d -> %d", before, after)
	}
	wantExact(t, g)
}

// TestDemoteQuantumOverBudgetSheds holds the pressure path: a tiny cap the arena
// already overruns makes the combined footprint over budget, so a demote quantum
// packs the victim's members to the cold tier, frees its slab, and drops the
// running total while every member still reads back.
func TestDemoteQuantumOverBudgetSheds(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	members := gen("m", 0, 1000, 12)
	addKey(g, "k", members...)
	if !cx.St.ResidentOverBy(g.resident) {
		t.Fatal("tiny-cap store should read over budget after 1000 adds")
	}
	before := residentOf(g, "k")

	n := DemoteQuantum(cx)
	if n != len(members) {
		t.Fatalf("over-budget demote moved %d members, want %d", n, len(members))
	}
	s := g.m["k"]
	if s.ht.slab != nil {
		t.Fatal("over-budget demote left the slab held")
	}
	if after := residentOf(g, "k"); after >= before {
		t.Fatalf("footprint %d did not drop below the pre-demote %d", after, before)
	}
	for _, m := range members {
		if !s.has([]byte(m)) {
			t.Fatalf("member %q lost after the trigger demote", m)
		}
	}
	wantExact(t, g)
}

// TestDemoteQuantumColdOffZeroDelta is the L9 gate: a store with no cold tier turns
// accounting off, so the trigger returns zero however large the set grows and never
// touches the slab. This is the byte-for-byte M0-M6 path.
func TestDemoteQuantumColdOffZeroDelta(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	addKey(g, "k", gen("m", 0, 1000, 12)...)
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("cold-off demote moved %d members, want 0", n)
	}
	if g.m["k"].ht.slab == nil {
		t.Fatal("cold-off demote freed the slab")
	}
}

// TestDemoteQuantumNoRegistryNoOp holds the string-only shard: a Ctx that never
// built a set registry (Coll still nil) reads no reg from the slot, so the trigger
// returns zero without a panic. The worker calls the hook unconditionally, so it
// must be safe before the first SADD.
func TestDemoteQuantumNoRegistryNoOp(t *testing.T) {
	cx, _ := coldCtxCap(t, 4096)
	cx.Coll = nil
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("nil-registry demote moved %d members, want 0", n)
	}
}

// TestDemoteVictimPicksLargest holds the victim policy: with several demotable sets
// the trigger picks the one with the largest resident footprint, the biggest
// immediate win, and the demote frees that set's slab and leaves the smaller ones
// resident.
func TestDemoteVictimPicksLargest(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	addKey(g, "small", gen("s", 0, 200, 12)...)
	addKey(g, "big", gen("b", 0, 2000, 12)...)
	addKey(g, "mid", gen("d", 0, 800, 12)...)

	if got := g.demoteVictim(); got != "big" {
		t.Fatalf("victim %q, want the largest set \"big\"", got)
	}

	n := DemoteQuantum(cx)
	if n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	if g.m["big"].ht.slab != nil {
		t.Fatal("the largest set kept its slab after being picked")
	}
	if g.m["small"].ht.slab == nil || g.m["mid"].ht.slab == nil {
		t.Fatal("a non-victim set lost its slab")
	}
	wantExact(t, g)
}

// TestDemotableSkipsInlineAndFullyCold holds the demotable predicate that guards the
// victim pick: an inline set (below one chunk's worth) has nothing to shed, and a
// set whose every slab already left for the cold tier has nothing left either, so
// neither can win the pick and stall the loop while resident sets stay hot.
func TestDemotableSkipsInlineAndFullyCold(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	addKey(g, "ints", intGen(0, 8)...)
	if g.m["ints"].demotable() {
		t.Fatal("an inline intset reported demotable")
	}

	members := gen("m", 0, 1000, 12)
	addKey(g, "k", members...)
	if !g.m["k"].demotable() {
		t.Fatal("a native set with a live slab reported not demotable")
	}
	if g.demote(cx, []byte("k")) != len(members) {
		t.Fatal("failed to demote the whole native set")
	}
	if g.m["k"].demotable() {
		t.Fatal("a fully-cold set reported demotable")
	}
	if got := g.demoteVictim(); got != "" {
		t.Fatalf("victim %q with no demotable set, want empty", got)
	}
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("demote with every set inline or cold moved %d, want 0", n)
	}
}
