package zset

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The zset demotion trigger (spec 2064/f3/06 section 6, trigger.go). The worker's
// demote loop drives one collection quantum per boundary through DemoteQuantum, and
// these tests hold the trigger's contract: it self-gates so a store under its
// resident budget or with no cold tier demotes nothing (the L9 zero-delta path), it
// sheds a real quantum once the footprint runs past the cap, it picks the largest
// resident zset as the victim, and the DemoteQuantumOver form weighs a supplied
// extra footprint against the budget so the combined cap holds across types.

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
// registry accounts but the footprint sits well under budget, so a demote quantum is
// a no-op that leaves the native band whole and the running total unmoved.
func TestDemoteQuantumUnderBudgetNoOp(t *testing.T) {
	cx, g := coldCtx(t)

	addKey(g, "k", gen(0, 1000, 12)...)
	if enc := g.m["k"].enc; enc != encSkiplist {
		t.Fatalf("1000-member zset enc %v, want skiplist", enc)
	}
	before := g.resident

	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("under-budget demote moved %d members, want 0", n)
	}
	if coldTiers(g.m["k"].nat) != 0 {
		t.Fatal("under-budget demote sent members cold")
	}
	if g.resident != before {
		t.Fatalf("under-budget footprint changed %d -> %d", before, g.resident)
	}
	wantExact(t, g)
}

// TestDemoteQuantumOverBudgetSheds holds the pressure path: a tiny cap the arena
// already overruns makes the footprint over budget, so a demote quantum packs the
// victim's coldest members to the cold tier, drops the running total, and every
// member still reads back.
func TestDemoteQuantumOverBudgetSheds(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	members := gen(0, 1000, 12)
	addKey(g, "k", members...)
	if !cx.St.ResidentOverBy(g.resident) {
		t.Fatal("tiny-cap store should read over budget after 1000 adds")
	}
	before := g.resident

	n := DemoteQuantum(cx)
	if n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	z := g.m["k"]
	if coldTiers(z.nat) != n {
		t.Fatalf("%d members cold, want the %d the demote reported", coldTiers(z.nat), n)
	}
	if g.resident >= before {
		t.Fatalf("footprint %d did not drop below the pre-demote %d", g.resident, before)
	}
	for _, m := range members {
		if _, ok := z.score([]byte(m)); !ok {
			t.Fatalf("member %q lost after the trigger demote", m)
		}
	}
	wantExact(t, g)
}

// TestDemoteQuantumColdOffZeroDelta is the L9 gate: a store with no cold tier turns
// accounting off, so the trigger returns zero however large the zset grows and never
// sends a member cold. This is the byte-for-byte M0-M6 path.
func TestDemoteQuantumColdOffZeroDelta(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	addKey(g, "k", gen(0, 1000, 12)...)
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("cold-off demote moved %d members, want 0", n)
	}
	if coldTiers(g.m["k"].nat) != 0 {
		t.Fatal("cold-off demote sent members cold")
	}
}

// TestDemoteQuantumNoRegistryNoOp holds the string-only shard: a Ctx that never
// built a zset registry (ZColl still nil) reads no reg from the slot, so the trigger
// returns zero without a panic. The worker calls the hook unconditionally, so it
// must be safe before the first ZADD.
func TestDemoteQuantumNoRegistryNoOp(t *testing.T) {
	cx, _ := coldCtxCap(t, 4096)
	cx.ZColl = nil
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("nil-registry demote moved %d members, want 0", n)
	}
}

// TestDemoteVictimPicksLargest holds the victim policy: with several demotable zsets
// the trigger picks the one with the largest resident footprint, the biggest
// immediate win, and the demote sends that zset's members cold while the smaller
// ones stay resident.
func TestDemoteVictimPicksLargest(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	addKey(g, "small", gen(0, 200, 12)...)
	addKey(g, "big", gen(2000, 3000, 12)...)
	addKey(g, "mid", gen(6000, 800, 12)...)

	if got := g.demoteVictim(); got != "big" {
		t.Fatalf("victim %q, want the largest zset \"big\"", got)
	}

	if n := DemoteQuantum(cx); n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	if coldTiers(g.m["big"].nat) == 0 {
		t.Fatal("the largest zset sent nothing cold after being picked")
	}
	if coldTiers(g.m["small"].nat) != 0 || coldTiers(g.m["mid"].nat) != 0 {
		t.Fatal("a non-victim zset sent members cold")
	}
	wantExact(t, g)
}

// TestDemotableSkipsListpackAndFullyCold holds the demotable predicate that guards
// the victim pick: a listpack zset (below one chunk's worth) has nothing to shed,
// and a native band whose every member already left for the cold tier has nothing
// left either, so neither can win the pick and stall the loop.
func TestDemotableSkipsListpackAndFullyCold(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	addKey(g, "small", gen(0, 8, 12)...)
	if g.m["small"].enc != encListpack {
		t.Fatalf("8-member zset enc %v, want listpack", g.m["small"].enc)
	}
	if g.m["small"].demotable() {
		t.Fatal("a listpack zset reported demotable")
	}

	members := gen(100, 1000, 12)
	addKey(g, "k", members...)
	if !g.m["k"].demotable() {
		t.Fatal("a native zset with a live slab reported not demotable")
	}
	// Drain the whole band cold over as many quanta as it takes.
	for g.demote(cx, []byte("k"), demoteQuantum) > 0 {
	}
	if g.m["k"].demotable() {
		t.Fatal("a fully-cold zset reported demotable")
	}
	if got := g.demoteVictim(); got != "" {
		t.Fatalf("victim %q with no demotable zset, want empty", got)
	}
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("demote with every zset listpack or cold moved %d, want 0", n)
	}
}

// TestDemoteQuantumOverAddsExtraToBudget holds the combined-budget contract the
// dispatch fan-in relies on: the same registry that sits under its own budget goes
// over once an extra other-collection footprint is added, so DemoteQuantumOver sheds
// where DemoteQuantum would not. This is what keeps the one resident cap an RSS bound
// across the set and the zset together.
func TestDemoteQuantumOverAddsExtraToBudget(t *testing.T) {
	// A 1MB cap sits above the 1000-member zset's own footprint but below it plus a
	// 1MB extra, so the same registry is under budget alone and over it combined.
	const cap = 1 << 20
	cx, g := coldCtxCap(t, cap)
	addKey(g, "k", gen(0, 1000, 12)...)
	own := g.resident
	if cx.St.ResidentOverBy(own) {
		t.Fatal("1MB-cap store should read under budget on the zset alone")
	}

	// Its own budget leaves it resident.
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("own-budget demote moved %d, want 0", n)
	}
	if coldTiers(g.m["k"].nat) != 0 {
		t.Fatal("own-budget demote sent members cold")
	}

	// An extra footprint the size of the cap pushes the combined figure over, so it
	// sheds where its own budget would not.
	if n := DemoteQuantumOver(cx, cap); n == 0 {
		t.Fatal("combined-budget demote over the cap shed nothing")
	}
	if coldTiers(g.m["k"].nat) == 0 {
		t.Fatal("combined-budget demote sent nothing cold")
	}
	wantExact(t, g)
}
