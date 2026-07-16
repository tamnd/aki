package stream

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The stream demotion trigger (spec 2064/f3/06 section 6, trigger.go). The worker's
// demote loop drives one collection quantum per boundary through DemoteQuantum, and
// these tests hold the trigger's contract: it self-gates so a store under its resident
// budget or with no cold tier demotes nothing (the L9 zero-delta path), it sheds a real
// quantum once the footprint runs past the cap, it picks the largest resident stream as
// the victim, and the DemoteQuantumOver form weighs a supplied extra footprint against
// the budget so the combined cap holds across types.

// coldCtxCap builds a cold-configured store like coldCtx but with an explicit resident
// cap, so a test can push the arena plus the registry heap past the budget and watch
// the trigger fire.
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
// registry accounts but the footprint sits well under budget, so a demote quantum is a
// no-op that leaves the log whole and the running total unmoved.
func TestDemoteQuantumUnderBudgetNoOp(t *testing.T) {
	cx, g := coldCtx(t)
	s, _ := buildLog(t, g, 700)
	before := g.resident

	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("under-budget demote shed %d blocks, want 0", n)
	}
	if s.cold != nil {
		t.Fatal("under-budget demote built cold state")
	}
	if g.resident != before {
		t.Fatalf("under-budget footprint changed %d -> %d", before, g.resident)
	}
}

// TestDemoteQuantumOverBudgetSheds holds the pressure path: a tiny cap the log already
// overruns makes the footprint over budget, so a demote quantum sheds the victim's
// front blocks to the cold tier, drops the running total, and every entry still reads
// back byte-for-byte across the boundary.
func TestDemoteQuantumOverBudgetSheds(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)
	s, want := buildLog(t, g, 700)
	if !cx.St.ResidentOverBy(g.resident) {
		t.Fatal("tiny-cap store should read over budget after the native log")
	}
	before := g.resident

	n := DemoteQuantum(cx)
	if n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	if s.cold == nil {
		t.Fatal("over-budget demote built no cold state")
	}
	if g.resident >= before {
		t.Fatalf("footprint %d did not drop below the pre-demote %d", g.resident, before)
	}
	wantExact(t, g)
	sameFlat(t, want, snapshotAll(s))
}

// TestDemoteQuantumColdOffZeroDelta is the L9 gate: a store with no cold tier turns
// accounting off, so the trigger returns zero however long the log grows and never
// builds cold state. This is the byte-for-byte M0-M6 path.
func TestDemoteQuantumColdOffZeroDelta(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}
	s, _ := buildLog(t, g, 700)

	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("cold-off demote shed %d blocks, want 0", n)
	}
	if s.cold != nil {
		t.Fatal("cold-off demote built cold state")
	}
}

// TestDemoteQuantumNoRegistryNoOp holds the string-only shard: a cold store that never
// built a stream registry carries no entry in the regs map, so the trigger loads
// nothing and returns zero without building one. The worker calls the hook
// unconditionally, so it must be safe before the first stream command.
func TestDemoteQuantumNoRegistryNoOp(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         1 << 20,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: 4096,
	})
	if err != nil {
		t.Fatalf("open cold store: %v", err)
	}
	cx := &shard.Ctx{St: st, NowMs: 1}
	// No registry(cx) call, so regs holds no entry for this store.
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("no-registry demote shed %d blocks, want 0", n)
	}
	if _, ok := regs.Load(st); ok {
		t.Fatal("the trigger built a registry on a stream-free shard")
	}
}

// TestDemoteVictimPicksLargest holds the victim policy: with several demotable streams
// the trigger picks the one with the largest resident footprint, the biggest immediate
// win, and the demote sends that stream's front blocks cold while the smaller ones stay
// resident.
func TestDemoteVictimPicksLargest(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)
	for i := uint64(1); i <= 200; i++ {
		addEntry(g, "small", i, "f", "v")
	}
	for i := uint64(1); i <= 900; i++ {
		addEntry(g, "big", i, "f", "v")
	}
	for i := uint64(1); i <= 400; i++ {
		addEntry(g, "mid", i, "f", "v")
	}

	if got := g.demoteVictim(); got != "big" {
		t.Fatalf("victim %q, want the largest stream \"big\"", got)
	}
	if n := DemoteQuantum(cx); n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	if g.m["big"].cold == nil {
		t.Fatal("the largest stream sent nothing cold after being picked")
	}
	if g.m["small"].cold != nil || g.m["mid"].cold != nil {
		t.Fatal("a non-victim stream sent blocks cold")
	}
}

// TestDemotableSkipsInlineAndFullyCold holds the demotable predicate that guards the
// victim pick: an inline stream (one block below a chunk's worth) has nothing to shed,
// and a native band whose every front block ahead of the tail margin is already cold
// has nothing left either, so neither can win the pick and stall the loop.
func TestDemotableSkipsInlineAndFullyCold(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	addEntry(g, "small", 1, "f", "v")
	if g.m["small"].demotable() {
		t.Fatal("an inline stream reported demotable")
	}

	s, _ := buildLog(t, g, 700)
	if !s.demotable() {
		t.Fatal("a native log with resident front blocks reported not demotable")
	}
	// Drain every sheddable front block cold over as many quanta as it takes.
	for g.demote(cx, []byte("k")) > 0 {
	}
	if s.demotable() {
		t.Fatal("a log with every front block cold reported demotable")
	}
	if got := g.demoteVictim(); got != "" {
		t.Fatalf("victim %q with no demotable stream, want empty", got)
	}
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("demote with every stream inline or cold shed %d, want 0", n)
	}
}

// TestDemoteQuantumOverAddsExtraToBudget holds the combined-budget contract the dispatch
// fan-in relies on: the same registry that sits under its own budget goes over once an
// extra other-collection footprint is added, so DemoteQuantumOver sheds where
// DemoteQuantum would not. This is what keeps the one resident cap an RSS bound across
// the collection types together.
func TestDemoteQuantumOverAddsExtraToBudget(t *testing.T) {
	// A 1MB cap sits above the native log's own footprint but below it plus a 1MB
	// extra, so the same registry is under budget alone and over it combined.
	const cap = 1 << 20
	cx, g := coldCtxCap(t, cap)
	s, _ := buildLog(t, g, 700)
	own := g.resident
	if cx.St.ResidentOverBy(own) {
		t.Fatal("1MB-cap store should read under budget on the log alone")
	}

	// Its own budget leaves it resident.
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("own-budget demote shed %d, want 0", n)
	}
	if s.cold != nil {
		t.Fatal("own-budget demote built cold state")
	}

	// An extra footprint the size of the cap pushes the combined figure over, so it
	// sheds where its own budget would not.
	if n := DemoteQuantumOver(cx, cap); n == 0 {
		t.Fatal("combined-budget demote over the cap shed nothing")
	}
	if s.cold == nil {
		t.Fatal("combined-budget demote built no cold state")
	}
}
