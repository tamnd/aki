package list

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The list demotion trigger (spec 2064/f3/06 section 6, trigger.go). The worker's
// demote loop drives one collection quantum per boundary through DemoteQuantum, and
// these tests hold the trigger's contract: it self-gates so a store under its
// resident budget or with no cold tier demotes nothing (the L9 zero-delta path), it
// sheds a real quantum once the footprint runs past the cap, it picks the largest
// resident list as the victim, and the DemoteQuantumOver form weighs a supplied extra
// footprint against the budget so the combined cap holds across types.

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

// addNativeList hangs a native band of n distinct w-byte elements off the registry
// under key and reconciles its footprint into the running total, the registry-backed
// list a trigger test drives. It returns the values so a read can be checked after a
// demote.
func addNativeList(t *testing.T, g *reg, key string, n, w int) (*native, [][]byte) {
	t.Helper()
	nt, want := coldTestNative(n, w)
	if nt.ring.n <= 2*demoteMargin {
		t.Fatalf("need at least one interior chunk to demote, got %d chunks", nt.ring.n)
	}
	l := &list{nat: nt, everLarge: true}
	g.m[key] = l
	g.note(l)
	return nt, want
}

// TestDemoteQuantumUnderBudgetNoOp holds the steady path: with the roomy cap the
// registry accounts but the footprint sits well under budget, so a demote quantum is
// a no-op that leaves the native band whole and the running total unmoved.
func TestDemoteQuantumUnderBudgetNoOp(t *testing.T) {
	cx, g := coldCtx(t)
	nt, _ := addNativeList(t, g, "k", 400, 40)
	before := g.resident

	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("under-budget demote shed %d chunks, want 0", n)
	}
	if nt.coldN != 0 {
		t.Fatal("under-budget demote sent chunks cold")
	}
	if g.resident != before {
		t.Fatalf("under-budget footprint changed %d -> %d", before, g.resident)
	}
}

// TestDemoteQuantumOverBudgetSheds holds the pressure path: a tiny cap the native
// heap already overruns makes the footprint over budget, so a demote quantum sheds
// the victim's interior chunks to the cold tier, drops the running total, and every
// element still reads back in order.
func TestDemoteQuantumOverBudgetSheds(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)
	nt, want := addNativeList(t, g, "k", 400, 40)
	if !cx.St.ResidentOverBy(g.resident) {
		t.Fatal("tiny-cap store should read over budget after the native list")
	}
	before := g.resident

	n := DemoteQuantum(cx)
	if n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	if nt.coldN != n {
		t.Fatalf("%d chunks cold, want the %d the demote reported", nt.coldN, n)
	}
	if g.resident >= before {
		t.Fatalf("footprint %d did not drop below the pre-demote %d", g.resident, before)
	}
	for i := range want {
		if got := nt.at(i); !bytes.Equal(got, want[i]) {
			t.Fatalf("at(%d) = %q, want %q after the trigger demote", i, got, want[i])
		}
	}
}

// TestDemoteQuantumColdOffZeroDelta is the L9 gate: a store with no cold tier turns
// accounting off, so the trigger returns zero however large the list grows and never
// sends a chunk cold. This is the byte-for-byte M0-M6 path.
func TestDemoteQuantumColdOffZeroDelta(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}
	nt, _ := addNativeList(t, g, "k", 400, 40)

	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("cold-off demote shed %d chunks, want 0", n)
	}
	if nt.coldN != 0 {
		t.Fatal("cold-off demote sent chunks cold")
	}
}

// TestDemoteQuantumNoRegistryNoOp holds the string-only shard: a cold store that
// never built a list registry carries no entry in the regs map, so the trigger loads
// nothing and returns zero without building one. The worker calls the hook
// unconditionally, so it must be safe before the first list command.
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
		t.Fatalf("no-registry demote shed %d chunks, want 0", n)
	}
	if _, ok := regs.Load(st); ok {
		t.Fatal("the trigger built a registry on a list-free shard")
	}
}

// TestDemoteVictimPicksLargest holds the victim policy: with several demotable lists
// the trigger picks the one with the largest resident footprint, the biggest
// immediate win, and the demote sends that list's interior chunks cold while the
// smaller ones stay resident.
func TestDemoteVictimPicksLargest(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)
	small, _ := addNativeList(t, g, "small", 200, 40)
	big, _ := addNativeList(t, g, "big", 900, 40)
	mid, _ := addNativeList(t, g, "mid", 400, 40)

	if got := g.demoteVictim(); got != "big" {
		t.Fatalf("victim %q, want the largest list \"big\"", got)
	}
	if n := DemoteQuantum(cx); n == 0 {
		t.Fatal("over-budget demote shed nothing")
	}
	if big.coldN == 0 {
		t.Fatal("the largest list sent nothing cold after being picked")
	}
	if small.coldN != 0 || mid.coldN != 0 {
		t.Fatal("a non-victim list sent chunks cold")
	}
}

// TestDemotableSkipsInlineAndFullyCold holds the demotable predicate that guards the
// victim pick: an inline listpack list (no native band) has nothing to shed, and a
// native band whose interior is already fully cold has nothing left either, so
// neither can win the pick and stall the loop.
func TestDemotableSkipsInlineAndFullyCold(t *testing.T) {
	cx, g := coldCtxCap(t, 4096)

	g.m["small"] = newList()
	g.note(g.m["small"])
	if g.m["small"].demotable() {
		t.Fatal("an inline listpack list reported demotable")
	}

	addNativeList(t, g, "k", 400, 40)
	if !g.m["k"].demotable() {
		t.Fatal("a native list with a resident interior reported not demotable")
	}
	// Drain the whole interior cold over as many quanta as it takes.
	for g.demote(cx, []byte("k")) > 0 {
	}
	if g.m["k"].demotable() {
		t.Fatal("a list with a fully-cold interior reported demotable")
	}
	if got := g.demoteVictim(); got != "" {
		t.Fatalf("victim %q with no demotable list, want empty", got)
	}
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("demote with every list inline or cold shed %d, want 0", n)
	}
}

// TestDemoteQuantumOverAddsExtraToBudget holds the combined-budget contract the
// dispatch fan-in relies on: the same registry that sits under its own budget goes
// over once an extra other-collection footprint is added, so DemoteQuantumOver sheds
// where DemoteQuantum would not. This is what keeps the one resident cap an RSS bound
// across the collection types together.
func TestDemoteQuantumOverAddsExtraToBudget(t *testing.T) {
	// A 1MB cap sits above the native list's own footprint but below it plus a 1MB
	// extra, so the same registry is under budget alone and over it combined.
	const cap = 1 << 20
	cx, g := coldCtxCap(t, cap)
	nt, _ := addNativeList(t, g, "k", 400, 40)
	own := g.resident
	if cx.St.ResidentOverBy(own) {
		t.Fatal("1MB-cap store should read under budget on the list alone")
	}

	// Its own budget leaves it resident.
	if n := DemoteQuantum(cx); n != 0 {
		t.Fatalf("own-budget demote shed %d, want 0", n)
	}
	if nt.coldN != 0 {
		t.Fatal("own-budget demote sent chunks cold")
	}

	// An extra footprint the size of the cap pushes the combined figure over, so it
	// sheds where its own budget would not.
	if n := DemoteQuantumOver(cx, cap); n == 0 {
		t.Fatal("combined-budget demote over the cap shed nothing")
	}
	if nt.coldN == 0 {
		t.Fatal("combined-budget demote sent nothing cold")
	}
}
