package list

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The collection resident-byte accounting (spec 2064/f3/06 section 6). A list is
// a native-heap structure the store's arena budget cannot see, so the registry
// keeps a running sum of every live list's footprint that the shard reads without
// a walk. These tests hold two contracts: the running total stays exactly the
// walked sum across the push, pop, move, and interior-surgery paths on both bands,
// and the whole machinery stays off (the total never leaves zero) when the store
// runs no cold tier, the L9 gate that keeps the M0-M6 list matrix byte-for-byte.

// coldCtx builds a cold-configured store (a value log, a cold region, and a
// resident cap), so ColdConfigured is true and the registry turns accounting on.
// The cap is large enough that nothing demotes during the test: accounting runs
// whether or not a demotion is pending, and these tests measure the figure, not
// the eviction it will later drive.
func coldCtx(t *testing.T) (*shard.Ctx, *reg) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(store.Options{
		ArenaBytes:       16 << 20,
		SegBytes:         1 << 20,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: 1 << 30,
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

// walkedResident is the running total's ground truth: the footprint of every list
// the registry still holds, summed the slow way.
func walkedResident(g *reg) uint64 {
	var n uint64
	for _, l := range g.m {
		n += l.residentBytes()
	}
	return n
}

// wantExact fails unless the running total equals the walked sum, the invariant a
// mutating command restores before it returns.
func wantExact(t *testing.T, g *reg) {
	t.Helper()
	if got, want := g.resident, walkedResident(g); got != want {
		t.Fatalf("running total %d != walked sum %d", got, want)
	}
}

// pushKey mirrors the push handler's discipline (pushCmd): create the list on
// first element, push every value onto the chosen end, then reconcile the
// footprint. It is the create-push-note sequence LPUSH/RPUSH runs, driven here
// without the RESP reply the handler needs.
func pushKey(g *reg, key string, front bool, vals ...string) {
	l := g.m[key]
	if l == nil {
		l = newList()
		g.m[key] = l
	}
	for _, v := range vals {
		pushEnd(l, []byte(v), front)
	}
	g.note(l)
}

// popKey mirrors the pop handler (popCmd): pop one element off the chosen end,
// then drop an emptied list or reconcile a surviving one.
func popKey(g *reg, key string, front bool) {
	l := g.m[key]
	popEnd(l, front)
	if l.length() == 0 {
		g.drop([]byte(key))
	} else {
		g.note(l)
	}
}

// vals builds n distinct values each of width w, so LREM and LMOVE can target a
// single element without matching its neighbours.
func vals(prefix string, n, w int) []string {
	out := make([]string, n)
	for i := range out {
		s := fmt.Sprintf("%s%d", prefix, i)
		for len(s) < w {
			s += "x"
		}
		out[i] = s
	}
	return out
}

// TestResidentAccountingTracksPushPop walks one list through its whole lifecycle
// on the inline band and holds the running total to the walked sum at every step:
// it starts zero, grows as elements arrive, keeps matching a surviving list after
// partial pops, and returns to zero when the last element leaves and the key drops.
func TestResidentAccountingTracksPushPop(t *testing.T) {
	_, g := coldCtx(t)

	if g.resident != 0 {
		t.Fatalf("fresh registry resident %d, want 0", g.resident)
	}

	pushKey(g, "k", false, gen(40, 12)...)
	wantExact(t, g)
	if g.resident == 0 {
		t.Fatal("resident stayed zero after 40 pushes")
	}
	if g.resident != g.m["k"].residentBytes() {
		t.Fatalf("single-list total %d != its footprint %d", g.resident, g.m["k"].residentBytes())
	}

	// A partial drain keeps the key, so the total keeps matching the surviving
	// list (an inline blob's capacity is sticky, so the figure need not shrink).
	for i := 0; i < 20; i++ {
		popKey(g, "k", true)
	}
	wantExact(t, g)
	if _, ok := g.m["k"]; !ok {
		t.Fatal("key dropped by a partial drain")
	}

	// Popping the rest empties the list: the key drops and the total returns to
	// zero, the gone list's bytes fully reclaimed from the figure.
	for g.m["k"] != nil && g.m["k"].length() > 0 {
		popKey(g, "k", false)
	}
	if _, ok := g.m["k"]; ok {
		t.Fatal("key survived popping every element")
	}
	if g.resident != 0 {
		t.Fatalf("resident %d after draining the only key, want 0", g.resident)
	}
	wantExact(t, g)
}

// TestResidentAccountingCrossesToNative pushes one list past the listpack budget
// into the native chunked band and asserts the total jumps to the denser
// representation and keeps the walked-sum invariant through a native drain.
func TestResidentAccountingCrossesToNative(t *testing.T) {
	_, g := coldCtx(t)

	// Stay inline first: a small blob well within the 8 KiB budget.
	pushKey(g, "k", false, gen(20, 16)...)
	if enc := g.m["k"].encoding(); enc != encListpack {
		t.Fatalf("small list enc %v, want listpack", enc)
	}
	wantExact(t, g)
	inline := g.resident

	// Push far past the budget: the write that crosses it promotes to the native
	// ring, whose chunk slabs cost strictly more than the inline blob did.
	pushKey(g, "k", false, gen(400, 40)...)
	if enc := g.m["k"].encoding(); enc != encQuicklist {
		t.Fatalf("over-budget list enc %v, want quicklist", enc)
	}
	wantExact(t, g)
	if g.resident <= inline {
		t.Fatalf("native footprint %d not above inline %d", g.resident, inline)
	}

	// Drain the native list to empty: every pop reconciles, the key drops on the
	// last element, and the total lands back at zero.
	for g.m["k"] != nil && g.m["k"].length() > 0 {
		popKey(g, "k", true)
	}
	if _, ok := g.m["k"]; ok {
		t.Fatal("native key survived a full drain")
	}
	if g.resident != 0 {
		t.Fatalf("resident %d after draining the native list, want 0", g.resident)
	}
}

// TestResidentAccountingMovePath drives the LMOVE core (lmove.go, directly
// callable) and holds the invariant across a move that grows one key and shrinks
// another, and across the move that empties and drops the source.
func TestResidentAccountingMovePath(t *testing.T) {
	cx, g := coldCtx(t)

	pushKey(g, "src", false, "a", "b", "c")
	pushKey(g, "dst", false, "x")
	wantExact(t, g)

	// LMOVE src dst RIGHT LEFT: pop the source tail, push the destination head.
	if _, ok, wrong, _ := lmove(g, cx, []byte("src"), []byte("dst"), false, true); !ok || wrong {
		t.Fatalf("lmove c: ok=%v wrong=%v", ok, wrong)
	}
	wantExact(t, g)

	// Move the last two elements out of src: the final one empties and drops it,
	// so only dst's bytes remain and the total still matches the walk.
	for i := 0; i < 2; i++ {
		if _, ok, _, _ := lmove(g, cx, []byte("src"), []byte("dst"), false, true); !ok {
			t.Fatalf("lmove drain step %d did not move", i)
		}
	}
	if _, ok := g.m["src"]; ok {
		t.Fatal("src survived moving every element out")
	}
	wantExact(t, g)
	if g.resident != g.m["dst"].residentBytes() {
		t.Fatalf("total %d != dst footprint %d after draining src", g.resident, g.m["dst"].residentBytes())
	}
}

// TestResidentAccountingInteriorMutations drives LSET, LINSERT, LREM, and LTRIM
// on one list and holds the invariant across each: the interior surgeries can grow
// or shrink the packed bytes, and note reconciles the footprint at every boundary.
func TestResidentAccountingInteriorMutations(t *testing.T) {
	_, g := coldCtx(t)

	pushKey(g, "k", false, vals("m", 30, 20)...)
	wantExact(t, g)

	l := g.m["k"]

	// LSET rewrites one element in place; the list survives with a possibly
	// different byte layout, so note.
	l.setAt(5, []byte("a-much-longer-replacement-value-than-the-original"))
	g.note(l)
	wantExact(t, g)

	// LINSERT before a pivot grows the list by one; on a successful insert the key
	// survives with a grown footprint, so note.
	if !l.insert(true, []byte("m10xxxxxxxxxxxxxxxxx"), []byte("inserted")) {
		t.Fatal("pivot m10 not found for LINSERT")
	}
	g.note(l)
	wantExact(t, g)

	// LREM removes the matched element; the list stays non-empty, so note.
	if removed := l.remove(0, []byte("inserted")); removed != 1 {
		t.Fatalf("LREM removed %d, want 1", removed)
	}
	g.note(l)
	wantExact(t, g)

	// LTRIM keeps a middle window; the list survives, so note.
	l.trim(5, 20)
	if l.length() == 0 {
		g.drop([]byte("k"))
	} else {
		g.note(l)
	}
	wantExact(t, g)
	if _, ok := g.m["k"]; !ok {
		t.Fatal("LTRIM to a non-empty window dropped the key")
	}
}

// TestResidentAccountingOffWithoutColdTier is the L9 gate: a store with no cold
// region and no resident cap turns accounting off, so the running total never
// leaves zero no matter how the lists churn, and the shard-readable accessor reads
// zero. This is what keeps a store with no LTM configured byte-for-byte with M0.
func TestResidentAccountingOffWithoutColdTier(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	pushKey(g, "k", false, gen(400, 40)...)
	if enc := g.m["k"].encoding(); enc != encQuicklist {
		t.Fatalf("over-budget list enc %v, want quicklist", enc)
	}
	if g.resident != 0 {
		t.Fatalf("resident %d with accounting off, want 0", g.resident)
	}
	if ResidentBytes(cx) != 0 {
		t.Fatalf("accessor %d with accounting off, want 0", ResidentBytes(cx))
	}
	popKey(g, "k", true)
	if g.resident != 0 {
		t.Fatalf("resident %d after a pop with accounting off, want 0", g.resident)
	}
}

// TestResidentBytesAccessor covers the shard-readable seam: it returns the
// registry total on a cold-configured shard and zero on a shard that has never
// built a list registry (the regs map still holds no entry for the store), so the
// demote loop can read it unconditionally.
func TestResidentBytesAccessor(t *testing.T) {
	cx, g := coldCtx(t)
	pushKey(g, "k", false, gen(64, 12)...)
	if got := ResidentBytes(cx); got != g.resident {
		t.Fatalf("accessor %d != registry total %d", got, g.resident)
	}

	var bare shard.Ctx
	if got := ResidentBytes(&bare); got != 0 {
		t.Fatalf("accessor on a bare ctx %d, want 0", got)
	}
}
