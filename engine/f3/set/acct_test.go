package set

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The collection resident-byte accounting (spec 2064/f3/06 section 6). A set is a
// native-heap structure the store's arena budget cannot see, so the registry
// keeps a running sum of every live set's footprint that the shard reads without a
// walk. These tests hold two contracts: the running total stays exactly the walked
// sum across the create, add, remove, drop, move, and store paths, and the whole
// machinery stays off (the total never leaves zero) when the store runs no cold
// tier, which is the L9 gate that keeps the M0-M6 set matrix byte-for-byte.

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

// walkedResident is the running total's ground truth: the footprint of every set
// the registry still holds, summed the slow way.
func walkedResident(g *reg) uint64 {
	var n uint64
	for _, s := range g.m {
		n += s.residentBytes()
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

// addKey mirrors the SADD handler's discipline: create the set on first member,
// add every member, then reconcile the footprint. It is the exact create-add-note
// sequence Sadd runs, driven here without the RESP reply the handler needs.
func addKey(g *reg, key string, members ...string) {
	s := g.m[key]
	if s == nil {
		s = newSet([]byte(members[0]))
		g.m[key] = s
	}
	for _, m := range members {
		s.add([]byte(m))
	}
	g.note(s)
}

// remKey mirrors the SREM handler: remove every member, then drop an emptied set
// or reconcile a surviving one.
func remKey(g *reg, key string, members ...string) {
	s := g.m[key]
	for _, m := range members {
		s.rem([]byte(m))
	}
	if s.card() == 0 {
		g.drop([]byte(key))
	} else {
		g.note(s)
	}
}

// TestResidentAccountingTracksAddsRemoves walks a single set through its whole
// lifecycle and holds the running total to the walked sum at every step: it starts
// zero, grows as members arrive, keeps matching a surviving set after a partial
// remove, and returns to zero when the last member leaves and the key drops.
func TestResidentAccountingTracksAddsRemoves(t *testing.T) {
	_, g := coldCtx(t)

	if g.resident != 0 {
		t.Fatalf("fresh registry resident %d, want 0", g.resident)
	}

	first := gen("m", 0, 200, 12)
	addKey(g, "k", first...)
	wantExact(t, g)
	if g.resident == 0 {
		t.Fatal("resident stayed zero after 200 adds")
	}
	if g.resident != g.m["k"].residentBytes() {
		t.Fatalf("single-set total %d != its footprint %d", g.resident, g.m["k"].residentBytes())
	}
	afterFirst := g.resident

	// More members can only grow the footprint (the backing capacities never
	// shrink on insert), and the total tracks it exactly.
	addKey(g, "k", gen("m", 200, 400, 12)...)
	wantExact(t, g)
	if g.resident < afterFirst {
		t.Fatalf("resident shrank on more adds: %d < %d", g.resident, afterFirst)
	}

	// A partial remove keeps the key, so the total keeps matching the surviving
	// set (the capacity is sticky until compaction, so the figure need not drop).
	remKey(g, "k", first...)
	wantExact(t, g)
	if _, ok := g.m["k"]; !ok {
		t.Fatal("key dropped by a partial remove")
	}

	// Removing the rest empties the set: the key drops and the total returns to
	// zero, the gone set's bytes fully reclaimed from the figure.
	rest := make([]string, 0, 400)
	g.m["k"].each(func(m []byte) { rest = append(rest, string(m)) })
	remKey(g, "k", rest...)
	if _, ok := g.m["k"]; ok {
		t.Fatal("key survived removing every member")
	}
	if g.resident != 0 {
		t.Fatalf("resident %d after draining the only key, want 0", g.resident)
	}
	wantExact(t, g)
}

// TestResidentAccountingGrowsAcrossEncodings pushes one set up the band ladder
// (intset to native to partitioned) and asserts the total climbs with each denser
// representation and never loses the walked-sum invariant.
func TestResidentAccountingGrowsAcrossEncodings(t *testing.T) {
	// Threshold above the 512 intset cap so there is a stable native-table window
	// between the intset and the partitioned band to observe.
	withThreshold(t, 4096)
	_, g := coldCtx(t)

	addKey(g, "k", intGen(0, 8)...)
	if enc := g.m["k"].enc; enc != encIntset {
		t.Fatalf("small integer set enc %v, want intset", enc)
	}
	wantExact(t, g)
	intset := g.resident

	// Cross the intset cap into the native table, then past the lowered partition
	// threshold into the partitioned band. Each transition allocates strictly more
	// backing than the last, so the accounted figure rises.
	addKey(g, "k", intGen(8, 1000)...)
	if enc := g.m["k"].enc; enc != encHashtable {
		t.Fatalf("1008-member set enc %v, want hashtable", enc)
	}
	wantExact(t, g)
	native := g.resident
	if native <= intset {
		t.Fatalf("native footprint %d not above intset %d", native, intset)
	}

	addKey(g, "k", intGen(1008, 4000)...)
	if enc := g.m["k"].enc; enc != encPartitioned {
		t.Fatalf("5008-member set enc %v, want partitioned", enc)
	}
	wantExact(t, g)
	if g.resident <= native {
		t.Fatalf("partitioned footprint %d not above native %d", g.resident, native)
	}
}

// TestResidentAccountingMovePath drives the SMOVE core (smove.go, directly
// callable) and holds the invariant across a move that shrinks one key and grows
// another, and across the move that empties and drops the source.
func TestResidentAccountingMovePath(t *testing.T) {
	cx, g := coldCtx(t)

	addKey(g, "src", "a", "b", "c")
	addKey(g, "dst", "x")
	wantExact(t, g)

	moved, wrong := smove(g, cx, []byte("src"), []byte("dst"), []byte("a"))
	if !moved || wrong {
		t.Fatalf("smove a: moved=%v wrong=%v", moved, wrong)
	}
	wantExact(t, g)

	// Move the last two members out of src: the final one empties and drops it, so
	// only dst's bytes remain and the total still matches the walk.
	for _, m := range []string{"b", "c"} {
		if moved, _ := smove(g, cx, []byte("src"), []byte("dst"), []byte(m)); !moved {
			t.Fatalf("smove %s did not move", m)
		}
	}
	if _, ok := g.m["src"]; ok {
		t.Fatal("src survived moving every member out")
	}
	wantExact(t, g)
	if g.resident != g.m["dst"].residentBytes() {
		t.Fatalf("total %d != dst footprint %d after draining src", g.resident, g.m["dst"].residentBytes())
	}
}

// TestResidentAccountingStorePath drives the STORE placement (setstore.go place,
// directly callable): a fresh result is accounted when installed, a replaced
// destination's bytes leave the total on the swap, and an empty result drops the
// key back to nothing.
func TestResidentAccountingStorePath(t *testing.T) {
	cx, g := coldCtx(t)

	g.m["a"] = setFrom([]string{"1", "2", "3", "4"})
	g.m["b"] = setFrom([]string{"3", "4", "5", "6"})
	// The two sources were seeded straight into the map, so post their footprints
	// once to bring the total in line before the store paths run.
	g.note(g.m["a"])
	g.note(g.m["b"])
	wantExact(t, g)

	// SUNIONSTORE into a new key: the result is accounted on placement.
	union := storeResult(totalCard([]*set{g.m["a"], g.m["b"]}), func(e func([]byte)) {
		unionInto([]*set{g.m["a"], g.m["b"]}, e)
	})
	place(cx, g, []byte("dest"), union, "sunionstore")
	wantExact(t, g)
	if _, ok := g.m["dest"]; !ok {
		t.Fatal("union result not installed at dest")
	}

	// SINTERSTORE overwriting dest: the replaced set's bytes must leave the total
	// before the new result's are posted, so the figure still equals the walk.
	inter := storeResult(minCard([]*set{g.m["a"], g.m["b"]}), func(e func([]byte)) {
		sinter(cx, []*set{g.m["a"], g.m["b"]}, e)
	})
	place(cx, g, []byte("dest"), inter, "sinterstore")
	wantExact(t, g)

	// An empty result deletes dest: its bytes leave the total.
	empty := storeResult(minCard([]*set{setFrom([]string{"9"}), setFrom([]string{"8"})}), func(e func([]byte)) {
		sinter(cx, []*set{setFrom([]string{"9"}), setFrom([]string{"8"})}, e)
	})
	place(cx, g, []byte("dest"), empty, "sinterstore")
	if _, ok := g.m["dest"]; ok {
		t.Fatal("empty store result left dest present")
	}
	wantExact(t, g)
}

// TestResidentAccountingOffWithoutColdTier is the L9 gate: a store with no cold
// region and no resident cap turns accounting off, so the running total never
// leaves zero no matter how the sets churn, and the shard-readable accessor reads
// zero. This is what keeps a store with no LTM configured byte-for-byte with M0.
func TestResidentAccountingOffWithoutColdTier(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	addKey(g, "k", gen("m", 0, 500, 16)...)
	if g.resident != 0 {
		t.Fatalf("resident %d with accounting off, want 0", g.resident)
	}
	if ResidentBytes(cx) != 0 {
		t.Fatalf("accessor %d with accounting off, want 0", ResidentBytes(cx))
	}
	remKey(g, "k", "m0xxxxxxxxxxxxx")
	if g.resident != 0 {
		t.Fatalf("resident %d after a remove with accounting off, want 0", g.resident)
	}
}

// TestResidentBytesAccessor covers the shard-readable seam: it returns the
// registry total on a cold-configured shard and zero on a shard that has never
// built a set registry (the Coll slot still nil), so the demote loop can read it
// unconditionally.
func TestResidentBytesAccessor(t *testing.T) {
	cx, g := coldCtx(t)
	addKey(g, "k", gen("m", 0, 64, 12)...)
	if got := ResidentBytes(cx); got != g.resident {
		t.Fatalf("accessor %d != registry total %d", got, g.resident)
	}

	var bare shard.Ctx
	if got := ResidentBytes(&bare); got != 0 {
		t.Fatalf("accessor on a bare ctx %d, want 0", got)
	}
}
