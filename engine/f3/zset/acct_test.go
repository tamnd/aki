package zset

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
)

// The collection resident-byte accounting (spec 2064/f3/06 section 6). A zset is
// a native-heap structure the store's arena budget cannot see, so the registry
// keeps a running sum of every live zset's footprint that the shard reads without
// a walk. These tests hold two contracts: the running total stays exactly the
// walked sum across the create, add, remove, drop, and store paths, and the whole
// machinery stays off (the total never leaves zero) when the store runs no cold
// tier, which is the L9 gate that keeps the M0-M6 zset matrix byte-for-byte.

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

// walkedResident is the running total's ground truth: the footprint of every zset
// the registry still holds, summed the slow way.
func walkedResident(g *reg) uint64 {
	var n uint64
	for _, z := range g.m {
		n += z.residentBytes()
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

// addKey mirrors the ZADD handler's discipline: create the zset on first member,
// update every pair, then reconcile the footprint. Each member's score is its
// index so the members stay distinct across the ladder.
func addKey(g *reg, key string, members ...string) {
	z := g.m[key]
	if z == nil {
		z = newZset()
		g.m[key] = z
	}
	for i, m := range members {
		z.update([]byte(m), float64(i), flags{})
	}
	g.note(z)
}

// remKey mirrors the ZREM handler: remove every member, then drop an emptied zset
// or reconcile a surviving one.
func remKey(g *reg, key string, members ...string) {
	z := g.m[key]
	for _, m := range members {
		z.rem([]byte(m))
	}
	if z.card() == 0 {
		g.drop([]byte(key))
	} else {
		g.note(z)
	}
}

// gen builds n distinct members of the given byte width, indexed from base: each
// member is the base-26 encoding of its global index, left-padded with 'a', so no
// two collide across calls with disjoint base ranges.
func gen(base, n, width int) []string {
	out := make([]string, n)
	for i := range out {
		b := make([]byte, width)
		for j := range b {
			b[j] = 'a'
		}
		v := base + i
		for p := width - 1; p >= 0 && v > 0; p-- {
			b[p] = 'a' + byte(v%26)
			v /= 26
		}
		out[i] = string(b)
	}
	return out
}

// TestResidentAccountingTracksAddsRemoves walks a single zset through its whole
// lifecycle and holds the running total to the walked sum at every step: it starts
// zero, grows as members arrive, keeps matching a surviving zset after a partial
// remove, and returns to zero when the last member leaves and the key drops.
func TestResidentAccountingTracksAddsRemoves(t *testing.T) {
	_, g := coldCtx(t)

	if g.resident != 0 {
		t.Fatalf("fresh registry resident %d, want 0", g.resident)
	}

	first := gen(0, 200, 12)
	addKey(g, "k", first...)
	wantExact(t, g)
	if g.resident == 0 {
		t.Fatal("resident stayed zero after 200 adds")
	}
	if g.resident != g.m["k"].residentBytes() {
		t.Fatalf("single-zset total %d != its footprint %d", g.resident, g.m["k"].residentBytes())
	}
	afterFirst := g.resident

	// More members can only grow the footprint (the backing capacities never
	// shrink on insert), and the total tracks it exactly.
	addKey(g, "k", gen(200, 200, 12)...)
	wantExact(t, g)
	if g.resident < afterFirst {
		t.Fatalf("resident shrank on more adds: %d < %d", g.resident, afterFirst)
	}

	// A partial remove keeps the key, so the total keeps matching the surviving
	// zset (the capacity is sticky until a rebuild, so the figure need not drop).
	remKey(g, "k", first...)
	wantExact(t, g)
	if _, ok := g.m["k"]; !ok {
		t.Fatal("key dropped by a partial remove")
	}

	// Removing the rest empties the zset: the key drops and the total returns to
	// zero, the gone zset's bytes fully reclaimed from the figure.
	rest := make([]string, 0, 400)
	g.m["k"].eachEntry(func(m []byte, _ uint64) { rest = append(rest, string(m)) })
	remKey(g, "k", rest...)
	if _, ok := g.m["k"]; ok {
		t.Fatal("key survived removing every member")
	}
	if g.resident != 0 {
		t.Fatalf("resident %d after draining the only key, want 0", g.resident)
	}
	wantExact(t, g)
}

// TestResidentAccountingGrowsAcrossEncodings pushes one zset over the listpack cap
// into the native band and asserts the total climbs into the denser
// representation and never loses the walked-sum invariant.
func TestResidentAccountingGrowsAcrossEncodings(t *testing.T) {
	_, g := coldCtx(t)

	// A handful of short members stays inline: the packed blob is the footprint.
	addKey(g, "k", gen(0, 8, 12)...)
	if enc := g.m["k"].enc; enc != encListpack {
		t.Fatalf("small zset enc %v, want listpack", enc)
	}
	wantExact(t, g)
	inline := g.resident

	// Cross the 128-entry listpack cap into the native band. The native tree, its
	// member hash, the record cells, and the member slab allocate strictly more
	// backing than the packed blob, so the accounted figure rises.
	addKey(g, "k", gen(8, 400, 12)...)
	if enc := g.m["k"].enc; enc != encSkiplist {
		t.Fatalf("408-member zset enc %v, want skiplist", enc)
	}
	wantExact(t, g)
	if g.resident <= inline {
		t.Fatalf("native footprint %d not above inline %d", g.resident, inline)
	}
}

// TestResidentAccountingStorePath drives the STORE placement (place, directly
// callable): a fresh result is accounted when installed, a replaced destination's
// bytes leave the total on the swap, and an empty result drops the key back to
// nothing.
func TestResidentAccountingStorePath(t *testing.T) {
	cx, g := coldCtx(t)

	// A pair of sources seeded straight into the map, footprints posted once to
	// bring the total in line before the store paths run.
	g.m["a"] = buildDest(scored("1", "2", "3", "4"))
	g.m["b"] = buildDest(scored("3", "4", "5", "6"))
	g.note(g.m["a"])
	g.note(g.m["b"])
	wantExact(t, g)

	// A union-shaped result into a new key: accounted on placement.
	place(cx, g, []byte("dest"), buildDest(scored("1", "2", "3", "4", "5", "6")))
	wantExact(t, g)
	if _, ok := g.m["dest"]; !ok {
		t.Fatal("store result not installed at dest")
	}

	// Overwriting dest: the replaced zset's bytes must leave the total before the
	// new result's are posted, so the figure still equals the walk.
	place(cx, g, []byte("dest"), buildDest(scored("3", "4")))
	wantExact(t, g)

	// An empty result deletes dest: its bytes leave the total.
	place(cx, g, []byte("dest"), buildDest(nil))
	if _, ok := g.m["dest"]; ok {
		t.Fatal("empty store result left dest present")
	}
	wantExact(t, g)
}

// scored turns members into ascending-scored pairs buildDest can materialize.
func scored(members ...string) []scoredMember {
	if len(members) == 0 {
		return nil
	}
	out := make([]scoredMember, len(members))
	for i, m := range members {
		out[i] = scoredMember{member: []byte(m), score: float64(i)}
	}
	return out
}

// TestResidentAccountingOffWithoutColdTier is the L9 gate: a store with no cold
// region and no resident cap turns accounting off, so the running total never
// leaves zero no matter how the zsets churn, and the shard-readable accessor reads
// zero. This is what keeps a store with no LTM configured byte-for-byte with M0.
func TestResidentAccountingOffWithoutColdTier(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	members := gen(0, 500, 16)
	addKey(g, "k", members...)
	if g.resident != 0 {
		t.Fatalf("resident %d with accounting off, want 0", g.resident)
	}
	if ResidentBytes(cx) != 0 {
		t.Fatalf("accessor %d with accounting off, want 0", ResidentBytes(cx))
	}
	remKey(g, "k", members[0])
	if g.resident != 0 {
		t.Fatalf("resident %d after a remove with accounting off, want 0", g.resident)
	}
}

// TestResidentBytesAccessor covers the shard-readable seam: it returns the
// registry total on a cold-configured shard and zero on a shard that has never
// built a zset registry (the ZColl slot still nil), so the demote loop can read it
// unconditionally.
func TestResidentBytesAccessor(t *testing.T) {
	cx, g := coldCtx(t)
	addKey(g, "k", gen(0, 64, 12)...)
	if got := ResidentBytes(cx); got != g.resident {
		t.Fatalf("accessor %d != registry total %d", got, g.resident)
	}

	var bare shard.Ctx
	if got := ResidentBytes(&bare); got != 0 {
		t.Fatalf("accessor on a bare ctx %d, want 0", got)
	}
}
