package hash

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The collection resident-byte accounting (spec 2064/f3/06 section 6). A hash is a
// native-heap structure the store's arena budget cannot see, so the registry keeps
// a running sum of every live hash's footprint that the shard reads without a walk.
// These tests hold two contracts: the running total stays exactly the walked sum
// across the set, overwrite, delete, field-TTL, and band-crossing paths, and the
// whole machinery stays off (the total never leaves zero) when the store runs no
// cold tier, the L9 gate that keeps the M0-M6 hash matrix byte-for-byte.

// coldCtx builds a cold-configured store (a value log, a cold region, and a
// resident cap), so ColdConfigured is true and the registry turns accounting on.
// The cap is large enough that nothing demotes during the test: accounting runs
// whether or not a demotion is pending, and these tests measure the figure, not the
// eviction it will later drive.
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

// walkedResident is the running total's ground truth: the footprint of every hash
// the registry still holds, summed the slow way.
func walkedResident(g *reg) uint64 {
	var n uint64
	for _, h := range g.m {
		n += h.residentBytes()
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

// setKey mirrors the HSET handler (commands.go Hset): create the hash on first
// field, write every field-value pair, then reconcile the footprint. It is the
// create-set-note sequence Hset runs, driven here without the RESP reply the
// handler needs.
func setKey(g *reg, key string, pairs ...string) {
	h := g.m[key]
	if h == nil {
		h = newHash()
		g.m[key] = h
	}
	for i := 0; i < len(pairs); i += 2 {
		h.set([]byte(pairs[i]), []byte(pairs[i+1]))
	}
	g.note(h)
}

// delKey mirrors the HDEL handler: remove one field, then drop an emptied hash or
// reconcile a surviving one.
func delKey(g *reg, key, field string) {
	h := g.m[key]
	h.del([]byte(field))
	if h.card() == 0 {
		g.drop([]byte(key))
	} else {
		g.note(h)
	}
}

// pairs builds n field-value pairs each padded to width w, distinct so a delete can
// target one field without disturbing its neighbours.
func fvPairs(prefix string, n, w int) []string {
	out := make([]string, 0, 2*n)
	for i := 0; i < n; i++ {
		f := fmt.Sprintf("%sf%d", prefix, i)
		v := fmt.Sprintf("%sv%d", prefix, i)
		for len(f) < w {
			f += "x"
		}
		for len(v) < w {
			v += "y"
		}
		out = append(out, f, v)
	}
	return out
}

// TestResidentAccountingTracksSetDel walks one hash through its whole lifecycle on
// the inline band and holds the running total to the walked sum at every step: it
// starts zero, grows as fields arrive, keeps matching a surviving hash after partial
// deletes, and returns to zero when the last field leaves and the key drops.
func TestResidentAccountingTracksSetDel(t *testing.T) {
	_, g := coldCtx(t)

	if g.resident != 0 {
		t.Fatalf("fresh registry resident %d, want 0", g.resident)
	}

	ps := fvPairs("", 40, 12)
	setKey(g, "k", ps...)
	wantExact(t, g)
	if g.resident == 0 {
		t.Fatal("resident stayed zero after 40 fields")
	}
	if g.resident != g.m["k"].residentBytes() {
		t.Fatalf("single-hash total %d != its footprint %d", g.resident, g.m["k"].residentBytes())
	}
	if enc := g.m["k"].enc; enc != encListpack {
		t.Fatalf("40-field hash enc %v, want listpack", enc)
	}

	// A partial delete keeps the key, so the total keeps matching the surviving hash
	// (an inline blob's capacity is sticky, so the figure need not shrink).
	for i := 0; i < 20; i++ {
		delKey(g, "k", ps[2*i])
	}
	wantExact(t, g)
	if _, ok := g.m["k"]; !ok {
		t.Fatal("key dropped by a partial delete")
	}

	// Deleting the rest empties the hash: the key drops and the total returns to
	// zero, the gone hash's bytes fully reclaimed from the figure.
	for i := 20; i < 40; i++ {
		delKey(g, "k", ps[2*i])
	}
	if _, ok := g.m["k"]; ok {
		t.Fatal("key survived deleting every field")
	}
	if g.resident != 0 {
		t.Fatalf("resident %d after draining the only key, want 0", g.resident)
	}
	wantExact(t, g)
}

// TestResidentAccountingCrossesToNative sets a value past the inline value cap so
// the write promotes to the native field table, and asserts the total jumps to the
// denser representation and keeps the walked-sum invariant through a native drain.
func TestResidentAccountingCrossesToNative(t *testing.T) {
	_, g := coldCtx(t)

	// Stay inline first: a handful of short fields well within the caps.
	setKey(g, "k", fvPairs("", 8, 10)...)
	if enc := g.m["k"].enc; enc != encListpack {
		t.Fatalf("small hash enc %v, want listpack", enc)
	}
	wantExact(t, g)
	inline := g.resident

	// A value past the 64-byte inline cap forces the one-way promotion to the native
	// table, whose record cells and table slots cost strictly more than the blob did.
	setKey(g, "k", "big", strings.Repeat("z", 100))
	if enc := g.m["k"].enc; enc != encHashtable {
		t.Fatalf("over-cap hash enc %v, want hashtable", enc)
	}
	wantExact(t, g)
	if g.resident <= inline {
		t.Fatalf("native footprint %d not above inline %d", g.resident, inline)
	}

	// Drain the native hash to empty: every delete reconciles, the key drops on the
	// last field, and the total lands back at zero. A native band never converts back
	// to inline (F4), so the drain stays on the table.
	var names []string
	g.m["k"].each(func(f, _ []byte) { names = append(names, string(f)) })
	for _, f := range names {
		delKey(g, "k", f)
	}
	if _, ok := g.m["k"]; ok {
		t.Fatal("native key survived a full drain")
	}
	if g.resident != 0 {
		t.Fatalf("resident %d after draining the native hash, want 0", g.resident)
	}
}

// TestResidentAccountingOverwritePath holds the invariant across value overwrites
// that grow the packed bytes on both bands: an in-place inline rewrite to a longer
// value, and a native overwrite that re-seats the value at the slab tail and charges
// the old bytes to the dead count.
func TestResidentAccountingOverwritePath(t *testing.T) {
	_, g := coldCtx(t)

	// Inline overwrite: replace a short value with a longer one (still under the
	// inline cap), which splices the blob and may grow its capacity.
	setKey(g, "k", "f", "v")
	wantExact(t, g)
	setKey(g, "k", "f", "a-longer-value-than-before")
	if enc := g.m["k"].enc; enc != encListpack {
		t.Fatalf("inline overwrite left enc %v, want listpack", enc)
	}
	wantExact(t, g)

	// Native overwrite: promote first with an over-cap value, then overwrite an
	// existing field with a longer value so the slab grows at the tail.
	setKey(g, "n", "big", strings.Repeat("z", 100))
	if enc := g.m["n"].enc; enc != encHashtable {
		t.Fatalf("promoted hash enc %v, want hashtable", enc)
	}
	setKey(g, "n", "f", "short")
	wantExact(t, g)
	setKey(g, "n", "f", strings.Repeat("w", 200))
	wantExact(t, g)
}

// TestResidentAccountingFieldTTL holds the invariant across the field-TTL paths that
// move the footprint: the first HEXPIRE on an inline hash flips it to the wider
// listpackex blob (eight bytes per entry), and a set-to-the-past expiry deletes a
// field. Both go through note, so the running total tracks the reshaped blob.
func TestResidentAccountingFieldTTL(t *testing.T) {
	_, g := coldCtx(t)

	setKey(g, "k", "f0", "v0", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4", "f5", "v5")
	wantExact(t, g)
	before := g.resident

	// The first field TTL flips the inline blob to listpackex, growing every entry by
	// an eight-byte expiry slot; note reconciles the wider blob.
	h := g.m["k"]
	if !h.setFieldExp([]byte("f0"), 5_000) {
		t.Fatal("setFieldExp on a present field reported absent")
	}
	g.note(h)
	wantExact(t, g)
	if g.resident <= before {
		t.Fatalf("listpackex footprint %d not above the plain-listpack %d", g.resident, before)
	}

	// A set-to-the-past expiry deletes the field on the next reap; the surviving hash
	// reconciles and the walked sum still matches.
	h.setFieldExp([]byte("f1"), 1)
	h.reap(uint64(2))
	if h.has([]byte("f1")) {
		t.Fatal("a fired field survived the reap")
	}
	g.note(h)
	wantExact(t, g)
}

// TestResidentAccountingOffWithoutColdTier is the L9 gate: a store with no cold
// region and no resident cap turns accounting off, so the running total never leaves
// zero no matter how the hashes churn, and the shard-readable accessor reads zero.
// This is what keeps a store with no LTM configured byte-for-byte with M0.
func TestResidentAccountingOffWithoutColdTier(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	setKey(g, "k", "big", strings.Repeat("z", 100))
	if enc := g.m["k"].enc; enc != encHashtable {
		t.Fatalf("over-cap hash enc %v, want hashtable", enc)
	}
	if g.resident != 0 {
		t.Fatalf("resident %d with accounting off, want 0", g.resident)
	}
	if ResidentBytes(cx) != 0 {
		t.Fatalf("accessor %d with accounting off, want 0", ResidentBytes(cx))
	}
	delKey(g, "k", "big")
	if g.resident != 0 {
		t.Fatalf("resident %d after a delete with accounting off, want 0", g.resident)
	}
}

// TestResidentBytesAccessor covers the shard-readable seam: it returns the registry
// total on a cold-configured shard and zero on a shard that has never built a hash
// registry (the regs map still holds no entry for the store), so the demote loop can
// read it unconditionally.
func TestResidentBytesAccessor(t *testing.T) {
	cx, g := coldCtx(t)
	setKey(g, "k", fvPairs("", 64, 12)...)
	if got := ResidentBytes(cx); got != g.resident {
		t.Fatalf("accessor %d != registry total %d", got, g.resident)
	}

	var bare shard.Ctx
	if got := ResidentBytes(&bare); got != 0 {
		t.Fatalf("accessor on a bare ctx %d, want 0", got)
	}
}
