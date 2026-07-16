package stream

import (
	"path/filepath"
	"testing"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
)

// The collection resident-byte accounting (spec 2064/f3/06 section 6). A stream is
// a native-heap structure the store's arena budget cannot see: its entry blocks,
// the counted directory over them, and each consumer group's pending-entries ledger
// all live off the arena. The registry keeps a running sum of every live stream's
// footprint that the shard reads at a demote boundary without a walk. These tests
// hold two contracts: the running total stays exactly the walked sum across the
// append, band-crossing, trim, gc-reclaim, and group-delivery paths, and the whole
// machinery stays off (the total never leaves zero) when the store runs no cold
// tier, the L9 gate that keeps the M0-M6 stream matrix byte-for-byte.

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

// walkedResident is the running total's ground truth: the footprint of every stream
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

// mkFields builds the []field an XADD would parse from a name/value run.
func mkFields(kv ...string) []field {
	f := make([]field, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		f = append(f, field{name: []byte(kv[i]), value: []byte(kv[i+1])})
	}
	return f
}

// addEntry mirrors the XADD handler (commands.go Xadd): create the stream on first
// entry, append with a ready ID, then reconcile the footprint. It drives the
// append-note sequence the handler runs, without the RESP reply the handler frames.
func addEntry(g *reg, key string, ms uint64, kv ...string) *stream {
	s := g.m[key]
	if s == nil {
		s = newStream()
		g.m[key] = s
	}
	s.appendEntry(streamID{ms: ms}, mkFields(kv...))
	g.note(s)
	return s
}

// delEntry mirrors the XDEL handler: tombstone the entry, mark a native stream for
// the gc pass, then reconcile. A tombstone leaves the blob length unchanged, so the
// footprint holds until gcPass reclaims the block.
func delEntry(g *reg, key string, ms uint64) {
	s := g.m[key]
	if s.delete(streamID{ms: ms}) && s.kind == bandNative {
		g.markDirty(s)
	}
	g.note(s)
}

// gcPass mirrors the shard maintainer: run one gc pass over every dirtied stream and
// reconcile the freed bytes, the between-batches step that reclaims tombstoned
// sealed blocks.
func gcPass(g *reg) { g.maintain() }

// TestResidentAccountingTracksXadd walks one inline stream through the append path
// and holds the running total to the walked sum at every step: it starts zero, grows
// as entries arrive, and equals the single stream's footprint.
func TestResidentAccountingTracksXadd(t *testing.T) {
	_, g := coldCtx(t)

	if g.resident != 0 {
		t.Fatalf("fresh registry resident %d, want 0", g.resident)
	}

	addEntry(g, "k", 1, "f", "v")
	wantExact(t, g)
	if g.resident == 0 {
		t.Fatal("resident stayed zero after the first entry")
	}
	if g.resident != g.m["k"].residentBytes() {
		t.Fatalf("single-stream total %d != its footprint %d", g.resident, g.m["k"].residentBytes())
	}
	if g.m["k"].kind != bandInline {
		t.Fatalf("single-entry stream band %v, want inline", g.m["k"].kind)
	}

	// A second entry extends the inline block, so the total grows by the added frame
	// and keeps matching the walked sum.
	before := g.resident
	addEntry(g, "k", 2, "f", "v2")
	wantExact(t, g)
	if g.resident <= before {
		t.Fatalf("resident %d did not grow past %d after a second entry", g.resident, before)
	}
}

// TestResidentAccountingCrossesToNative pushes an inline stream past the entry cap so
// the append upgrades to the native band, and asserts the total jumps to include the
// counted directory the native band seeds and keeps the walked-sum invariant as the
// append log grows.
func TestResidentAccountingCrossesToNative(t *testing.T) {
	_, g := coldCtx(t)

	// Stay inline first: a few small entries well within the caps.
	for i := uint64(1); i <= 4; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	if g.m["k"].kind != bandInline {
		t.Fatalf("small stream band %v, want inline", g.m["k"].kind)
	}
	wantExact(t, g)
	inline := g.resident

	// Cross the inline entry cap: the append that would breach it upgrades the stream
	// one-way to the native band, which builds a counted directory the inline band had
	// no need for, so the footprint steps up past the plain inline blob.
	for i := uint64(5); i <= inlineMaxEntries+4; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	if g.m["k"].kind != bandNative {
		t.Fatalf("over-cap stream band %v, want native", g.m["k"].kind)
	}
	wantExact(t, g)
	if g.resident <= inline {
		t.Fatalf("native footprint %d not above inline %d", g.resident, inline)
	}

	// Keep appending across a block boundary so a second block opens and the directory
	// gains a key; the invariant still holds.
	for i := uint64(inlineMaxEntries + 5); i <= 400; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	if len(g.m["k"].blocks) < 2 {
		t.Fatalf("stream holds %d blocks, want the append log to have opened a second", len(g.m["k"].blocks))
	}
	wantExact(t, g)
}

// TestResidentAccountingReclaimsOnGc holds the invariant across the delete-then-gc
// path: an XDEL tombstones entries in a sealed native block without moving the blob
// bytes, so the total holds; the gc pass then re-encodes the block to its survivors,
// shrinking resBlob, and the total drops to match.
func TestResidentAccountingReclaimsOnGc(t *testing.T) {
	_, g := coldCtx(t)

	// Build a native stream with a full sealed block plus an open tail (128 entries
	// per fixed-schema block, so 300 seals the first two).
	for i := uint64(1); i <= 300; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	s := g.m["k"]
	if s.kind != bandNative {
		t.Fatalf("300-entry stream band %v, want native", s.kind)
	}
	wantExact(t, g)

	// Tombstone half of the first sealed block. The blob bytes stay put (a tombstone
	// flips a flag in place), so the running total does not move yet, but it stays
	// exact against the walked sum.
	before := g.resident
	for i := uint64(1); i <= 127; i += 2 {
		delEntry(g, "k", i)
	}
	wantExact(t, g)
	if g.resident != before {
		t.Fatalf("resident %d moved on tombstone-only deletes, want it held at %d", g.resident, before)
	}

	// The gc pass re-encodes the partially-dead sealed block to its live entries,
	// dropping the dead bytes from resBlob; the total falls and still equals the walked
	// sum.
	gcPass(g)
	wantExact(t, g)
	if g.resident >= before {
		t.Fatalf("resident %d did not shrink after gc reclaimed dead bytes, was %d", g.resident, before)
	}
}

// TestResidentAccountingTrimDropsBlocks holds the invariant across the approximate
// trim path, which drops whole front blocks: their entry bytes leave resBlob and the
// directory loses their keys, so the total falls to the surviving tail.
func TestResidentAccountingTrimDropsBlocks(t *testing.T) {
	_, g := coldCtx(t)

	for i := uint64(1); i <= 400; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	s := g.m["k"]
	if s.kind != bandNative {
		t.Fatalf("400-entry stream band %v, want native", s.kind)
	}
	wantExact(t, g)
	before := g.resident

	// An approximate MAXLEN trim drops whole front blocks (never a partial rewrite), so
	// the freed blob bytes leave the running total at the command boundary.
	removed := s.trim(trimSpec{kind: trimMaxlen, approx: true, maxlen: 128})
	g.note(s)
	if removed == 0 {
		t.Fatal("approximate trim to 128 dropped nothing from a 400-entry stream")
	}
	wantExact(t, g)
	if g.resident >= before {
		t.Fatalf("resident %d did not shrink after a whole-block trim, was %d", g.resident, before)
	}
}

// TestResidentAccountingGroupPEL holds the invariant across the consumer-group ledger
// paths: creating a group upgrades the stream and adds a group cell, a `>` delivery
// grows the pending-entries list, and an ack shrinks it. Each goes through note, so
// the running total tracks the auxiliary heap the entry blocks never see.
func TestResidentAccountingGroupPEL(t *testing.T) {
	cx, g := coldCtx(t)

	for i := uint64(1); i <= 8; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	s := g.m["k"]
	wantExact(t, g)
	beforeGroup := g.resident

	// XGROUP CREATE from 0 upgrades the stream to native (if it was inline) and records
	// a group with an empty PEL; the footprint gains the directory and the group cell.
	s.addGroup([]byte("gr"), newGroup(streamID{}, 0, true))
	g.note(s)
	wantExact(t, g)
	if g.resident <= beforeGroup {
		t.Fatalf("resident %d did not grow after a group was created, was %d", g.resident, beforeGroup)
	}
	beforeDeliver := g.resident

	// A `>` delivery hands every entry to the consumer and records one pending entry
	// each in the group PEL, which allocates its slab arena and id-ordered tree.
	grp := s.group([]byte("gr"))
	con := grp.ensureConsumer([]byte("c1"), cx.NowMs)
	if got := grp.deliverNew(s, con, -1, false, cx.NowMs); len(got) != 8 {
		t.Fatalf("delivered %d entries, want 8", len(got))
	}
	g.note(s)
	wantExact(t, g)
	if g.resident <= beforeDeliver {
		t.Fatalf("resident %d did not grow after the PEL filled, was %d", g.resident, beforeDeliver)
	}

	// Acking every pending entry retires them from the group ledger; the tree shrinks
	// and the total still equals the walked sum (a slab arena's capacity is sticky, so
	// the figure need not fall back to the pre-delivery value).
	for i := uint64(1); i <= 8; i++ {
		grp.ackOne(streamID{ms: i})
	}
	g.note(s)
	wantExact(t, g)
	if grp.pelCount != 0 {
		t.Fatalf("group still holds %d pending after acking every entry", grp.pelCount)
	}
}

// TestResidentAccountingOffWithoutColdTier is the L9 gate: a store with no cold
// region and no resident cap turns accounting off, so the running total never leaves
// zero no matter how the streams churn, and the shard-readable accessor reads zero.
// This is what keeps a store with no LTM configured byte-for-byte with M0.
func TestResidentAccountingOffWithoutColdTier(t *testing.T) {
	cx := &shard.Ctx{St: store.New(16<<20, 1<<20), NowMs: 1}
	g := registry(cx)
	if g.acctOn {
		t.Fatal("registry on a plain store should not account")
	}

	for i := uint64(1); i <= 300; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	if g.m["k"].kind != bandNative {
		t.Fatalf("300-entry stream band %v, want native", g.m["k"].kind)
	}
	if g.resident != 0 {
		t.Fatalf("resident %d with accounting off, want 0", g.resident)
	}
	if ResidentBytes(cx) != 0 {
		t.Fatalf("accessor %d with accounting off, want 0", ResidentBytes(cx))
	}
	delEntry(g, "k", 1)
	gcPass(g)
	if g.resident != 0 {
		t.Fatalf("resident %d after a delete with accounting off, want 0", g.resident)
	}
}

// TestResidentBytesAccessor covers the shard-readable seam: it returns the registry
// total on a cold-configured shard and zero on a shard that has never built a stream
// registry (the regs map still holds no entry for the store), so the demote loop can
// read it unconditionally.
func TestResidentBytesAccessor(t *testing.T) {
	cx, g := coldCtx(t)
	for i := uint64(1); i <= 64; i++ {
		addEntry(g, "k", i, "f", "v")
	}
	if got := ResidentBytes(cx); got != g.resident {
		t.Fatalf("accessor %d != registry total %d", got, g.resident)
	}

	var bare shard.Ctx
	if got := ResidentBytes(&bare); got != 0 {
		t.Fatalf("accessor on a bare ctx %d, want 0", got)
	}
}
