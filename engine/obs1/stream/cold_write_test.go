package stream

import "testing"

// The stream cold write paths (spec 2064/f3/06 section 7.3, plan
// M7-slice-cold-chunk-stream, slice E). A demoted block keeps its header and master
// schema resident but drops its blob, so a read preads it (slice D). A write cannot
// mutate a blob that is not there: an XDEL or an exact-XTRIM boundary tombstone flips
// a flag byte in place, so each promotes the whole block it lands on first, the
// unconditional bring-up of section 7.3. An approximate XTRIM instead drops whole cold
// front blocks by handle with no pread, forgetting their descriptors and orphaning the
// frames. And the gc pass, which rewrites partially-dead sealed blocks, must skip a
// cold block rather than walk its absent blob. These tests hold each of those.

// TestXDelPromotesColdBlock deletes an entry that lives in a demoted block and asserts
// the block comes resident, its demote descriptor is dropped, the live count falls by
// one, and the log reads back as the pre-demote snapshot minus the deleted entry.
func TestXDelPromotesColdBlock(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700
	s, want := buildLog(t, g, n)

	if s.demote(cx.St, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	g.note(s)
	if !s.blocks[0].cold() {
		t.Fatal("front block did not demote")
	}
	descBefore := s.cold.dir.Len()

	// want[0] is the first entry of the front block, which is now cold; deleting it must
	// bring the whole block up before the flag write.
	target := want[0].id
	delEntry(g, "k", target.ms)

	if s.blocks[0].cold() {
		t.Fatal("front block still cold after an XDEL landed in it")
	}
	if s.blocks[0].blob == nil {
		t.Fatal("promoted front block has no resident blob")
	}
	if got := s.cold.dir.Len(); got != descBefore-1 {
		t.Fatalf("demote directory holds %d descriptors after promote, want %d", got, descBefore-1)
	}
	if s.length != n-1 {
		t.Fatalf("length %d after one XDEL, want %d", s.length, n-1)
	}
	wantExact(t, g)

	sameFlat(t, want[1:], snapshotAll(s))
}

// TestExactTrimPromotesColdBoundary runs an exact MINID trim whose window drops two
// whole cold front blocks and then tombstones the overshoot in the next block, which
// is itself cold. The whole-block drops forget their descriptors with no pread; the
// boundary block promotes so its overshoot can be tombstoned in place.
func TestExactTrimPromotesColdBoundary(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700 // blocks seal every 128 entries: ms 1..128, 129..256, 257..384, ...
	s, want := buildLog(t, g, n)

	if s.demote(cx.St, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	g.note(s)
	descBefore := s.cold.dir.Len()
	if descBefore < 3 {
		t.Fatalf("demote shed %d blocks, want at least 3 for this trim", descBefore)
	}

	// MINID 300 drops the two whole front blocks (ms 1..256), then tombstones ms 257..299
	// in the third block, which is cold, so the boundary path promotes it.
	removed := s.trim(trimSpec{kind: trimMinid, minid: streamID{ms: 300}})
	g.note(s)

	const dropped = 256 // two full front blocks
	const boundary = 43 // ms 257..299
	if removed != dropped+boundary {
		t.Fatalf("exact MINID removed %d, want %d", removed, dropped+boundary)
	}
	if s.blocks[0].cold() {
		t.Fatal("boundary block still cold after the exact trim tombstoned it")
	}
	if got := s.cold.dir.Len(); got != descBefore-3 {
		t.Fatalf("demote directory holds %d descriptors, want %d (two drops plus one promote)", got, descBefore-3)
	}
	if s.length != n-uint64(removed) {
		t.Fatalf("length %d, want %d", s.length, n-uint64(removed))
	}
	wantExact(t, g)

	// Everything from ms 300 up survives, in order, byte-for-byte.
	sameFlat(t, want[299:], snapshotAll(s))
}

// TestApproxTrimDropsColdFrontByHandle runs an approximate MINID trim that drops whole
// cold front blocks. It removes them by handle with no pread and forgets their
// descriptors, leaves the boundary block cold (approximate mode keeps its overshoot),
// and reads the survivors back.
func TestApproxTrimDropsColdFrontByHandle(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700
	s, want := buildLog(t, g, n)

	if s.demote(cx.St, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	g.note(s)
	descBefore := s.cold.dir.Len()

	// Approximate MINID 300 drops the two whole front blocks (ms 1..256) and stops; the
	// boundary block (ms 257..384) keeps its ms 257..299 overshoot and stays cold.
	removed := s.trim(trimSpec{kind: trimMinid, approx: true, minid: streamID{ms: 300}})
	g.note(s)

	const dropped = 256
	if removed != dropped {
		t.Fatalf("approx MINID removed %d, want the two whole front blocks (%d)", removed, dropped)
	}
	if !s.blocks[0].cold() {
		t.Fatal("approx trim promoted the boundary block, want it left cold and un-preaded")
	}
	if got := s.cold.dir.Len(); got != descBefore-2 {
		t.Fatalf("demote directory holds %d descriptors, want %d after two handle drops", got, descBefore-2)
	}
	if s.length != n-dropped {
		t.Fatalf("length %d, want %d", s.length, n-dropped)
	}
	wantExact(t, g)

	sameFlat(t, want[256:], snapshotAll(s))
}

// TestGcSkipsColdBlockCarryingTombstones tombstones most of the front block past the gc
// rewrite ratio while it is resident, then demotes it so it carries deleted>0 with no
// resident blob, then runs a gc pass. gc must skip the cold block rather than rewrite
// its absent blob (which would panic), leaving it cold with the log intact.
func TestGcSkipsColdBlockCarryingTombstones(t *testing.T) {
	cx, g := coldCtx(t)
	const n = 700
	s, want := buildLog(t, g, n)

	// Tombstone the front block's oldest entries past the 0.5 rewrite ratio, but do not
	// run gc yet, so the block keeps its dead bytes and stays resident.
	const dead = 70 // 70/128 is over the ratio, so a gc on the resident block would rewrite it
	for ms := uint64(1); ms <= dead; ms++ {
		delEntry(g, "k", ms)
	}
	if s.blocks[0].deleted != dead {
		t.Fatalf("front block deleted %d, want %d", s.blocks[0].deleted, dead)
	}

	// Demote sheds the front block cold with its tombstones intact.
	if s.demote(cx.St, []byte("k")) == 0 {
		t.Fatal("demote shed nothing")
	}
	g.note(s)
	if !s.blocks[0].cold() {
		t.Fatal("front block did not demote")
	}

	// The stream is still dirty from the deletes; a gc pass must skip the cold block, not
	// walk its nil blob.
	gcPass(g)

	if !s.blocks[0].cold() {
		t.Fatal("gc rewrote or promoted the cold block, want it left cold")
	}
	if s.length != n-dead {
		t.Fatalf("length %d, want %d", s.length, n-dead)
	}
	wantExact(t, g)

	sameFlat(t, want[dead:], snapshotAll(s))
}
