package store

import (
	"bytes"
	"testing"
)

// bigVal builds a deterministic value of n bytes, distinguishable per key.
func bigVal(seed byte, n int) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = seed + byte(i*7)
	}
	return v
}

// drivePtrDrain runs one deep drain cycle end to end and returns the drain,
// nil once nothing stages. It pins the fold-tap contract on every pass: a
// staged frame is never a pointer, whatever band the record came from,
// because the fold packs frames into segments that other nodes read and a
// band pointer only resolves against this shard's logs.
func drivePtrDrain(t *testing.T, s *Store, bufCap int) *coldDrain {
	t.Helper()
	d := s.StageColdDrainDeep(make([]byte, 0, bufCap))
	if d == nil || len(d.flips) == 0 {
		return nil
	}
	if err := WalkStagedFrames(d.buf, func(f FoldFrame) error {
		if f.Pointer {
			t.Fatalf("staged frame for %q is a pointer band", f.Key)
		}
		return nil
	}); err != nil {
		t.Fatalf("staged buffer walk: %v", err)
	}
	nw, err := s.ColdWriteAt(d.Off(), d.Buf())
	if err != nil || nw != len(d.Buf()) {
		t.Fatalf("cold write: n=%d err=%v, want n=%d", nw, err, len(d.Buf()))
	}
	s.CompleteColdDrain(d, true)
	return d
}

// TestColdDrainStagesPointerBands is the strings cold form (spec 2064/obs1
// doc 08 section 2): separated and chunked records enter the staged drain
// with their value bytes resolved into the frame, the flip releases their
// runs, and every key reads its full value back from the cold frame. The
// mix covers arena runs (written under the cap) and log runs (written past
// it), and the drain runs to exhaustion so the run release is provable:
// with every string cold, no band and no log run may remain.
func TestColdDrainStagesPointerBands(t *testing.T) {
	const cap = 256 << 10
	s := migratorStore(t, cap)

	vals := make(map[string][]byte)
	set := func(name string, v []byte) {
		if err := s.Set([]byte(name), v); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
		vals[name] = v
	}
	// Under the cap: separated and chunked values whose runs sit in the arena.
	set("sep:arena", bigVal(1, 4<<10))
	set("chunk:arena", bigVal(2, 100<<10))
	// Cross the cap with ballast, then write pointer values that spill to the log.
	fillSmall(t, s, 8000)
	set("sep:log", bigVal(3, 4<<10))
	set("chunk:log", bigVal(4, 200<<10))
	if st := s.Stats(); st.Separated != 2 || st.Chunked != 2 {
		t.Fatalf("fixture bands: %+v", st)
	}

	for pass := 0; pass < 4096; pass++ {
		if drivePtrDrain(t, s, 128<<10) == nil {
			break
		}
	}
	st := s.Stats()
	if st.Separated != 0 || st.Chunked != 0 {
		t.Fatalf("pointer bands survived the drain: %+v", st)
	}
	if st.LogRuns != 0 {
		t.Fatalf("%d log runs alive with every record cold: the flip leaked them", st.LogRuns)
	}
	assertCensus(t, s)

	var dst []byte
	for name, want := range vals {
		if !s.slotIsCold([]byte(name)) {
			t.Fatalf("%s did not go cold", name)
		}
		got, ok := s.GetString([]byte(name), 0, dst)
		if !ok {
			t.Fatalf("%s missing after flip", name)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s cold read: %d bytes, want %d, first diff at %d",
				name, len(got), len(want), firstDiff(got, want))
		}
	}
}

func firstDiff(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// TestColdDrainOversizedFitGate pins the buffer rule for resolved frames: a
// pointer record wider than the buffer's remaining capacity waits rather
// than blowing a shared pass, and takes an empty buffer to itself, growing
// it once. Any drain that ends past its pool capacity must therefore hold
// exactly one frame.
func TestColdDrainOversizedFitGate(t *testing.T) {
	s := migratorStore(t, 64<<10)
	want := bigVal(9, 300<<10)
	if err := s.Set([]byte("jumbo"), want); err != nil {
		t.Fatal(err)
	}
	fillSmall(t, s, 3000)

	const bufCap = 32 << 10
	for pass := 0; pass < 4096; pass++ {
		d := drivePtrDrain(t, s, bufCap)
		if d == nil {
			break
		}
		if len(d.buf) > bufCap && len(d.flips) != 1 {
			t.Fatalf("a grown drain holds %d frames (%d bytes), want exactly 1", len(d.flips), len(d.buf))
		}
	}
	if !s.slotIsCold([]byte("jumbo")) {
		t.Fatal("the oversized record never drained")
	}
	got, ok := s.GetString([]byte("jumbo"), 0, nil)
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("jumbo cold read: ok=%v len=%d, want %d", ok, len(got), len(want))
	}
	assertCensus(t, s)
}

// TestColdDrainPointerRacedWrite is the stale-flip race on the widened
// bands: a foreground SET replaces a staged separated record mid-window, so
// phase 2 must drop the frame without touching the record's current run.
// The new value survives and the census holds, which a double release of
// the replaced run would break.
func TestColdDrainPointerRacedWrite(t *testing.T) {
	s := migratorStore(t, 64<<10)
	if err := s.Set([]byte("sep:raced"), bigVal(5, 4<<10)); err != nil {
		t.Fatal(err)
	}
	fillSmall(t, s, 3000)

	var d *coldDrain
	for pass := 0; pass < 4096; pass++ {
		d = s.StageColdDrainDeep(make([]byte, 0, 128<<10))
		if d == nil || len(d.flips) == 0 {
			t.Fatal("the separated record never staged")
		}
		found := false
		for j := range d.flips {
			if string(flipKey(d, j)) == "sep:raced" {
				found = true
				break
			}
		}
		if found {
			break
		}
		if _, err := s.ColdWriteAt(d.Off(), d.Buf()); err != nil {
			t.Fatal(err)
		}
		s.CompleteColdDrain(d, true)
		d = nil
	}
	if d == nil {
		t.Fatal("no drain carried the separated record")
	}

	newVal := bigVal(6, 5<<10)
	if err := s.Set([]byte("sep:raced"), newVal); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ColdWriteAt(d.Off(), d.Buf()); err != nil {
		t.Fatal(err)
	}
	s.CompleteColdDrain(d, true)

	if s.slotIsCold([]byte("sep:raced")) {
		t.Fatal("raced key went cold over a live write")
	}
	got, ok := s.GetString([]byte("sep:raced"), 0, nil)
	if !ok || !bytes.Equal(got, newVal) {
		t.Fatalf("raced key: ok=%v len=%d, want the raced write's %d bytes", ok, len(got), len(newVal))
	}
	assertCensus(t, s)
	if s.migrating != 0 {
		t.Fatalf("migrating counter = %d, want 0", s.migrating)
	}
}

// TestColdPointerBringUp closes the loop: a cold frame whose value outgrew
// the embedded band comes back as a pointer-band resident, re-selected by
// size, never as an oversized embedded record (which would overflow the
// header's u16 vcap word). The separated key returns through a write's
// unconditional bring-up, the chunked one through the read doorkeeper's
// second sighting.
func TestColdPointerBringUp(t *testing.T) {
	s := migratorStore(t, 64<<10)
	sepVal := bigVal(7, 4<<10)
	chunkVal := bigVal(8, 100<<10)
	if err := s.Set([]byte("sep:up"), sepVal); err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("chunk:up"), chunkVal); err != nil {
		t.Fatal(err)
	}
	fillSmall(t, s, 3000)
	for pass := 0; pass < 4096; pass++ {
		if drivePtrDrain(t, s, 128<<10) == nil {
			break
		}
	}
	if !s.slotIsCold([]byte("sep:up")) || !s.slotIsCold([]byte("chunk:up")) {
		t.Fatal("fixture keys did not go cold")
	}

	// A write to the cold separated key brings it up unconditionally.
	suffix := []byte("-tail")
	if _, err := s.Append([]byte("sep:up"), suffix, 0); err != nil {
		t.Fatal(err)
	}
	if s.slotIsCold([]byte("sep:up")) {
		t.Fatal("write did not bring the key up")
	}
	got, ok := s.GetString([]byte("sep:up"), 0, nil)
	if !ok || !bytes.Equal(got, append(append([]byte(nil), sepVal...), suffix...)) {
		t.Fatalf("sep:up after append: ok=%v len=%d", ok, len(got))
	}
	if st := s.Stats(); st.Separated == 0 {
		t.Fatalf("brought-up separated key landed in the wrong band: %+v", st)
	}

	// Two reads take the chunked key through the doorkeeper's promotion.
	for range 2 {
		if _, ok := s.GetString([]byte("chunk:up"), 0, nil); !ok {
			t.Fatal("chunk:up unreadable")
		}
	}
	if s.slotIsCold([]byte("chunk:up")) {
		t.Fatal("second sighting did not promote the chunked key")
	}
	got, ok = s.GetString([]byte("chunk:up"), 0, nil)
	if !ok || !bytes.Equal(got, chunkVal) {
		t.Fatalf("chunk:up after promotion: ok=%v len=%d, want %d", ok, len(got), len(chunkVal))
	}
	if st := s.Stats(); st.Chunked == 0 {
		t.Fatalf("promoted chunked key landed in the wrong band: %+v", st)
	}
	assertCensus(t, s)
}
