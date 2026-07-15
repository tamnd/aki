package store

import (
	"fmt"
	"testing"
)

// driveStage runs phase 1 against a fresh staging buffer and returns the drain,
// the shape the shard worker's drainCold produces before it hands the buffer to
// the I/O worker. The buffer's capacity sets the drain's fill ceiling, so a
// modest cap keeps a pressured store's pass small and the test fast.
func driveStage(t *testing.T, s *Store) *coldDrain {
	t.Helper()
	buf := make([]byte, 0, 128<<10)
	d := s.StageColdDrain(buf)
	if d == nil {
		t.Fatal("StageColdDrain returned nil under pressure")
	}
	if len(d.flips) == 0 {
		t.Fatal("StageColdDrain staged nothing under pressure")
	}
	return d
}

// flipKey copies the key the j-th flip points at out of the drain buffer.
func flipKey(d *coldDrain, j int) []byte {
	f := d.flips[j]
	k := make([]byte, f.keyLen)
	copy(k, d.buf[f.keyOff:f.keyOff+f.keyLen])
	return k
}

// TestColdDrainAsyncFlip drives the two phases the way the I/O worker splits
// them: phase 1 stages a run of records into a buffer while they stay resident,
// the pwrite lands, and phase 2 flips their slots to the cold region. Every
// staged key then reads its value back from its frame, the cold count rises by
// the flip count, and the census holds.
func TestColdDrainAsyncFlip(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	const n = 40000
	fillSmall(t, s, n)
	if s.arena.live() <= cap {
		t.Fatalf("fixture did not cross the cap: live=%d cap=%d", s.arena.live(), cap)
	}

	d := driveStage(t, s)
	staged := len(d.flips)

	// The records are still resident and readable through the window.
	if s.Cold().Records != 0 {
		t.Fatalf("phase 1 flipped a slot: cold=%d", s.Cold().Records)
	}
	k0 := flipKey(d, 0)
	if s.slotIsCold(k0) {
		t.Fatal("phase 1 turned a staged key cold")
	}

	// The off-owner pwrite.
	nw, err := s.ColdWriteAt(d.Off(), d.Buf())
	if err != nil || nw != len(d.Buf()) {
		t.Fatalf("cold write: n=%d err=%v, want n=%d", nw, err, len(d.Buf()))
	}

	// Phase 2 flips.
	flipped := s.CompleteColdDrain(d, true)
	if flipped != staged {
		t.Fatalf("flipped %d, staged %d, want equal with no racing write", flipped, staged)
	}
	if s.Cold().Records != uint64(flipped) {
		t.Fatalf("cold records = %d, want %d", s.Cold().Records, flipped)
	}
	if s.migrating != 0 {
		t.Fatalf("migrating counter = %d after completion, want 0", s.migrating)
	}
	assertCensus(t, s)

	// Every staged key reads its value from the cold frame now.
	var dst []byte
	for j := 0; j < len(d.flips); j++ {
		k := flipKey(d, j)
		if !s.slotIsCold(k) {
			t.Fatalf("staged key %q did not go cold", k)
		}
		got, ok := s.GetString(k, 0, dst)
		if !ok {
			t.Fatalf("staged key %q missing after flip", k)
		}
		var i int
		if _, err := fmt.Sscanf(string(k), "k:%d", &i); err != nil {
			t.Fatalf("parse key %q: %v", k, err)
		}
		if string(got) != wantVal(i) {
			t.Fatalf("cold key %d = %q, want %q", i, got, wantVal(i))
		}
	}

	// And every key in the store still answers, cold or resident.
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		got, ok := s.GetString(k, 0, dst)
		if !ok || string(got) != wantVal(i) {
			t.Fatalf("key %d = %q,%v, want %q", i, got, ok, wantVal(i))
		}
	}
}

// TestColdDrainStaleFlipDrops is the load-bearing race: a foreground SET writes a
// staged key while its drain is in flight, so phase 2 must drop that frame rather
// than flip the slot onto a value the write superseded. The overwritten key stays
// resident with its new value, its frame is abandoned, and the cold count picks up
// only the records no write touched.
func TestColdDrainStaleFlipDrops(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	fillSmall(t, s, 40000)

	d := driveStage(t, s)
	staged := len(d.flips)

	// A foreground write to one staged key, mid-window: findResident cancels its
	// migration in place, so phase 2's compare will miss it.
	raced := flipKey(d, 0)
	const newVal = "raced-write-wins"
	if err := s.Set(raced, []byte(newVal)); err != nil {
		t.Fatal(err)
	}

	nw, err := s.ColdWriteAt(d.Off(), d.Buf())
	if err != nil || nw != len(d.Buf()) {
		t.Fatalf("cold write: n=%d err=%v", nw, err)
	}

	flipped := s.CompleteColdDrain(d, true)
	if flipped != staged-1 {
		t.Fatalf("flipped %d, want %d (one racing write dropped)", flipped, staged-1)
	}
	if s.migrating != 0 {
		t.Fatalf("migrating counter = %d, want 0", s.migrating)
	}

	// The raced key kept its new value and stayed resident.
	if s.slotIsCold(raced) {
		t.Fatal("raced key went cold: the stale frame flipped over a live write")
	}
	var dst []byte
	got, ok := s.GetString(raced, 0, dst)
	if !ok || string(got) != newVal {
		t.Fatalf("raced key = %q,%v, want %q", got, ok, newVal)
	}
	if s.Cold().Records != uint64(flipped) {
		t.Fatalf("cold records = %d, want %d", s.Cold().Records, flipped)
	}
	assertCensus(t, s)
}

// TestColdDrainWriteFailureStaysResident checks the pwrite-failure path: with no
// durable frame, phase 2 keeps every staged record resident and clears its mark,
// so nothing goes cold, the values are intact, and a later drain can stage the
// same records again.
func TestColdDrainWriteFailureStaysResident(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	fillSmall(t, s, 40000)

	d := driveStage(t, s)
	keys := make([][]byte, len(d.flips))
	for j := range d.flips {
		keys[j] = flipKey(d, j)
	}

	// The write failed: no frame is on disk.
	if kept := s.CompleteColdDrain(d, false); kept != 0 {
		t.Fatalf("flipped %d on a failed write, want 0", kept)
	}
	if s.Cold().Records != 0 {
		t.Fatalf("cold records = %d after a failed write, want 0", s.Cold().Records)
	}
	if s.migrating != 0 {
		t.Fatalf("migrating counter = %d, want 0", s.migrating)
	}
	assertCensus(t, s)

	var dst []byte
	for _, k := range keys {
		if s.slotIsCold(k) {
			t.Fatalf("key %q went cold on a failed write", k)
		}
		var i int
		if _, err := fmt.Sscanf(string(k), "k:%d", &i); err != nil {
			t.Fatal(err)
		}
		got, ok := s.GetString(k, 0, dst)
		if !ok || string(got) != wantVal(i) {
			t.Fatalf("key %d = %q,%v, want %q", i, got, ok, wantVal(i))
		}
	}

	// The mark is cleared, so the pressure that survives lets a fresh drain stage
	// records again rather than seeing them all already migrating.
	d2 := s.StageColdDrain(make([]byte, 0, 128<<10))
	if d2 == nil || len(d2.flips) == 0 {
		t.Fatal("no records staged on the retry after a failed write")
	}
}

// TestColdDrainNoPressureNil is the L9 guard on the async path: under the cap
// StageColdDrain returns nil, so the worker draws no buffer and starts no
// goroutine.
func TestColdDrainNoPressureNil(t *testing.T) {
	s := migratorStore(t, 8<<20)
	fillSmall(t, s, 100)
	if s.NeedsColdDrain() {
		t.Fatal("NeedsColdDrain true under the cap")
	}
	if d := s.StageColdDrain(make([]byte, 0, 128<<10)); d != nil {
		t.Fatalf("staged a drain under the cap: %d flips", len(d.flips))
	}
}
