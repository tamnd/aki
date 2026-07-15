package store

import (
	"fmt"
	"path/filepath"
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

// tightArenaStore opens a store with a small fixed arena behind a resident cap
// far larger than the arena, the shape where the low-water migrator never
// engages (live can never reach cap-cap/8) yet the arena still fills with live
// records. It is the deterministic form of the backpressure wedge: a write that
// cannot allocate has no low-water drain to free it a segment.
func tightArenaStore(t *testing.T, arenaBytes, segBytes int, capBytes uint64) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(Options{
		ArenaBytes:       arenaBytes,
		SegBytes:         segBytes,
		VlogPath:         filepath.Join(dir, "vlog"),
		ColdPath:         filepath.Join(dir, "cold"),
		ResidentCapBytes: capBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// driveDeepDrain runs one backpressure drain cycle the way the shard worker does
// while a write is parked: phase 1 past the low-water (StageColdDrainDeep), the
// off-owner pwrite, phase 2, then the retire-and-reclaim that hands any emptied
// segment back to the free list. It stamps the retire at ep and reclaims one past
// it, standing in for the F6 owner bracket the shard test drives directly. It
// returns whether the pass staged anything, so the caller can stop at
// convergence.
func driveDeepDrain(t *testing.T, s *Store, buf []byte, ep uint64) bool {
	t.Helper()
	d := s.StageColdDrainDeep(buf)
	if d == nil || len(d.Buf()) == 0 {
		return false
	}
	nw, err := s.ColdWriteAt(d.Off(), d.Buf())
	if err != nil || nw != len(d.Buf()) {
		t.Fatalf("cold write: n=%d err=%v, want n=%d", nw, err, len(d.Buf()))
	}
	s.CompleteColdDrain(d, true)
	for _, si := range s.TakeColdDrained() {
		if !s.RetireSegment(si, ep) {
			t.Fatalf("retire of drained segment %d refused", si)
		}
	}
	s.ReclaimSafe(ep + 1)
	return true
}

// TestColdDrainDeepServesParkedWritePastLowWater is the F9 rail regression
// (spec 2064/f3/06 section 8): a full arena under a resident cap so generous the
// low-water migrator never engages leaves a write with no way to allocate and no
// low-water drain to free it a segment, the wedge the CI backpressure flake hit.
// The deep drain past the low-water (StageColdDrainDeep, reached while a write is
// parked) frees a segment, so the write that took ErrFull is served rather than
// stalled out, and every key still answers from its cold frame.
func TestColdDrainDeepServesParkedWritePastLowWater(t *testing.T) {
	// 4 MiB arena, 256 KiB segments (16), 64 MiB resident cap: the low-water sits
	// at 56 MiB, far above anything a 4 MiB arena can hold live, so NeedsColdDrain
	// is false however full the arena gets.
	s := tightArenaStore(t, 4<<20, 256<<10, 64<<20)

	// Fill with distinct small keys (no overwrite, so the arena fills with live
	// records) until a write cannot allocate.
	var keys [][]byte
	var i int
	for ; i < 1<<20; i++ {
		k := fmt.Appendf(nil, "k:%08d", i)
		v := fmt.Appendf(nil, "val-%d", i)
		err := s.Set(k, v)
		if err == ErrFull {
			break
		}
		if err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
		keys = append(keys, k)
	}
	if i >= 1<<20 {
		t.Fatal("arena never filled")
	}
	if s.arena.freeSegCount() != 0 {
		t.Fatalf("arena reported full but %d free segments remain", s.arena.freeSegCount())
	}

	// The low-water migrator is quiet: nothing to drain, nothing staged. Without
	// the deep path a parked write here has no route to a free segment.
	if s.NeedsColdDrain() {
		t.Fatal("NeedsColdDrain true under a cap far above the arena")
	}
	if d := s.StageColdDrain(make([]byte, 0, 128<<10)); d != nil {
		t.Fatalf("low-water drain staged %d flips past its floor", len(d.flips))
	}
	if !s.NeedsColdDrainDeep() {
		t.Fatal("NeedsColdDrainDeep false with a full arena of live records")
	}

	// The parked write: it cannot allocate, exactly what backpressure parks on.
	parkKey := fmt.Appendf(nil, "k:%08d", i)
	parkVal := fmt.Appendf(nil, "val-%d", i)
	if err := s.Set(parkKey, parkVal); err != ErrFull {
		t.Fatalf("probe write err = %v, want ErrFull (the arena is full)", err)
	}

	// Drive the deep drain until a segment comes back, the F9 rail freeing room
	// for the parked write. Bounded passes; the store must reach a free segment
	// well before this cap.
	buf := make([]byte, 0, 128<<10)
	ep := uint64(2)
	for pass := 0; s.arena.freeSegCount() == 0 && pass < 4096; pass++ {
		if !driveDeepDrain(t, s, buf, ep) {
			break
		}
		ep += 2
	}
	if s.arena.freeSegCount() == 0 {
		t.Fatalf("deep drain freed no segment: live=%d cold=%d", s.arena.live(), s.Cold().Records)
	}

	// The write that took ErrFull now allocates: block-not-drop served it.
	if err := s.Set(parkKey, parkVal); err != nil {
		t.Fatalf("parked write still refused after the deep drain: %v", err)
	}
	if s.Cold().Records == 0 {
		t.Fatal("nothing went cold on the deep drain")
	}

	// Every key answers, resident or cold, including the one that was parked.
	var dst []byte
	for _, k := range keys {
		var j int
		if _, err := fmt.Sscanf(string(k), "k:%d", &j); err != nil {
			t.Fatalf("parse key %q: %v", k, err)
		}
		got, ok := s.GetString(k, 0, dst)
		if !ok || string(got) != fmt.Sprintf("val-%d", j) {
			t.Fatalf("key %q = %q,%v, want val-%d", k, got, ok, j)
		}
	}
	if got, ok := s.GetString(parkKey, 0, dst); !ok || string(got) != string(parkVal) {
		t.Fatalf("served write %q = %q,%v, want %q", parkKey, got, ok, parkVal)
	}
	assertCensus(t, s)
}
