package store

import (
	"fmt"
	"path/filepath"
	"testing"
)

// migratorStore opens a store with a small resident cap, a cold region, and a
// roomy arena behind both: the shape where a flood of small int and embedded
// keys drives the live charge past the cap with no separated run for the
// residency hand to move, so the whole-record migrator is the only valve.
func migratorStore(t *testing.T, capBytes uint64) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(Options{
		ArenaBytes:       64 << 20,
		SegBytes:         256 << 10,
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

// fillSmall writes n small string keys, alternating an int cell and a short
// embedded value so both self-contained bands are represented.
func fillSmall(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		var v []byte
		if i%2 == 0 {
			v = fmt.Appendf(nil, "%d", i)
		} else {
			v = fmt.Appendf(nil, "v-%d", i)
		}
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
}

// wantVal recomputes the value fillSmall wrote for key i.
func wantVal(i int) string {
	if i%2 == 0 {
		return fmt.Sprintf("%d", i)
	}
	return fmt.Sprintf("v-%d", i)
}

// TestMigrateColdBoundsResident is the memory-bar assertion in unit form: a
// small-key flood past the cap has the migrator move whole records to the cold
// region and the compaction that follows free the drained segments, so the
// arena's handed-out fill comes back down to the cap while every key still reads
// its value, some from the arena and some from a cold frame.
func TestMigrateColdBoundsResident(t *testing.T) {
	const cap = 2 << 20
	s := migratorStore(t, cap)
	const n = 60000
	fillSmall(t, s, n)

	if s.arena.live() <= cap {
		t.Fatalf("fixture did not cross the cap: live=%d cap=%d", s.arena.live(), cap)
	}
	usedBefore, _ := s.ArenaBytes()
	// Drive the boundary the shard loop drives: migrate, then reclaim, until the
	// migrator has nothing left to move. Bounded passes, so loop to convergence.
	low := uint64(cap) - uint64(cap)/residSlackDen
	for i := 0; i < 64; i++ {
		moved := s.MigrateCold()
		s.CompactArena()
		if moved == 0 {
			break
		}
	}
	if s.arena.live() > low {
		t.Fatalf("migrator did not reach the low-water: live=%d low=%d", s.arena.live(), low)
	}
	if s.Cold().Records == 0 {
		t.Fatal("nothing went cold")
	}
	// The handed-out fill (the resident pressure figure, the RSS proxy) came
	// down from the pre-migration peak and now sits within a small factor of the
	// cap: the residual over the cap is the reuse slack behind the bump cursors
	// in segments not yet dead enough to compact, not live data. The exact peak
	// bar versus rivals is the box gate; here the claim is that the fill bounds.
	used, _ := s.ArenaBytes()
	if used >= usedBefore {
		t.Fatalf("resident fill did not fall: used=%d before=%d", used, usedBefore)
	}
	if used > uint64(cap)*3/2 {
		t.Fatalf("resident fill did not bound near the cap: used=%d cap=%d", used, cap)
	}
	assertCensus(t, s)

	// Every key still answers, wherever it now lives.
	var dst []byte
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		dst, ok := s.GetString(k, 0, dst)
		if !ok || string(dst) != wantVal(i) {
			t.Fatalf("key %d = %q,%v, want %q", i, dst, ok, wantVal(i))
		}
	}
}

// TestMigrateColdReadsStayHotThenWriteBringsUp checks the interaction the tier
// is designed around: a cold key served by a read stays cold, but a write to it
// brings it back to the arena, so a working set that turns over is tracked from
// both sides.
func TestMigrateColdReadsStayHotThenWriteBringsUp(t *testing.T) {
	const cap = 1 << 20
	s := migratorStore(t, cap)
	fillSmall(t, s, 40000)
	for i := 0; i < 64; i++ {
		if s.MigrateCold() == 0 {
			s.CompactArena()
			break
		}
		s.CompactArena()
	}
	if s.Cold().Records == 0 {
		t.Fatal("nothing went cold; fixture too small")
	}
	// Find a key that is cold.
	var coldKey []byte
	for i := 0; i < 40000; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		if s.slotIsCold(k) {
			coldKey = k
			break
		}
	}
	if coldKey == nil {
		t.Fatal("no cold key found")
	}
	before := s.Cold().Records
	// A read leaves it cold.
	var dst []byte
	if _, ok := s.GetString(coldKey, 0, dst); !ok {
		t.Fatal("cold key missing")
	}
	if !s.slotIsCold(coldKey) {
		t.Fatal("read promoted a cold key")
	}
	if s.Cold().Records != before {
		t.Fatal("read changed the cold count")
	}
	// A write brings it up.
	if err := s.Set(coldKey, []byte("rewritten")); err != nil {
		t.Fatal(err)
	}
	if s.slotIsCold(coldKey) {
		t.Fatal("write left the key cold")
	}
	if s.Cold().Records != before-1 {
		t.Fatalf("cold count = %d after bring-up, want %d", s.Cold().Records, before-1)
	}
	dst, ok := s.GetString(coldKey, 0, dst)
	if !ok || string(dst) != "rewritten" {
		t.Fatalf("brought-up value = %q,%v", dst, ok)
	}
	assertCensus(t, s)
}

// TestMigrateColdNoPressureNoOp is the L9 guard: with the live charge under the
// cap the migrator does nothing, so a store that never fills past the mark never
// stages a frame and the resident path is untouched.
func TestMigrateColdNoPressureNoOp(t *testing.T) {
	s := migratorStore(t, 8<<20)
	fillSmall(t, s, 100)
	if got := s.MigrateCold(); got != 0 {
		t.Fatalf("migrated %d under the cap, want 0", got)
	}
	if s.Cold().Records != 0 {
		t.Fatalf("cold records = %d under the cap", s.Cold().Records)
	}
}

// TestMigrateColdNoColdRegionNoOp confirms the migrator is inert without a cold
// region even when the cap is configured: there is nowhere to demote, so the
// arena stays the whole store and the trigger declines.
func TestMigrateColdNoColdRegionNoOp(t *testing.T) {
	s, err := Open(Options{
		ArenaBytes:       64 << 20,
		SegBytes:         256 << 10,
		VlogPath:         filepath.Join(t.TempDir(), "vlog"),
		ResidentCapBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	fillSmall(t, s, 40000)
	if got := s.MigrateCold(); got != 0 {
		t.Fatalf("migrated %d with no cold region, want 0", got)
	}
}
