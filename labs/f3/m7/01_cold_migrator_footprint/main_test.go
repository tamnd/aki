package main

import "testing"

// The lab's claims as invariants, so CI catches a resident-model regression: a
// dataset that fits the cap tracks the rival, one past the cap bounds aki's RAM
// to the index plus the cap while the rival grows, the ratio converges to the
// index share, and the migrator makes past-cap datasets admissible where the
// arena alone would ErrFull.

// TestFitsUnderCapIsAdmissible pins the L9 no-pressure case: a dataset that fits
// the cap is fully resident and admissible, so the migrator never engages and
// aki holds the same records the arena would without a cold tier.
func TestFitsUnderCapIsAdmissible(t *testing.T) {
	cap := int64(64) << 20
	f := model(100_000, 16, 8, false, cap)
	if f.coldRecs != 0 {
		t.Fatalf("a set under the cap put %d records cold, want 0", f.coldRecs)
	}
	if !f.admissible {
		t.Fatal("a set under the cap read as inadmissible")
	}
	if f.akiDisk != 0 {
		t.Fatalf("a set under the cap wrote %d disk bytes, want 0", f.akiDisk)
	}
}

// TestPastCapBoundsResident pins the memory bar: a small-key set far past the cap
// has aki hold less RAM than the rival, and the arena share stays bounded near
// the cap rather than tracking the dataset.
func TestPastCapBoundsResident(t *testing.T) {
	cap := int64(64) << 20
	f := model(64_000_000, 16, 8, false, cap)
	if f.coldRecs == 0 {
		t.Fatal("a set far past the cap put nothing cold")
	}
	if ratio(f.akiRAM, f.rivalRAM) >= 1.0 {
		t.Fatalf("aki/rival = %.4f past the cap, want < 1", ratio(f.akiRAM, f.rivalRAM))
	}
	// The arena share (aki RAM minus the index) never exceeds the cap: the
	// migrator holds the fill to the cap while the index carries the key count.
	arena := f.akiRAM - int64(64_000_000)*indexBytesPerKey
	if arena > cap {
		t.Fatalf("arena share %d exceeds the cap %d", arena, cap)
	}
}

// TestConvergesToIndexShare pins that as the set grows the ratio floors at the
// index-over-rival per-key share, the bounded-arena term washing out.
func TestConvergesToIndexShare(t *testing.T) {
	cap := int64(64) << 20
	small := ratio(mustRAM(1_000_000, cap), rivalRAM(1_000_000, 16, 8))
	big := ratio(mustRAM(256_000_000, cap), rivalRAM(256_000_000, 16, 8))
	fl := floor(16, 8)
	if big > small {
		t.Fatalf("ratio rose with scale: %.4f then %.4f", small, big)
	}
	if big < fl-0.001 || big > fl+0.02 {
		t.Fatalf("large-set ratio %.4f not near the floor %.4f", big, fl)
	}
}

// TestMigratorMakesPastCapAdmissible pins the admissibility claim: without the
// migrator a small-key set past the cap cannot fit the arena (ErrFull), so the
// tier is what lets the store hold the dataset at all, not only what shrinks its
// RAM.
func TestMigratorMakesPastCapAdmissible(t *testing.T) {
	cap := int64(64) << 20
	under := model(1_000_000, 16, 8, false, cap)
	if !under.admissible {
		t.Fatal("a 1M set under the cap should be admissible without the migrator")
	}
	over := model(64_000_000, 16, 8, false, cap)
	if over.admissible {
		t.Fatal("a 64M set past the cap should be inadmissible without the migrator")
	}
	if over.residentRecs+over.coldRecs != 64_000_000 {
		t.Fatalf("record split %d+%d does not sum to the set", over.residentRecs, over.coldRecs)
	}
}

// TestLargerValueDeepensWin pins sweep B: a larger value costs the rival more RAM
// per key and pushes more bytes to disk, so the ratio falls as the value grows.
func TestLargerValueDeepensWin(t *testing.T) {
	cap := int64(64) << 20
	prev := 2.0
	for _, vl := range []int{8, 32, 128, 512} {
		f := model(16_000_000, 16, vl, false, cap)
		r := ratio(f.akiRAM, f.rivalRAM)
		if r >= prev {
			t.Fatalf("value %d ratio %.4f did not fall below %.4f", vl, r, prev)
		}
		prev = r
	}
}

// mustRAM is a test shim for the aki RAM figure at a key count and cap, the
// 16-byte-key 8-byte-value shape the floor test sweeps.
func mustRAM(n int, cap int64) int64 { return model(n, 16, 8, false, cap).akiRAM }

// rivalRAM is the rival figure for the same shape.
func rivalRAM(n, klen, vlen int) int64 { return int64(n) * int64(rivalPerKey+klen+vlen) }
