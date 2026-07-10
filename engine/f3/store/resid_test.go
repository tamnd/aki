package store

import (
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"
)

// residStore opens a store shaped for the residency tests: separated-band
// values, a cap a few segments wide, plenty of arena behind it.
func residStore(t *testing.T, capBytes uint64) *Store {
	t.Helper()
	s, err := Open(Options{
		ArenaBytes:       64 << 20,
		SegBytes:         256 << 10,
		VlogPath:         filepath.Join(t.TempDir(), "vlog"),
		ResidentCapBytes: capBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic doorkeeper: every first touch marks, so the two-touch
	// tests can pin exact promotion points. The shipped sampled doorkeeper
	// only changes how many first touches mark, not the machinery after.
	s.TuneDoorkeeper(1)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fillPast writes n separated values of size sz so the later ones spill.
func fillPast(t *testing.T, s *Store, n, sz int) {
	t.Helper()
	v := make([]byte, sz)
	for i := range v {
		v[i] = 'a' + byte(i%26)
	}
	for i := 0; i < n; i++ {
		if err := s.Set(fmt.Appendf(nil, "key:%05d", i), v); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
}

// TestPromoteSecondTouch pins the doorkeeper: a log-resident run's first read
// only marks, the second read moves the bytes to the arena, and every read
// after that is a memory read.
func TestPromoteSecondTouch(t *testing.T) {
	s := residStore(t, 2<<20)
	fillPast(t, s, 3000, 2<<10) // ~6MiB of values against a 2MiB cap
	if s.Stats().LogRuns == 0 {
		t.Fatal("nothing spilled; the fixture is wrong")
	}
	// The last key written spilled (the cap was long crossed).
	k := []byte("key:02999")
	var dst []byte

	base := s.Resid()
	dst, ok := s.Get(k, dst)
	if !ok {
		t.Fatal("key unreadable")
	}
	r := s.Resid()
	if r.LogReads != base.LogReads+1 || r.Promotes != base.Promotes {
		t.Fatalf("first touch: logReads %d->%d promotes %d->%d, want one read and no promotion",
			base.LogReads, r.LogReads, base.Promotes, r.Promotes)
	}

	// Second touch promotes, but only with headroom: spillNow gates on the
	// arena fill, and fill drops when compaction frees drained segments, not
	// when a record dies. Kill a contiguous early stretch so whole segments
	// drain and the compactor hands their bytes back.
	for i := 0; i < 512; i++ {
		s.Del(fmt.Appendf(nil, "key:%05d", i), 0)
	}
	s.CompactArena()
	if _, ok = s.Get(k, dst); !ok {
		t.Fatal("key unreadable")
	}
	r2 := s.Resid()
	if r2.Promotes != r.Promotes+1 {
		t.Fatalf("second touch did not promote: promotes %d->%d", r.Promotes, r2.Promotes)
	}
	// Third touch: no log read.
	if _, ok = s.Get(k, dst); !ok {
		t.Fatal("key unreadable")
	}
	r3 := s.Resid()
	if r3.LogReads != r2.LogReads {
		t.Fatalf("promoted key still read the log: logReads %d->%d", r2.LogReads, r3.LogReads)
	}
	checkLedger(t, s, "after promotion")
}

// TestDemoteEvictsColdKeepsHot pins the SIEVE half: past the cap, a demotion
// pass moves unvisited resident runs to the log and gives visited ones a
// second chance.
func TestDemoteEvictsColdKeepsHot(t *testing.T) {
	s := residStore(t, 2<<20)
	fillPast(t, s, 1500, 2<<10) // ~3MiB of values against a 2MiB cap
	before := s.Stats().LogRuns
	if before == 0 {
		t.Fatal("nothing spilled; the fixture is wrong")
	}

	// Touch a hot resident subset so its visited bits are set.
	var dst []byte
	hot := [][]byte{}
	for i := 0; i < 64; i++ {
		hot = append(hot, fmt.Appendf(nil, "key:%05d", i))
	}
	for _, k := range hot {
		if dst, _ = s.Get(k, dst); len(dst) == 0 {
			t.Fatalf("hot key %s unreadable", k)
		}
	}

	// Drive the fill over the cap so MaybeDemote engages: overwrite spilled
	// keys with fresh values while there is no headroom, then demote at the
	// emulated boundary until the pass declines.
	moved := uint64(0)
	for i := 0; i < 64; i++ {
		n := s.MaybeDemote()
		moved += n
		s.CompactArena()
		if n == 0 {
			break
		}
	}
	if moved == 0 {
		t.Fatal("no demotion ran over a store past its cap")
	}
	checkLedger(t, s, "after demotion")

	// The hot subset survived resident: reading it again adds no log reads.
	base := s.Resid().LogReads
	for _, k := range hot {
		if dst, _ = s.Get(k, dst); len(dst) == 0 {
			t.Fatalf("hot key %s unreadable after demotion", k)
		}
	}
	if got := s.Resid().LogReads; got != base {
		t.Fatalf("hot keys were demoted: %d log reads on the hot set", got-base)
	}
	if after := s.Stats().LogRuns; after <= before {
		t.Fatalf("demotion moved nothing to the log: runs %d -> %d", before, after)
	}
}

// TestResidencyBoundsFill is the RSS half of the slice, in ledger terms: under
// sustained churn with boundary demotion and compaction, the arena fill (the
// figure the pages track) stays near the cap instead of walking to the arena
// size, and every value stays readable from one side or the other.
func TestResidencyBoundsFill(t *testing.T) {
	const capBytes = 2 << 20
	s := residStore(t, capBytes)
	rng := rand.New(rand.NewPCG(13, 542))
	const nKeys = 3000
	v := make([]byte, 2<<10)
	for i := range v {
		v[i] = 'a' + byte(i%26)
	}
	fillPast(t, s, nKeys, len(v))

	var dst []byte
	for op := 0; op < 30000; op++ {
		k := fmt.Appendf(nil, "key:%05d", rng.IntN(nKeys))
		if rng.IntN(4) == 0 {
			if err := s.Set(k, v); err != nil {
				t.Fatalf("op %d: %v", op, err)
			}
		} else {
			if dst, _ = s.Get(k, dst); len(dst) != len(v) {
				t.Fatalf("op %d: key %s lost", op, k)
			}
		}
		if op%1024 == 1023 {
			if s.MaybeDemote() > 0 || s.ArenaTight() || s.ResidentOver() {
				s.CompactArena()
			}
		}
	}
	s.MaybeDemote()
	s.CompactArena()
	checkLedger(t, s, "after churn")

	used, _ := s.ArenaBytes()
	// The bound: fill within the cap plus one pass of drift (the demotion
	// budget slack plus a segment of bump remainder), nowhere near the 64MiB
	// arena.
	slack := uint64(capBytes)/residSlackDen + 512<<10
	if used > capBytes+slack {
		t.Fatalf("arena fill %d over cap %d + slack %d after boundary demotion", used, capBytes, slack)
	}
}

// TestResidencyOffIsInert pins the tune surface the lab leans on: with the
// policy off the store behaves like the pre-residency engine, no promotion,
// no demotion, no visited traffic.
func TestResidencyOffIsInert(t *testing.T) {
	s := residStore(t, 2<<20)
	s.TuneResidency(ResidOff)
	fillPast(t, s, 1500, 2<<10)
	var dst []byte
	for i := 0; i < 3; i++ {
		if dst, _ = s.Get([]byte("key:01499"), dst); len(dst) == 0 {
			t.Fatal("key unreadable")
		}
	}
	if s.MaybeDemote() != 0 {
		t.Fatal("demotion ran with residency off")
	}
	r := s.Resid()
	if r.Promotes != 0 || r.Demotes != 0 {
		t.Fatalf("residency off still moved values: %+v", r)
	}
}

// TestMarkAlwaysSameBits pins lab 15's knob: the always-store mark variant
// must be observationally identical to the shipped check-then-set, because
// the two only differ in how many times the same bit value is written. Run
// the demote-evicts-cold shape under the knob and check the policy outcome
// is unchanged: hot stays resident, cold demotes.
func TestMarkAlwaysSameBits(t *testing.T) {
	s := residStore(t, 2<<20)
	s.TuneMarkAlways(true)
	fillPast(t, s, 1500, 2<<10) // ~3MiB of values against a 2MiB cap
	// Touch a hot resident subset so its visited bits are set (and, under
	// the knob, re-stored on every one of these reads).
	var dst []byte
	hot := [][]byte{}
	for i := 0; i < 64; i++ {
		hot = append(hot, fmt.Appendf(nil, "key:%05d", i))
	}
	for range 3 {
		for _, k := range hot {
			if dst, _ = s.Get(k, dst); len(dst) == 0 {
				t.Fatalf("hot key %s unreadable", k)
			}
		}
	}
	moved := uint64(0)
	for i := 0; i < 64; i++ {
		n := s.MaybeDemote()
		moved += n
		s.CompactArena()
		if n == 0 {
			break
		}
	}
	if moved == 0 {
		t.Fatal("no demotion ran over a store past its cap")
	}
	checkLedger(t, s, "after demotion under mark-always")
	base := s.Resid().LogReads
	for _, k := range hot {
		if dst, _ = s.Get(k, dst); len(dst) == 0 {
			t.Fatalf("hot key %s unreadable after demotion", k)
		}
	}
	if got := s.Resid().LogReads; got != base {
		t.Fatalf("hot keys were demoted under mark-always: %d log reads", got-base)
	}
}
