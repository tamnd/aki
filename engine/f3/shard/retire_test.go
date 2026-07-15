package shard

import (
	"fmt"
	"testing"
)

// TestDrainRetiresAndReclaims drives the migrator's phase-2 retire, the first
// caller of the F6 reclamation machinery (spec 2064/f3/06 section 3.1, "segment
// fully drained -> retire at the current batch boundary"). An aggressive resident
// cap (below one segment) forces the async migrator to fully evacuate segments:
// every record demotes cold, so a segment's last flip unlinks its last live
// record and leaves it fully dead, and the store notes it. The worker then stamps
// those segments with the current epoch and retires them through RetireSegment
// rather than freeing them outright, and ReclaimSafe frees them only once the safe
// epoch passes the stamp. The run loop's own drain boundary holds no bracket, so a
// real reader is what makes the gate bite; the test opens an owner bracket by hand
// to stand in for that future reader (a cross-shard hop, a parked cold read) and
// proves the retired segment waits it out.
//
// The compactor's relocation is the other, more common segment-emptying path, and
// it still frees outright; routing its frees through the epoch too is the follow-on
// slice that closes reader-safety for the compactor. This slice covers the
// migrator's own emptied segments.
func TestDrainRetiresAndReclaims(t *testing.T) {
	// A cap below one segment (256 KiB here) makes the migrator drain nearly the
	// whole resident set, so segments empty outright instead of keeping a scatter
	// of residents the way a roomy cap would.
	const cap = 16 << 10
	s := drainStore(t, cap)
	w := newWorker(0, s)

	const n = 40000
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		v := fmt.Appendf(nil, "v-%d", i)
		if err := s.Set(k, v); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if !s.NeedsColdDrain() {
		t.Fatal("fixture did not cross the cap")
	}

	// Stage and flip drains without the boundary retire, so the emptied segments
	// accumulate on the store's drained list instead of being retired each pass.
	// This lets the test drive the retire once, deliberately, against a bracket.
	for pass := 0; pass < 64 && s.NeedsColdDrain(); pass++ {
		w.drainCold()
		w.advanceIntents()
	}
	w.io.stop()
	for i := 0; i < 128 && w.io.pool.out > 0; i++ {
		if w.advanceIntents() == 0 {
			break
		}
	}

	if s.PendingDrained() == 0 {
		t.Fatal("the flood emptied no segment; nothing to retire")
	}
	if s.RetiredSegs() != 0 {
		t.Fatalf("segments retired before the boundary ran: %d", s.RetiredSegs())
	}

	// Open a batch bracket at the current epoch to stand in for an in-flight
	// reader that could still name a drained segment's bytes.
	globalBefore := w.ep.global.Load()
	w.ep.enter()

	// The boundary retire: stamp every emptied segment with the epoch current now
	// and park it. bump advances the global epoch past the stamp, so the open
	// bracket (published at the pre-bump epoch) still pins safe below it.
	w.retireDrained()
	if got := w.ep.global.Load(); got != globalBefore+1 {
		t.Fatalf("global epoch %d after retire, want %d (bump not called)", got, globalBefore+1)
	}
	retired := s.RetiredSegs()
	if retired == 0 {
		t.Fatal("retireDrained parked no segment")
	}
	if s.PendingDrained() != 0 {
		t.Fatalf("drained list not consumed: %d left", s.PendingDrained())
	}

	// With the bracket open, safe sits at the entry epoch, at or below the stamp,
	// so nothing reclaims: the drained bytes outlive the reader.
	if freed := s.ReclaimSafe(w.ep.safe()); freed != 0 {
		t.Fatalf("ReclaimSafe freed %d with the bracket open, want 0", freed)
	}
	if s.RetiredSegs() != retired {
		t.Fatalf("a retired segment reclaimed under an open bracket: %d -> %d", retired, s.RetiredSegs())
	}

	// The bracket exits; safe advances past the stamp and every retired segment
	// comes back on one reclaim.
	w.ep.exit()
	freed := s.ReclaimSafe(w.ep.safe())
	if freed != retired {
		t.Fatalf("ReclaimSafe freed %d after the bracket exited, want %d", freed, retired)
	}
	if s.RetiredSegs() != 0 {
		t.Fatalf("%d segments still retired after the safe epoch cleared them", s.RetiredSegs())
	}

	// Every key still answers after its segment retired and reclaimed.
	var dst []byte
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "k:%07d", i)
		got, ok := s.GetString(k, 0, dst)
		want := fmt.Sprintf("v-%d", i)
		if !ok || string(got) != want {
			t.Fatalf("key %d = %q,%v, want %q", i, got, ok, want)
		}
	}
}
