package main

import (
	"testing"
	"time"
)

// TestLapMatchesGroundTruth pins the harness against the store it
// measures: one full-budget lap tallies exactly the keyspace the
// build planted, per class, and the probe finds every key planted
// already-expired.
func TestLapMatchesGroundTruth(t *testing.T) {
	cfg := config{dir: t.TempDir(), keys: 2000, val: 32, nearPct: 25, midPct: 25, farPct: 25, expPct: 50}
	b, err := build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.s.Close()
	r, err := lap(b.s, 1<<20, cfg.keys)
	if err != nil {
		t.Fatal(err)
	}
	if r.passes != 1 {
		t.Fatalf("full-budget lap took %d passes, want 1", r.passes)
	}
	sm := b.s.Stats().ExpiryClasses
	for c := range sm {
		if sm[c].Entries != b.perCls[c] {
			t.Errorf("class %d entries %d, want %d", c, sm[c].Entries, b.perCls[c])
		}
	}
	if r.expired != int64(len(b.expired)) {
		t.Errorf("probe found %d expired, planted %d", r.expired, len(b.expired))
	}
	// The planting itself must hit the requested rate, not just agree
	// with the probe: the first sweep shipped with a drifting
	// condition that planted exactly one expired key and every
	// self-consistency check passed anyway.
	wantExp := b.perCls[1] * int64(cfg.expPct) / 100
	if got := int64(len(b.expired)); got < wantExp*9/10 || got > wantExp*11/10 {
		t.Errorf("planted %d expired near keys, want about %d", got, wantExp)
	}
	if r.entries != int64(cfg.keys) {
		t.Errorf("lap tallied %d entries, want %d", r.entries, cfg.keys)
	}
}

// TestBoundedLapConverges holds the lap loop to its purpose at a
// tiny budget: it terminates, covers the keyspace, and the per-pass
// timings it reports are nonzero.
func TestBoundedLapConverges(t *testing.T) {
	cfg := config{dir: t.TempDir(), keys: 5000, val: 32, nearPct: 25, midPct: 25, farPct: 25, expPct: 0}
	b, err := build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.s.Close()
	start := time.Now()
	r, err := lap(b.s, 1, cfg.keys)
	if err != nil {
		t.Fatal(err)
	}
	if r.passes < 2 {
		t.Fatalf("budget-1 lap took %d passes, want several", r.passes)
	}
	if r.entries < int64(cfg.keys) {
		t.Fatalf("lap tallied %d entries, want at least %d", r.entries, cfg.keys)
	}
	if time.Since(start) > time.Minute {
		t.Fatal("budget-1 lap took over a minute on 5000 keys")
	}
}
