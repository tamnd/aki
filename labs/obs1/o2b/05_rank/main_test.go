package main

import "testing"

// TestRankSmoke runs the shrunken decades with the latency model off:
// harness mechanics only, the flatness bands belong to the scored run.
func TestRankSmoke(t *testing.T) {
	res, err := run(cfg{maxN: 100_000, fetches: 50, lat: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.rows) != 3 {
		t.Fatalf("rows = %d, want 3 decades", len(res.rows))
	}
	for _, r := range res.rows {
		if r.getsPerOp != 2.0 {
			t.Errorf("n=%d gets per op %.4f, want exactly 2", r.n, r.getsPerOp)
		}
		if r.exactPct != 100.0 {
			t.Errorf("n=%d rank exact %.1f%%, want 100", r.n, r.exactPct)
		}
		if r.floorUs < 0 {
			t.Errorf("n=%d floor walk not measured", r.n)
		}
		if r.chunks <= 0 || r.dirBytes <= 0 {
			t.Errorf("n=%d empty directory: chunks %d bytes %d", r.n, r.chunks, r.dirBytes)
		}
	}
	if res.cold.Errs != 0 || res.cold.Unresolved != 0 || res.cold.Misses != 0 {
		t.Fatalf("cold stats not clean: %+v", res.cold)
	}
}
