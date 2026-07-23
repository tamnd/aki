package main

import "testing"

// TestFlatSmoke runs the shrunken decades with the latency model off:
// harness mechanics only, the flatness bands belong to the scored run.
func TestFlatSmoke(t *testing.T) {
	res, err := run(cfg{maxN: 100_000, fetches: 50, lat: false})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.rows) != 3 {
		t.Fatalf("rows = %d, want 3 decades", len(res.rows))
	}
	for _, r := range res.rows {
		if r.getsPerOp != 1.0 {
			t.Errorf("n=%d gets per op %.4f, want exactly 1", r.n, r.getsPerOp)
		}
		if r.foundPct != 100.0 {
			t.Errorf("n=%d found %.1f%%, want 100", r.n, r.foundPct)
		}
		if r.resolveUs <= 0 {
			t.Errorf("n=%d resolve time not measured", r.n)
		}
		if r.chunks <= 0 || r.dirBytes <= 0 {
			t.Errorf("n=%d empty directory: chunks %d bytes %d", r.n, r.chunks, r.dirBytes)
		}
	}
	if res.cold.Errs != 0 || res.cold.Unresolved != 0 || res.cold.Misses != 0 {
		t.Fatalf("cold stats not clean: %+v", res.cold)
	}
}
