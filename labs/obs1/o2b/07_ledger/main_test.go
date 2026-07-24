package main

import "testing"

// TestLedgerSmoke runs the small configuration: harness mechanics only,
// the bands belong to the scored run.
func TestLedgerSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("full server rig")
	}
	res, err := run(cfg{members: 4000, elems: 4000, entries: 4000, samples: 200})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.cells {
		if c.found != c.ops {
			t.Errorf("cell %s: %d of %d ops correct", c.name, c.found, c.ops)
		}
		if c.gets == 0 && c.name != "zscore_miss" {
			t.Errorf("cell %s billed no GETs, the corpus never went cold", c.name)
		}
	}
	if res.cold.Errs+res.cold.Unresolved != 0 {
		t.Errorf("cold reader errs/unresolved: %+v", res.cold)
	}
}
