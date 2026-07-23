package main

import "testing"

// The whole pipeline at the quick corpus, under race in CI: build over
// RESP, pressure to the fold, and every ledger cell scored on the
// landed cold plane.
func TestLedgerSmoke(t *testing.T) {
	res, err := run(cfg{strings: 200, fields: 1500, members: 600})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.cells {
		switch c.name {
		case "string_miss":
			if c.gets != 0 || c.found != c.ops {
				t.Fatalf("%s: %+v, want 0 GETs and every miss definitive", c.name, c)
			}
		default:
			if int(c.gets) != c.ops || c.found != c.ops {
				t.Fatalf("%s: %+v, want exactly one GET per op and 100%% found", c.name, c)
			}
		}
	}
	if res.cold.Errs != 0 || res.cold.Unresolved != 0 {
		t.Fatalf("cold stats %+v", res.cold)
	}
	if res.coll == 0 || res.share <= 0 || res.share > 0.3 {
		t.Fatalf("directory collection share %.4f over %d chunks, want inside the ledger row", res.share, res.coll)
	}
}
