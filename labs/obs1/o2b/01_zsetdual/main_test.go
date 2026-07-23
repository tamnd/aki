package main

import "testing"

func TestZsetDualSmoke(t *testing.T) {
	res, err := run(20_000, 100, 2_000)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !res.consist {
		t.Fatal("churned projections disagree")
	}
	if res.ampRatio < 1.9 || res.ampRatio > 2.1 {
		t.Fatalf("dual amp ratio %v outside the 2x band", res.ampRatio)
	}
	for _, c := range res.cells {
		if c.name == "zscore" || c.name == "zrank" {
			if c.gets != 1.0 {
				t.Fatalf("%s costs %v GETs per op, want exactly 1", c.name, c.gets)
			}
			if c.extra != "100.0 found%" {
				t.Fatalf("%s found rate %q", c.name, c.extra)
			}
		}
	}
}
