package main

import (
	"fmt"
	"testing"
)

func TestScanPlanSmoke(t *testing.T) {
	res, err := run(32<<20, 64, 50, 2_000, 512)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	byName := map[string]cell{}
	for _, cs := range [][]cell{res.scan, res.plans, res.fans, res.adm} {
		for _, c := range cs {
			byName[c.name] = c
		}
	}
	if g := byName["scan_perblock"].gets; g != 256 {
		t.Fatalf("perblock %d GETs, want 256", g)
	}
	if g := byName["scan_coalesce16"].gets; g != 2 {
		t.Fatalf("coalesce16 %d GETs, want 2", g)
	}
	if g := byName["plan_10gib_16mib"].gets; g != 640 {
		t.Fatalf("10 GiB plan %d GETs, want 640", g)
	}
	sp := byName["fan_1"].mib / byName["fan_8"].mib
	if sp < 7.5 || sp > 8.1 {
		t.Fatalf("fan 1 to 8 speedup %.2f outside 7.5..8.1", sp)
	}
	hit := func(name string) float64 {
		var h float64
		if _, err := fmt.Sscanf(byName[name].extra, "point_hit %f", &h); err != nil {
			t.Fatalf("parse %s extra %q: %v", name, byName[name].extra, err)
		}
		return h
	}
	ex, ad, lr := hit("adm_s3fifo_exempt"), hit("adm_s3fifo_admit"), hit("adm_lru_admit")
	if ex < ad {
		t.Fatalf("exempt hit %.4f below admit %.4f", ex, ad)
	}
	if ex-lr < 0.03 {
		t.Fatalf("LRU reference arm too close: exempt %.4f lru %.4f", ex, lr)
	}
}
