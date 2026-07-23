package main

import "testing"

func TestStreamCatchupSmoke(t *testing.T) {
	res, err := run(20_000, 5_000)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.amp < 1.05 || res.amp > 1.25 {
		t.Fatalf("amp %.4f outside 1.05..1.25", res.amp)
	}
	byName := map[string]cell{}
	for _, c := range res.cells {
		byName[c.name] = c
	}
	if g := byName["catchup_readahead"].gets; g != 1 {
		t.Fatalf("readahead catch-up took %d GETs, want 1", g)
	}
	if g := byName["perblock_c10"].gets; g != 500 {
		t.Fatalf("perblock c10 took %d GETs, want 500", g)
	}
	if byName["perblock_c10"].mib < 10*byName["perblock_c1000"].mib {
		t.Fatalf("knee inverted: c10 %.2f MiB vs c1000 %.2f MiB",
			byName["perblock_c10"].mib, byName["perblock_c1000"].mib)
	}
	if g := byName["xrange_k100"].gets; g != 1 {
		t.Fatalf("xrange k100 took %d GETs, want 1", g)
	}
	bpe, ratio, dropped, residual, err := pelRun(10_000, 1<<20)
	if err != nil {
		t.Fatalf("pel: %v", err)
	}
	if bpe < 25 || bpe > 45 {
		t.Fatalf("pel %.2f B/entry outside 25..45", bpe)
	}
	if ratio <= 0 || dropped == 0 || residual != 0 {
		t.Fatalf("pel reclaim ratio %.4f dropped %d residual %d", ratio, dropped, residual)
	}
}
