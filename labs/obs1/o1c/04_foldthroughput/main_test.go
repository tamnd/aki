package main

import "testing"

// A miniature stage cell exercises the whole owner loop, fill, drain
// trigger, stage, cold write, flip, refill, under race in CI.
func TestStageCellSmoke(t *testing.T) {
	out, err := runStageCell(stageCfg{
		valBytes: 200, passBytes: 1 << 20, targetBytes: 2 << 20,
		arenaBytes: 64 << 20, capBytes: 8 << 20, marginBytes: 4 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.stagedBytes < 2<<20 || out.passes == 0 {
		t.Fatalf("staged %d bytes in %d passes", out.stagedBytes, out.passes)
	}
	if len(out.stalls) != out.passes {
		t.Fatalf("stalls %d passes %d", len(out.stalls), out.passes)
	}
	if out.stallSec <= 0 || out.totalSec < out.stallSec {
		t.Fatalf("stall %.6fs total %.6fs", out.stallSec, out.totalSec)
	}
	if out.setsPerSec <= 0 {
		t.Fatal("no fill rate")
	}
}

func TestBuildCellSmoke(t *testing.T) {
	mibps, err := runBuildCell(200, 128, 4<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	if mibps <= 0 {
		t.Fatalf("build rate %f", mibps)
	}
}

func TestQuantile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5}
	if quantile(s, 0.5) != 3 || quantile(s, 0.99) != 5 || quantile(nil, 0.5) != 0 {
		t.Fatal("quantile shape")
	}
}

func TestPrngDeterministic(t *testing.T) {
	a, b := &prng{s: 3}, &prng{s: 3}
	for range 100 {
		if a.next() != b.next() {
			t.Fatal("diverged")
		}
	}
	if (&prng{s: 4}).intn(10) >= 10 {
		t.Fatal("intn range")
	}
}
