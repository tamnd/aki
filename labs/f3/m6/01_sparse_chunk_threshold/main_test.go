package main

import "testing"

// The lab's claims as invariants, so CI catches a resident-model regression: a
// single high bit costs one chunk not the extent, a gap chunk costs only its
// directory pointer, aki stays under half the rival below half chunk coverage and
// only reaches break-even at full density, and the full-density directory tax is a
// twentieth of a percent.

// TestSingleHighBitIsOneChunk pins the SETBIT k <big> 1 pathology: one set bit at
// the offset cap holds one live chunk and a directory spanning the extent, a sliver
// of the 512 MiB a rival allocates.
func TestSingleHighBitIsOneChunk(t *testing.T) {
	l := fromBits([]int64{1<<32 - 1})
	if l.liveChunks != 1 {
		t.Fatalf("one high bit touched %d chunks, want 1", l.liveChunks)
	}
	if l.extent != 512<<20 {
		t.Fatalf("extent %d, want 512 MiB", l.extent)
	}
	a, rv := akiBytes(l), rivalBytes(l)
	if r := ratio(a, rv); r > 0.001 {
		t.Fatalf("single-bit aki/rival = %.4f, want << 0.001 (%d vs %d)", r, a, rv)
	}
}

// TestGapChunksCostOnlyDirectory pins section 2.3: a gap chunk is a hole that costs
// only its 16-byte directory pointer, so aki for a sparse layout equals the same
// layout with the holes charged at a pointer each, not a whole chunk each.
func TestGapChunksCostOnlyDirectory(t *testing.T) {
	// A layout spanning 100 chunks with two live: the 98 holes must add only their
	// pointers over a hypothetical two-chunk value, not 98 chunk runs.
	sparse := layout{extent: 100 * chunkSize, nChunks: 100, liveChunks: 2}
	twoLive := int64(recBytes + 100*ptrSize + 2*(chunkSize+runHdr))
	if got := akiBytes(sparse); got != twoLive {
		t.Fatalf("sparse aki = %d, want %d (holes cost only a pointer)", got, twoLive)
	}
	// Charging the holes as full chunks would be the dense cost; the hole scheme must
	// beat it by 98 chunk runs.
	dense := int64(recBytes + 100*ptrSize + 100*(chunkSize+runHdr))
	saved := dense - twoLive
	if want := int64(98) * (chunkSize + runHdr); saved != want {
		t.Fatalf("holes saved %d bytes, want %d", saved, want)
	}
}

// TestDensityCrossover pins the memory bar and the break-even: below half chunk
// coverage aki holds under half the rival; at full coverage it only reaches
// break-even, the directory tax and no more.
func TestDensityCrossover(t *testing.T) {
	nc := int64(8192) // a 512 MiB extent
	ext := nc * chunkSize
	// Just under half coverage: aki under half the rival.
	lo := layout{extent: ext, nChunks: nc, liveChunks: nc * 49 / 100}
	if r := ratio(akiBytes(lo), rivalBytes(lo)); r >= 0.5 {
		t.Fatalf("49%% coverage aki/rival = %.4f, want < 0.5", r)
	}
	// Full coverage: aki at or just over break-even, never a rout.
	full := layout{extent: ext, nChunks: nc, liveChunks: nc}
	if r := ratio(akiBytes(full), rivalBytes(full)); r < 1.0 || r > 1.01 {
		t.Fatalf("full coverage aki/rival = %.4f, want in [1.0, 1.01]", r)
	}
}

// TestFullDensityOverheadNegligible pins that the hole scheme costs almost nothing
// when there is nothing to save: at full density the directory plus run headers are
// under a tenth of a percent over the rival's extent.
func TestFullDensityOverheadNegligible(t *testing.T) {
	nc := chunkCount(512 << 20)
	l := layout{extent: nc * chunkSize, nChunks: nc, liveChunks: nc}
	a, rv := akiBytes(l), rivalBytes(l)
	over := float64(a-rv) / float64(rv)
	if over < 0 || over > 0.001 {
		t.Fatalf("full-density overhead %.4f%%, want under 0.1%%", over*100)
	}
}

// TestCrossoverMatchesModel pins the stated thresholds against the per-chunk model:
// the 0.5x bar sits at ~50% coverage and break-even at ~100%.
func TestCrossoverMatchesModel(t *testing.T) {
	if h := crossover(0.5); h < 0.49 || h > 0.51 {
		t.Fatalf("0.5x crossover at %.3f coverage, want ~0.50", h)
	}
	if o := crossover(1.0); o < 0.99 {
		t.Fatalf("break-even crossover at %.3f coverage, want ~1.0", o)
	}
}

// TestLayoutLiveChunksMatchDistinct pins the model bookkeeping: the live-chunk count
// fromBits records equals the distinct chunk indices the offsets touch.
func TestLayoutLiveChunksMatchDistinct(t *testing.T) {
	bits := setBits(50_000, 1<<32-1, 7)
	l := fromBits(bits)
	if got := int64(distinctChunks(bits)); l.liveChunks != got {
		t.Fatalf("layout live chunks %d, distinct %d", l.liveChunks, got)
	}
}
