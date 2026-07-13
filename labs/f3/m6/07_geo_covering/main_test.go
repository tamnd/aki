package main

import (
	"math"
	"testing"
)

// The lab duplicates the engine's geohash kernel, so these tests anchor the copy
// to the same Sicily goldens the engine unit tests use and prove the covering
// walk returns exactly what the full scan does across shapes and cardinalities.

const (
	palermoLon, palermoLat = 13.361389, 38.115556
	cataniaLon, cataniaLat = 15.087269, 37.502669
	palermoScore           = 3479099956230698
	cataniaScore           = 3479447370796909
)

// TestCodecGolden checks the duplicated interleave matches Redis's scores and
// the documented Palermo-Catania distance, so the kernel under test is the same
// one the engine ships.
func TestCodecGolden(t *testing.T) {
	if got := geoEncode(palermoLon, palermoLat); got != palermoScore {
		t.Fatalf("Palermo score = %d, want %d", got, palermoScore)
	}
	if got := geoEncode(cataniaLon, cataniaLat); got != cataniaScore {
		t.Fatalf("Catania score = %d, want %d", got, cataniaScore)
	}
	lon1, lat1 := geoDecode(palermoScore)
	lon2, lat2 := geoDecode(cataniaScore)
	if d := geoDistM(lon1, lat1, lon2, lat2); math.Abs(d-166274.1516) > 0.01 {
		t.Fatalf("Palermo-Catania distance = %.4f, want 166274.1516", d)
	}
}

// TestCoveringMatchesFullScan is the load-bearing check: over a spread of
// cardinalities, region sizes, and radii, the covering walk must return the
// byte-identical survivor set the full scan does. Any cell the estimator drops
// too early, any neighbor the exclusion prunes wrongly, or any range-align slip
// shows up here as a divergence.
func TestCoveringMatchesFullScan(t *testing.T) {
	for _, n := range []int{200, 2000, 20000} {
		for _, side := range []float64{2, 10, 40} {
			gs := makeSet(n, 15, 37, side)
			for _, rk := range []float64{1, 8, 25, 75, 200} {
				rm := rk * 1000
				want, _ := fullScan(gs, 15, 37, rm)
				got, _ := coveringSearch(gs, 15, 37, rm)
				if !intsEqual(want, got) {
					t.Fatalf("n=%d side=%.0f r=%.0fkm: covering %d survivors, full scan %d",
						n, side, rk, len(got), len(want))
				}
			}
		}
	}
}

// TestCoveringMatchesAtLatitudes checks the equivalence holds where the cells
// distort: the estimator's pole widening and the bounding box's cosine term must
// still cover the true circle.
func TestCoveringMatchesAtLatitudes(t *testing.T) {
	for _, lat := range []float64{-70, -40, 0, 40, 70, 80} {
		gs := makeSet(20000, 20, lat, 12)
		for _, rk := range []float64{5, 40, 150} {
			rm := rk * 1000
			want, _ := fullScan(gs, 20, lat, rm)
			got, _ := coveringSearch(gs, 20, lat, rm)
			if !intsEqual(want, got) {
				t.Fatalf("lat=%.0f r=%.0fkm: covering %d survivors, full scan %d",
					lat, rk, len(got), len(want))
			}
		}
	}
}

// TestCoverExaminesFewer checks the point of the exercise: over a fixed-size
// search on a large map, the covering walk examines far fewer candidates than
// the full scan touches members.
func TestCoverExaminesFewer(t *testing.T) {
	gs := makeSet(200000, 15, 37, math.Sqrt(200000.0/400.0))
	_, scan := fullScan(gs, 15, 37, 30000)
	_, cover := coveringSearch(gs, 15, 37, 30000)
	if cover >= scan/10 {
		t.Fatalf("covering examined %d candidates, full scan %d: cover not sublinear enough", cover, scan)
	}
}
