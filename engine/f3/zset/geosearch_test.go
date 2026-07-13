package zset

import (
	"sort"
	"testing"
)

// The covering-range engine over the Sicily corpus (spec 2064/f3/15 section 11).
// These pin the geohash math a live server never sees directly, the step
// estimation, the nine-cell cover, and the exact-distance filter, so a same
// version Redis and this port agree on which members a shape contains.

// geoSetSicily builds a geo set with the two corpus members at their encoded
// scores, the substrate GEOSEARCH walks.
func geoSetSicily() *zset {
	z := newZset()
	z.update([]byte("Palermo"), float64(geoEncode(palermoLon, palermoLat)), flags{})
	z.update([]byte("Catania"), float64(geoEncode(cataniaLon, cataniaLat)), flags{})
	return z
}

// hitNames returns the survivor members in the order geoSearchHits produced them.
func hitNames(hits []geoHit) []string {
	names := make([]string, len(hits))
	for i, h := range hits {
		names[i] = string(h.member)
	}
	return names
}

// TestGeoEstimateSteps checks the step estimator brackets a radius the way
// Redis's geohashEstimateStepsByRadius does: a smaller reach picks a finer step,
// and the poles pull the step coarser.
func TestGeoEstimateSteps(t *testing.T) {
	if s := geoEstimateSteps(200000, 37); s < 1 || s > 26 {
		t.Fatalf("200 km step = %d, out of range", s)
	}
	fine := geoEstimateSteps(1000, 37)
	coarse := geoEstimateSteps(500000, 37)
	if fine <= coarse {
		t.Fatalf("finer reach should give a larger step: 1km=%d 500km=%d", fine, coarse)
	}
	// A pole latitude widens the cell, so the step is no finer than at the equator.
	if geoEstimateSteps(200000, 85) > geoEstimateSteps(200000, 0) {
		t.Fatal("polar step should not exceed the equatorial step for the same reach")
	}
}

// TestGeoSearchRadius checks a radius search from (15,37) returns both corpus
// members within 200 km and neither within a tight 100 km (which excludes
// Palermo at 190 km).
func TestGeoSearchRadius(t *testing.T) {
	z := geoSetSicily()

	wide := geoSearchHits(z, geoShape{lon: 15, lat: 37, radiusM: 200000, toMeters: 1})
	got := hitNames(wide)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "Catania" || got[1] != "Palermo" {
		t.Fatalf("200 km radius survivors = %v, want [Catania Palermo]", got)
	}

	tight := geoSearchHits(z, geoShape{lon: 15, lat: 37, radiusM: 100000, toMeters: 1})
	got = hitNames(tight)
	if len(got) != 1 || got[0] != "Catania" {
		t.Fatalf("100 km radius survivors = %v, want [Catania]", got)
	}
}

// TestGeoSearchDistances checks the exact-filter distances match the haversine
// goldens, so WITHDIST reports Redis's meters.
func TestGeoSearchDistances(t *testing.T) {
	z := geoSetSicily()
	hits := geoSearchHits(z, geoShape{lon: 15, lat: 37, radiusM: 200000, toMeters: 1})
	byName := map[string]float64{}
	for _, h := range hits {
		byName[string(h.member)] = h.distM
	}
	if d := byName["Catania"]; d < 56441 || d > 56442 {
		t.Errorf("Catania distance = %.4f, want ~56441.2579", d)
	}
	if d := byName["Palermo"]; d < 190442 || d > 190443 {
		t.Errorf("Palermo distance = %.4f, want ~190442.4298", d)
	}
}

// TestGeoSearchBox checks a box search applies the latitude and longitude
// half-extent gates: a box wide and tall enough for both members keeps both, and
// a short box drops the far-north Palermo.
func TestGeoSearchBox(t *testing.T) {
	z := geoSetSicily()

	wide := geoSearchHits(z, geoShape{lon: 15, lat: 37, box: true, widthM: 400000, heightM: 400000, toMeters: 1})
	got := hitNames(wide)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "Catania" || got[1] != "Palermo" {
		t.Fatalf("400 km box survivors = %v, want [Catania Palermo]", got)
	}

	// Palermo sits ~123 km north of the center; a 200 km-tall box reaches only
	// 100 km north and excludes it, while Catania at ~56 km stays.
	short := geoSearchHits(z, geoShape{lon: 15, lat: 37, box: true, widthM: 400000, heightM: 200000, toMeters: 1})
	got = hitNames(short)
	if len(got) != 1 || got[0] != "Catania" {
		t.Fatalf("short box survivors = %v, want [Catania]", got)
	}
}

// TestGeoCoveringUsesNeighbors checks the cover keeps its center cell and only
// prunes neighbors when the step is fine enough for the exclusion, so a coarse
// large-radius search never zeroes the center.
func TestGeoCoveringUsesNeighbors(t *testing.T) {
	step, cells := geoCovering(geoShape{lon: 15, lat: 37, radiusM: 200000, toMeters: 1})
	if !cells[idxCenter].used {
		t.Fatal("center cell must always be used")
	}
	if step < 1 || step > 26 {
		t.Fatalf("cover step = %d, out of range", step)
	}
	ranges := geoMergeRanges(step, cells)
	if len(ranges) == 0 {
		t.Fatal("cover produced no score ranges")
	}
	for i := 1; i < len(ranges); i++ {
		if ranges[i].min < ranges[i-1].maxExcl {
			t.Fatalf("merged ranges overlap: %v", ranges)
		}
	}
}
