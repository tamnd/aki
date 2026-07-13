package zset

import (
	"math"
	"testing"
)

// The Sicily corpus from the Redis GEO documentation, the canonical geo
// interop vectors. Their scores, distance, and geohash strings are the
// bit-for-bit values a same-version Redis returns, so matching them pins the
// codec without a live server.
const (
	palermoLon, palermoLat = 13.361389, 38.115556
	cataniaLon, cataniaLat = 15.087269, 37.502669
	palermoScore           = 3479099956230698
	cataniaScore           = 3479447370796909
)

// TestGeoEncodeGolden checks the interleave produces Redis's exact scores.
func TestGeoEncodeGolden(t *testing.T) {
	if got := geoEncode(palermoLon, palermoLat); got != palermoScore {
		t.Fatalf("Palermo score = %d, want %d", got, palermoScore)
	}
	if got := geoEncode(cataniaLon, cataniaLat); got != cataniaScore {
		t.Fatalf("Catania score = %d, want %d", got, cataniaScore)
	}
}

// TestGeoDecodeRoundtrip checks a decode lands back inside the source cell: the
// residual is bounded by the cell extent (a fraction of a degree at step 26),
// the near-but-not-exact GEOPOS behavior. Points at the exact ±180/±85 seam are
// left out: an offset of exactly 1.0 quantizes to the wrapped cell, the same
// boundary artifact Redis carries.
func TestGeoDecodeRoundtrip(t *testing.T) {
	for _, tc := range []struct{ lon, lat float64 }{
		{palermoLon, palermoLat},
		{cataniaLon, cataniaLat},
		{0, 0}, {-179.9, -84.9}, {179.9, 84.9}, {-122.4194, 37.7749},
	} {
		lon, lat := geoDecode(geoEncode(tc.lon, tc.lat))
		if math.Abs(lon-tc.lon) > 1e-3 || math.Abs(lat-tc.lat) > 1e-3 {
			t.Errorf("decode(encode(%v,%v)) = %v,%v, off by more than a cell", tc.lon, tc.lat, lon, lat)
		}
	}
}

// TestGeoDistGolden checks the haversine matches Redis's documented Palermo to
// Catania distance to the four decimals GEODIST reports.
func TestGeoDistGolden(t *testing.T) {
	lon1, lat1 := geoDecode(palermoScore)
	lon2, lat2 := geoDecode(cataniaScore)
	d := geoDistM(lon1, lat1, lon2, lat2)
	if math.Abs(d-166274.1516) > 0.01 {
		t.Fatalf("Palermo-Catania distance = %.4f, want 166274.1516", d)
	}
}

// TestGeoHashGolden checks the standard base-32 render matches Redis's GEOHASH
// output for the corpus, the geohash.org interop strings.
func TestGeoHashGolden(t *testing.T) {
	for _, tc := range []struct {
		score int
		want  string
	}{
		{palermoScore, "sqc8b49rny0"},
		{cataniaScore, "sqdtr74hyu0"},
	} {
		lon, lat := geoDecode(uint64(tc.score))
		got := string(geoHashString(lon, lat, nil))
		if got != tc.want {
			t.Errorf("geohash(%d) = %q, want %q", tc.score, got, tc.want)
		}
	}
}

// TestGeoUnit checks the unit multipliers and the case-insensitive spellings.
func TestGeoUnit(t *testing.T) {
	for _, tc := range []struct {
		tok  string
		want float64
	}{
		{"m", 1}, {"M", 1}, {"km", 1000}, {"KM", 1000},
		{"ft", 0.3048}, {"mi", 1609.34},
	} {
		if got, ok := geoUnit([]byte(tc.tok)); !ok || got != tc.want {
			t.Errorf("geoUnit(%q) = %v,%v, want %v,true", tc.tok, got, ok, tc.want)
		}
	}
	if _, ok := geoUnit([]byte("yards")); ok {
		t.Error("geoUnit(yards) accepted an unsupported unit")
	}
}
