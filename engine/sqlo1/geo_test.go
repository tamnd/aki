package sqlo1

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

// TestGeoInterleaveLadder pins the magic-number ladder against a
// bit-by-bit loop reference, and the compress against the ladder.
func TestGeoInterleaveLadder(t *testing.T) {
	ref := func(x, y uint32) uint64 {
		var out uint64
		for b := 0; b < 32; b++ {
			out |= uint64(x>>b&1) << (2 * b)
			out |= uint64(y>>b&1) << (2*b + 1)
		}
		return out
	}
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 10000; i++ {
		x, y := rng.Uint32(), rng.Uint32()
		got, want := geoInterleave64(x, y), ref(x, y)
		if got != want {
			t.Fatalf("interleave(%#x, %#x) = %#x, want %#x", x, y, got, want)
		}
		if gx := geoCompressEven(got); gx != x {
			t.Fatalf("compressEven(interleave(%#x, %#x)) = %#x", x, y, gx)
		}
		if gy := geoCompressEven(got >> 1); gy != y {
			t.Fatalf("compressEven(interleave>>1) = %#x, want %#x", gy, y)
		}
	}
}

// TestGeoCodecPinned pins the 52-bit scores and decoded midpoints a
// live Redis 8.8.0 produced for the doc fixture, the Z-I6 contract:
// geo data written here round-trips against Redis bit-identically.
func TestGeoCodecPinned(t *testing.T) {
	cases := []struct {
		lon, lat float64
		bits     uint64
		dlon     string
		dlat     string
	}{
		{13.361389, 38.115556, 3479099956230698, "13.361389338970184", "38.1155563954963"},
		{15.087269, 37.502669, 3479447370796909, "15.087267458438873", "37.50266842333162"},
		{13.5, 38.2, 3479101704338477, "13.500000536441803", "38.200000630919675"},
		{1, 41, 3471282553249203, "0.9999999403953552", "41.00000063735273"},
	}
	for _, c := range cases {
		if got := geoEncode(c.lon, c.lat); got != c.bits {
			t.Fatalf("geoEncode(%v, %v) = %d, want %d", c.lon, c.lat, got, c.bits)
		}
		lon, lat := geoDecode(c.bits)
		if s := strconv.FormatFloat(lon, 'g', -1, 64); s != c.dlon {
			t.Fatalf("decode(%d) lon = %s, want %s", c.bits, s, c.dlon)
		}
		if s := strconv.FormatFloat(lat, 'g', -1, 64); s != c.dlat {
			t.Fatalf("decode(%d) lat = %s, want %s", c.bits, s, c.dlat)
		}
	}
}

// TestGeoRoundTrip drives random coordinates through encode, midpoint
// decode, and re-encode: the midpoint must land back in its own cell,
// which is what makes GEOPOS-then-GEOADD stable.
func TestGeoRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 100000; i++ {
		lon := rng.Float64()*360 - 180
		lat := rng.Float64()*2*geoLatMax - geoLatMax
		bits := geoEncode(lon, lat)
		mlon, mlat := geoDecode(bits)
		if again := geoEncode(mlon, mlat); again != bits {
			t.Fatalf("re-encode moved cell: (%v, %v) -> %d, midpoint (%v, %v) -> %d", lon, lat, bits, mlon, mlat, again)
		}
		if math.Abs(mlon-lon) > 360/float64(uint64(1)<<geoStep)+1e-9 {
			t.Fatalf("midpoint lon %v too far from %v", mlon, lon)
		}
	}
}

// TestGeoEstimateStepPinned pins geohashEstimateStepsByRadius on a
// hand-computed table, the pole widening included, plus the clamps.
func TestGeoEstimateStepPinned(t *testing.T) {
	cases := []struct {
		r    float64
		lat  float64
		want int
	}{
		{100, 0, 17},
		{1000, 0, 14},
		{10000, 0, 10},
		{100000, 0, 7},
		{500000, 0, 5},
		{100, 70, 16},
		{100, 82, 15},
		{100, -82, 15},
		{0, 0, 26},
		{30000000, 0, 1},
	}
	for _, c := range cases {
		if got := geoEstimateStep(c.r, c.lat); got != c.want {
			t.Fatalf("geoEstimateStep(%v, %v) = %d, want %d", c.r, c.lat, got, c.want)
		}
	}
}

// TestGeoDistKnown bands the haversine on a known city pair and pins
// the two exact shortcuts: self distance and the same-longitude leg.
func TestGeoDistKnown(t *testing.T) {
	paris := [2]float64{2.3522, 48.8566}
	london := [2]float64{-0.1276, 51.5074}
	d := geoDist(paris[0], paris[1], london[0], london[1])
	if d < 340e3 || d > 348e3 {
		t.Fatalf("Paris-London = %v m, want ~344 km", d)
	}
	if d := geoDist(1, 2, 1, 2); d != 0 {
		t.Fatalf("self distance = %v", d)
	}
	if got, want := geoDist(10, 0, 10, 1), geoEarthR*geoDeg2rad(1); got != want {
		t.Fatalf("same-lon leg = %v, want %v", got, want)
	}
	if got, want := geoLatDist(0, 1), geoEarthR*geoDeg2rad(1); got != want {
		t.Fatalf("geoLatDist = %v, want %v", got, want)
	}
}

// TestGeoSearchOracle drives GeoSearch against a brute filter over
// every stored point, radius and box shapes, clustered and uniform
// points, the antimeridian wrap included, then replays the same
// shapes on a cold reopen so the cover walk crosses real run reads.
func TestGeoSearchOracle(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	rng := rand.New(rand.NewSource(9))
	key := []byte("geo")

	type pt struct{ lon, lat float64 }
	pts := map[string]pt{}
	for i := 0; i < 3000; i++ {
		var lon, lat float64
		switch i % 4 {
		case 0:
			lon, lat = 13+rng.Float64()*4-2, 38+rng.Float64()*4-2
		case 1:
			lon, lat = -70+rng.Float64()*2-1, -33+rng.Float64()*2-1
		case 2:
			if i%8 == 2 {
				lon = 179.5 + rng.Float64()*0.5
			} else {
				lon = -180 + rng.Float64()*0.5
			}
			lat = rng.Float64()*2 - 1
		default:
			lon = rng.Float64()*360 - 180
			lat = rng.Float64()*2*geoLatMax - geoLatMax
		}
		m := fmt.Sprintf("m%04d", i)
		bits := geoEncode(lon, lat)
		if _, _, _, _, err := r.z.ZAdd(ctx, key, []byte(m), float64(bits), ZAddFlags{}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
		dlon, dlat := geoDecode(bits)
		pts[m] = pt{dlon, dlat}
	}

	shapes := []geoShape{
		{lon: 13.36, lat: 38.11, radius: 200e3},
		{lon: 13.36, lat: 38.11, radius: 5e3},
		{lon: -70, lat: -33, radius: 100e3},
		{lon: 0, lat: 84, radius: 300e3},
		{lon: 179.9, lat: 0, radius: 60e3},
		{lon: -179.9, lat: 0, radius: 60e3},
		{lon: 13.36, lat: 38.11, byBox: true, w: 300e3, h: 120e3},
		{lon: -70, lat: -33, byBox: true, w: 50e3, h: 400e3},
		{lon: 179.95, lat: 0.1, byBox: true, w: 80e3, h: 80e3},
	}

	check := func(z *ZSet, sh geoShape) {
		t.Helper()
		got := map[string]float64{}
		err := z.GeoSearch(ctx, key, sh, func(m []byte, bits uint64, lon, lat, dist float64) bool {
			if _, dup := got[string(m)]; dup {
				t.Fatalf("shape %+v emitted %q twice", sh, m)
			}
			got[string(m)] = dist
			return true
		})
		if err != nil {
			t.Fatalf("GeoSearch(%+v): %v", sh, err)
		}
		want := map[string]float64{}
		for m, p := range pts {
			if d, ok := sh.contains(p.lon, p.lat); ok {
				want[m] = d
			}
		}
		if len(got) != len(want) {
			t.Fatalf("shape %+v: %d hits, oracle wants %d", sh, len(got), len(want))
		}
		for m, d := range want {
			gd, ok := got[m]
			if !ok {
				t.Fatalf("shape %+v missed %q at dist %v", sh, m, d)
			}
			if gd != d {
				t.Fatalf("shape %+v dist(%q) = %v, oracle %v", sh, m, gd, d)
			}
		}
	}

	for _, sh := range shapes {
		check(r.z, sh)
	}

	if err := r.tr.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	z2 := r.reopen()
	for _, sh := range shapes {
		check(z2, sh)
	}
}

// TestGeoSearchEarlyStop pins the ANY door's storage half: an emit
// that answers false ends the whole walk with no error and nothing
// further surfaces.
func TestGeoSearchEarlyStop(t *testing.T) {
	r := newZsetRig(t)
	ctx := context.Background()
	key := []byte("geo")
	rng := rand.New(rand.NewSource(21))
	for i := 0; i < 500; i++ {
		lon, lat := 13+rng.Float64(), 38+rng.Float64()
		m := fmt.Sprintf("m%03d", i)
		if _, _, _, _, err := r.z.ZAdd(ctx, key, []byte(m), float64(geoEncode(lon, lat)), ZAddFlags{}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
	}
	n := 0
	err := r.z.GeoSearch(ctx, key, geoShape{lon: 13.5, lat: 38.5, radius: 500e3}, func(m []byte, bits uint64, lon, lat, dist float64) bool {
		n++
		return n < 5
	})
	if err != nil {
		t.Fatalf("GeoSearch: %v", err)
	}
	if n != 5 {
		t.Fatalf("emitted %d times after stop at 5", n)
	}
}
