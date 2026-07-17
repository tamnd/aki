package main

import (
	"math"
	"math/rand"
	"testing"
)

// interleaveRef is the bit-by-bit definition the magic-number ladder
// must match.
func interleaveRef(x, y uint32) uint64 {
	var out uint64
	for i := range 32 {
		out |= uint64(x>>i&1) << (2 * i)
		out |= uint64(y>>i&1) << (2*i + 1)
	}
	return out
}

func TestInterleave(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for range 10000 {
		x, y := rng.Uint32(), rng.Uint32()
		if interleave64(x, y) != interleaveRef(x, y) {
			t.Fatalf("interleave64(%x, %x) = %x, ref %x", x, y, interleave64(x, y), interleaveRef(x, y))
		}
		v := rng.Uint64()
		var wx, wy uint32
		for b := range 32 {
			wx |= uint32(v>>(2*b)&1) << b
			wy |= uint32(v>>(2*b+1)&1) << b
		}
		if compressEven(v) != wx || compressEven(v>>1) != wy {
			t.Fatalf("compressEven(%x) mismatch", v)
		}
	}
}

// The codec must be stable: a decoded midpoint re-encodes to the same
// 52-bit cell, for random coordinates across the legal ranges.
func TestCodecRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	for range 100000 {
		lon := geoLonMin + rng.Float64()*(geoLonMax-geoLonMin)
		lat := geoLatMin + rng.Float64()*(geoLatMax-geoLatMin)
		bits := geoEncode(lon, lat)
		if bits >= 1<<52 {
			t.Fatalf("encode(%v, %v) = %d, past 52 bits", lon, lat, bits)
		}
		mlon, mlat := geoDecode(bits)
		if got := geoEncode(mlon, mlat); got != bits {
			t.Fatalf("midpoint of %d re-encodes to %d", bits, got)
		}
	}
}

// The step estimator's pinned values, computed by hand from Redis's
// loop: doublings to MERCATOR_MAX minus two, minus the pole widening.
func TestEstimateStep(t *testing.T) {
	cases := []struct {
		r, lat float64
		want   int
	}{
		{100, 0, 17}, {1000, 0, 14}, {10000, 0, 10},
		{100000, 0, 7}, {500000, 0, 5},
		{100, 70, 16}, {100, 82, 15},
		{0, 0, 26}, {30000000, 0, 1},
	}
	for _, c := range cases {
		if got := estimateStep(c.r, c.lat); got != c.want {
			t.Fatalf("estimateStep(%v, %v) = %d, want %d", c.r, c.lat, got, c.want)
		}
	}
}

// One degree of latitude on Redis's sphere, and the zero and
// symmetry properties.
func TestDistance(t *testing.T) {
	oneDeg := 2 * math.Pi * earthR / 360
	if d := geoDist(0, 0, 0, 1); math.Abs(d-oneDeg) > 0.01 {
		t.Fatalf("1 degree lat = %v, want %v", d, oneDeg)
	}
	if d := geoDist(12.5, 41.9, 12.5, 41.9); d != 0 {
		t.Fatalf("self distance = %v", d)
	}
	a := geoDist(2.35, 48.85, -0.13, 51.51)
	b := geoDist(-0.13, 51.51, 2.35, 48.85)
	if math.Abs(a-b) > 1e-9 {
		t.Fatalf("asymmetric: %v vs %v", a, b)
	}
	// Paris to London on this radius is ~343.9 km; sanity band.
	if a < 330000 || a > 360000 {
		t.Fatalf("paris-london = %v", a)
	}
}

// The safety property every arm must hold: no point inside the
// radius escapes the cover. Brute-filter the decoded scores and
// demand every hit surfaces from the cover walk, at all three arms,
// across radii and latitude bands including the pole widening.
func TestCoverComplete(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for _, lat := range []float64{0, 45, 70} {
		m := buildModel(genPoints("uniform", 20000, lat, rng), 104)
		for _, r := range []float64{100, 1000, 10000, 100000, 500000} {
			for _, delta := range []int{-1, 0, 1} {
				for range 20 {
					clon := (rng.Float64()*2 - 1) * 2
					clat := lat + (rng.Float64()*2-1)*2
					got := map[int]bool{}
					res := m.search(clon, clat, r, delta, func(i int) { got[i] = true })
					brute := 0
					for i, sc := range m.scores {
						plon, plat := geoDecode(sc)
						if geoDist(clon, clat, plon, plat) <= r {
							brute++
							if !got[i] {
								t.Fatalf("lat %v r %v delta %d: point %d inside radius missed by cover", lat, r, delta, i)
							}
						}
					}
					if res.results != brute {
						t.Fatalf("lat %v r %v delta %d: cover results %d, brute %d", lat, r, delta, res.results, brute)
					}
				}
			}
		}
	}
}
