package main

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
)

// specialScores are the edge doubles the codec must get exactly right: the
// signed zeros, the infinities, the float64 extremes, and a spread of ordinary
// magnitudes. NaN is deliberately absent because the parser rejects it before
// the encoder ever sees it (doc 12 section 3.1).
var specialScores = []float64{
	math.Inf(-1),
	-math.MaxFloat64,
	-1e300, -1e6, -3.5, -1, -math.SmallestNonzeroFloat64,
	math.Copysign(0, -1), // -0.0
	0.0,                  // +0.0
	math.SmallestNonzeroFloat64, 1, 3.5, 17, 1e6, 1e300,
	math.MaxFloat64,
	math.Inf(1),
}

// TestRoundTripNaive checks the transform is exactly invertible for every
// non-NaN float64: scoreFromKey(scoreKeyNaive(f)) reproduces f bit for bit,
// including the sign of zero. This is the bijection the range-bound decode and
// the cold-chunk directory rely on (doc 12 sections 3.1, 6.5, 8.2).
func TestRoundTripNaive(t *testing.T) {
	check := func(f float64) {
		if math.IsNaN(f) {
			return
		}
		got := scoreFromKey(scoreKeyNaive(f))
		if math.Float64bits(got) != math.Float64bits(f) {
			t.Fatalf("round-trip %v (%#016x) -> %v (%#016x)",
				f, math.Float64bits(f), got, math.Float64bits(got))
		}
	}
	for _, f := range specialScores {
		check(f)
	}
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 2_000_000; i++ {
		// Draw uniformly across the whole 64-bit pattern space, skipping NaN,
		// so denormals, huge exponents and random mantissas all get exercised.
		b := r.Uint64()
		f := math.Float64frombits(b)
		check(f)
	}
}

// TestTotalOrder checks the tree's key order agrees with the Redis zset total
// order for every pair drawn from the special and random scores: keyLess over
// scoreKey must equal zsetLess over the raw scores. This is the property that
// lets the tree compare keys instead of scores and still be correct.
func TestTotalOrder(t *testing.T) {
	ma := []byte("alpha")
	mb := []byte("beta")
	same := []byte("same")
	pairs := func(sa, sb float64) {
		for _, mp := range [][2][]byte{{ma, mb}, {mb, ma}, {same, same}} {
			want := zsetLess(sa, mp[0], sb, mp[1])
			got := keyLess(scoreKey(sa), mp[0], scoreKey(sb), mp[1])
			if got != want {
				t.Fatalf("order sa=%v sb=%v ma=%q mb=%q: key=%v ref=%v",
					sa, sb, mp[0], mp[1], got, want)
			}
		}
	}
	for _, sa := range specialScores {
		for _, sb := range specialScores {
			pairs(sa, sb)
		}
	}
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 500_000; i++ {
		sa := math.Float64frombits(r.Uint64())
		sb := math.Float64frombits(r.Uint64())
		if math.IsNaN(sa) || math.IsNaN(sb) {
			continue
		}
		pairs(sa, sb)
	}
}

// TestSignedZeroCollapses pins the codec's one real trap. The normalized
// scoreKey maps -0.0 and +0.0 to one key so their order is decided by member
// bytes, matching Redis. The literal scoreKeyNaive from the doc's code block
// does not: it places -0.0 one key below +0.0, which flips the order of two
// members whenever both zeros are present. The test asserts both the fix and
// the hazard so the dual-write slice cannot quietly drop the normalization.
func TestSignedZeroCollapses(t *testing.T) {
	if scoreKey(math.Copysign(0, -1)) != scoreKey(0.0) {
		t.Fatal("scoreKey did not collapse -0.0 and +0.0")
	}
	// Reference order at equal score is by member: "b" does not sort before
	// "a" even when "b" carries -0.0 and "a" carries +0.0.
	if zsetLess(math.Copysign(0, -1), []byte("b"), 0.0, []byte("a")) {
		t.Fatal("reference order wrong for signed zero")
	}
	// The normalized key form agrees with the reference.
	if keyLess(scoreKey(math.Copysign(0, -1)), []byte("b"), scoreKey(0.0), []byte("a")) {
		t.Fatal("normalized key ordered -0.0,\"b\" before +0.0,\"a\"")
	}
	// The naive form disagrees, which is exactly why normalization is required.
	if !keyLess(scoreKeyNaive(math.Copysign(0, -1)), []byte("b"), scoreKeyNaive(0.0), []byte("a")) {
		t.Fatal("naive transform unexpectedly honored signed-zero order; " +
			"the hazard this lab documents would be gone")
	}
}

// TestInfinityOrder checks the infinities land at the ends of the key space:
// -inf below every finite score, +inf above every finite score.
func TestInfinityOrder(t *testing.T) {
	negInf := scoreKey(math.Inf(-1))
	posInf := scoreKey(math.Inf(1))
	for _, f := range specialScores {
		if math.IsInf(f, 0) {
			continue
		}
		k := scoreKey(f)
		if negInf >= k {
			t.Fatalf("-inf key %#016x not below %v key %#016x", negInf, f, k)
		}
		if posInf <= k {
			t.Fatalf("+inf key %#016x not above %v key %#016x", posInf, f, k)
		}
	}
	if negInf >= posInf {
		t.Fatal("-inf key not below +inf key")
	}
}

// TestGeoExact is the M6 groundwork: a 52-bit geohash integer stored as a
// float64 (Redis GEO's representation) must survive the codec exactly and order
// as an integer, because GEOSEARCH decomposes to score-range scans on this key
// (doc 12 section 7). The test sweeps the integer range plus the boundaries and
// checks both exact round-trip and monotone order.
func TestGeoExact(t *testing.T) {
	const geoBits = 52
	vals := []uint64{0, 1, 2, 1 << 25, 1 << 51, (1 << geoBits) - 2, (1 << geoBits) - 1}
	r := rand.New(rand.NewSource(9))
	for i := 0; i < 200_000; i++ {
		vals = append(vals, r.Uint64()&((1<<geoBits)-1))
	}
	for _, g := range vals {
		f := float64(g)
		if uint64(f) != g {
			t.Fatalf("geohash %d not exact as float64", g)
		}
		back := scoreFromKey(scoreKeyNaive(f))
		if back != f || uint64(back) != g {
			t.Fatalf("geohash %d round-trip to %v", g, back)
		}
	}
	// Order: for any two distinct geohashes, integer order equals key order.
	for i := 0; i < 200_000; i++ {
		a := r.Uint64() & ((1 << geoBits) - 1)
		b := r.Uint64() & ((1 << geoBits) - 1)
		wantLess := a < b
		gotLess := scoreKey(float64(a)) < scoreKey(float64(b))
		if a != b && gotLess != wantLess {
			t.Fatalf("geohash order %d vs %d: key=%v want=%v", a, b, gotLess, wantLess)
		}
	}
}

// TestDescendKernelsAgree checks the three descent kernels count the same path
// length for the same target, so a difference in the timed loop is a difference
// in representation cost and not in behavior.
func TestDescendKernelsAgree(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	for _, tied := range []bool{false, true} {
		for _, sz := range []int{8, 24} {
			tr := buildTree(r, 4, sz, tied, true)
			for i := 0; i < 5000; i++ {
				var sc float64
				if tied {
					sc = 0
				} else {
					sc = r.NormFloat64() * 1e6
				}
				m := makeMember(r, sz, true)
				k := scoreKey(sc)
				blob := make([]byte, 8+sz)
				binary.BigEndian.PutUint64(blob, k)
				copy(blob[8:], m)
				a := descendRaw(tr, sc, m)
				b := descendU64(tr, k, m)
				c := descendBlob(tr, blob)
				if a != b || b != c {
					t.Fatalf("kernels disagree tied=%v sz=%d: raw=%d u64=%d blob=%d",
						tied, sz, a, b, c)
				}
			}
		}
	}
}
