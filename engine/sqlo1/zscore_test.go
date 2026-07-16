package sqlo1

import (
	"math"
	"math/rand/v2"
	"testing"
)

// TestZScoreSortableOrder is the doc 09 section 2.3 ordering property
// test: for any two non-NaN doubles, the u64 order of the encodings
// must match Redis's score comparison, which is the C < and ==
// operators on doubles, so -0 == +0 and the infinities bracket the
// line. The corpus is the explicit boundary table plus random bit
// patterns, so denormals, exponent edges, and the zero pair are all
// exercised.
func TestZScoreSortableOrder(t *testing.T) {
	corpus := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1e100,
		-2.5,
		-1.0,
		-math.SmallestNonzeroFloat64,
		math.Copysign(0, -1),
		0,
		math.SmallestNonzeroFloat64,
		1.0,
		2.5,
		1e100,
		math.MaxFloat64,
		math.Inf(1),
	}
	rng := rand.New(rand.NewPCG(9, 2064))
	for len(corpus) < 4000 {
		f := math.Float64frombits(rng.Uint64())
		if !math.IsNaN(f) {
			corpus = append(corpus, f)
		}
	}
	for i, a := range corpus {
		for _, b := range corpus[i:] {
			ea, eb := zScoreSortable(a), zScoreSortable(b)
			if (a < b) != (ea < eb) {
				t.Fatalf("order broke: %v < %v is %v, encodings %#x %#x", a, b, a < b, ea, eb)
			}
			if (a == b) != (ea == eb) {
				t.Fatalf("equality broke: %v == %v is %v, encodings %#x %#x", a, b, a == b, ea, eb)
			}
		}
	}
}

// TestZScoreSortableRoundTrip: decoding inverts encoding for every
// non-NaN double, with the single deliberate exception that -0 comes
// back as +0, Redis's comparison and printing semantics.
func TestZScoreSortableRoundTrip(t *testing.T) {
	check := func(f float64) {
		got := zScoreFromSortable(zScoreSortable(f))
		want := f
		if f == 0 {
			want = 0
		}
		if got != want || math.Signbit(got) != math.Signbit(want) {
			t.Fatalf("round trip of %v (bits %#x) came back %v", f, math.Float64bits(f), got)
		}
	}
	for _, f := range []float64{
		math.Inf(-1), -math.MaxFloat64, -1.5, -math.SmallestNonzeroFloat64,
		math.Copysign(0, -1), 0,
		math.SmallestNonzeroFloat64, 1.5, math.MaxFloat64, math.Inf(1),
	} {
		check(f)
	}
	rng := rand.New(rand.NewPCG(10, 2064))
	for range 100000 {
		f := math.Float64frombits(rng.Uint64())
		if !math.IsNaN(f) {
			check(f)
		}
	}
	// The zero pair encodes identically and decodes to the positive
	// zero, so no distinct -0 image ever reaches a run.
	if zScoreSortable(0) != zScoreSortable(math.Copysign(0, -1)) {
		t.Fatal("the zero pair encodes to two images")
	}
	if math.Signbit(zScoreFromSortable(zScoreSortable(math.Copysign(0, -1)))) {
		t.Fatal("-0 survived the codec")
	}
}
