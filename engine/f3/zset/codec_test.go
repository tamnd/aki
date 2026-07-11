package zset

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// The codec proofs labs/f3/m2/03 demands before the native band ships: the
// sortable transform must agree with the zset total order everywhere, collapse
// the signed zeros onto one key, keep the raw bits round-tripping through the
// structure, and stay exact on geo's 52-bit integers.

// specialScores is the edge set every ordering and round-trip test sweeps.
var specialScores = []float64{
	math.Inf(-1),
	-math.MaxFloat64,
	-1e300, -12345.678, -1, -math.SmallestNonzeroFloat64,
	math.Copysign(0, -1), 0,
	math.SmallestNonzeroFloat64, 1, 12345.678, 1e300,
	math.MaxFloat64,
	math.Inf(1),
}

// TestScoreKeyTotalOrder checks the u64 key order equals the float order over
// every special pair and a large random sweep: strictly less floats map to
// strictly less keys, and equal floats (which includes the -0.0/+0.0 pair) map
// to the one key.
func TestScoreKeyTotalOrder(t *testing.T) {
	check := func(a, b float64) {
		t.Helper()
		ka, kb := scoreKey(a), scoreKey(b)
		switch {
		case a < b:
			if ka >= kb {
				t.Fatalf("scoreKey(%v)=%#x not below scoreKey(%v)=%#x", a, ka, b, kb)
			}
		case a > b:
			if ka <= kb {
				t.Fatalf("scoreKey(%v)=%#x not above scoreKey(%v)=%#x", a, ka, b, kb)
			}
		default: // equal, including -0.0 == +0.0
			if ka != kb {
				t.Fatalf("equal scores %v and %v map to keys %#x and %#x", a, b, ka, kb)
			}
		}
	}
	for _, a := range specialScores {
		for _, b := range specialScores {
			check(a, b)
		}
	}
	rng := rand.New(rand.NewPCG(3, 3))
	rf := func() float64 {
		// Random bit patterns cover exponent extremes uniform floats never hit.
		for {
			f := math.Float64frombits(rng.Uint64())
			if !math.IsNaN(f) {
				return f
			}
		}
	}
	for i := 0; i < 100000; i++ {
		check(rf(), rf())
	}
}

// TestScoreKeySignedZero pins the lab's trap directly: the doc-12 literal
// transform puts -0.0 one key below +0.0; the normalization must collapse them.
func TestScoreKeySignedZero(t *testing.T) {
	neg := math.Copysign(0, -1)
	if scoreKey(neg) != scoreKey(0) {
		t.Fatalf("scoreKey(-0.0)=%#x, scoreKey(+0.0)=%#x, must be one key", scoreKey(neg), scoreKey(0))
	}
	if scoreKey(0) != keySignBit {
		t.Fatalf("scoreKey(0)=%#x, want %#x", scoreKey(0), keySignBit)
	}
}

// TestScoreKeyRoundTrip proves scoreFromKey inverts the transform bit for bit
// everywhere except -0.0, which decodes to +0.0 by design.
func TestScoreKeyRoundTrip(t *testing.T) {
	for _, f := range specialScores {
		got := scoreFromKey(scoreKey(f))
		want := f
		if f == 0 {
			want = 0 // -0.0 normalizes away; +0.0 comes back
		}
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("round-trip of %v: got bits %#x, want bits %#x", f, math.Float64bits(got), math.Float64bits(want))
		}
	}
	rng := rand.New(rand.NewPCG(4, 4))
	for i := 0; i < 100000; i++ {
		f := math.Float64frombits(rng.Uint64())
		if math.IsNaN(f) || f == 0 {
			continue
		}
		if got := scoreFromKey(scoreKey(f)); math.Float64bits(got) != math.Float64bits(f) {
			t.Fatalf("round-trip of bits %#x came back %#x", math.Float64bits(f), math.Float64bits(got))
		}
	}
}

// TestScoreKeyInfinityPlacement pins -inf as the smallest key and +inf as the
// largest, so ZRANGEBYSCORE -inf +inf spans without special cases.
func TestScoreKeyInfinityPlacement(t *testing.T) {
	lo, hi := scoreKey(math.Inf(-1)), scoreKey(math.Inf(1))
	// -inf inverts to 0x000FFFFFFFFFFFFF and +inf sign-flips to
	// 0xFFF0000000000000; the keys outside that span are NaN bit patterns the
	// encoder never emits, so these are the extreme keys the tree can hold.
	if lo != 0x000FFFFFFFFFFFFF {
		t.Fatalf("scoreKey(-inf)=%#x, want 0x000FFFFFFFFFFFFF", lo)
	}
	if hi != 0xFFF0000000000000 {
		t.Fatalf("scoreKey(+inf)=%#x, want 0xFFF0000000000000", hi)
	}
	for _, f := range specialScores[1 : len(specialScores)-1] {
		k := scoreKey(f)
		if k <= lo || k >= hi {
			t.Fatalf("finite %v key %#x outside (-inf, +inf) keys (%#x, %#x)", f, k, lo, hi)
		}
	}
}

// TestSignedZeroCollapseInTree puts one member at -0.0 and one at +0.0 through
// the native band and checks they tie on score and order by member bytes, the
// behavior the normalization buys, while ZSCORE still tells them apart.
func TestSignedZeroCollapseInTree(t *testing.T) {
	z := nativeSeeded(t)
	neg := math.Copysign(0, -1)
	// b gets -0.0 and a gets +0.0: member order must win, not zero sign.
	z.update([]byte("zzb"), neg, flags{})
	z.update([]byte("zza"), 0, flags{})

	ra, _, _ := z.rank([]byte("zza"))
	rb, _, _ := z.rank([]byte("zzb"))
	if ra >= rb {
		t.Fatalf("rank(zza)=%d, rank(zzb)=%d: -0.0/+0.0 must tie and fall to member order", ra, rb)
	}

	sa, _ := z.score([]byte("zza"))
	sb, _ := z.score([]byte("zzb"))
	if got := string(resp.FormatScore(nil, sa)); got != "0" {
		t.Fatalf("ZSCORE of +0.0 formats %q, want \"0\"", got)
	}
	if got := string(resp.FormatScore(nil, sb)); got != "-0" {
		t.Fatalf("ZSCORE of -0.0 formats %q, want \"-0\"", got)
	}
	if math.Float64bits(sb) != math.Float64bits(neg) {
		t.Fatalf("stored -0.0 came back with bits %#x", math.Float64bits(sb))
	}
}

// TestSignedZeroInlineCollapses pins the band split Redis has: the listpack
// band integer-encodes a zero score and loses the sign, so an inline member
// added at -0.0 answers ZSCORE "0"; only the native band keeps the raw bits.
func TestSignedZeroInlineCollapses(t *testing.T) {
	z := newZset()
	neg := math.Copysign(0, -1)
	z.update([]byte("a"), neg, flags{})
	if z.enc != encListpack {
		t.Fatalf("enc = %s, want listpack", z.enc)
	}
	s, ok := z.score([]byte("a"))
	if !ok || math.Signbit(s) {
		t.Fatalf("inline -0.0 came back %v (signbit %v), want +0.0", s, math.Signbit(s))
	}
	if got := string(resp.FormatScore(nil, s)); got != "0" {
		t.Fatalf("inline ZSCORE of -0.0 formats %q, want \"0\"", got)
	}
	// The rescore path collapses too.
	z.update([]byte("a"), 1, flags{})
	z.update([]byte("a"), neg, flags{})
	if s, _ := z.score([]byte("a")); math.Signbit(s) {
		t.Fatal("inline rescore to -0.0 kept the sign")
	}
}

// TestRawBitsRoundTripNative pushes every special score and a random sweep
// through the native band and reads them back bit-exact, the doc-12 rule that
// ZSCORE formats from the raw bits and never decodes a tree key.
func TestRawBitsRoundTripNative(t *testing.T) {
	z := nativeSeeded(t)
	put := func(m string, f float64) {
		t.Helper()
		z.update([]byte(m), f, flags{})
		got, ok := z.score([]byte(m))
		if !ok || math.Float64bits(got) != math.Float64bits(f) {
			t.Fatalf("score(%q) bits %#x, want %#x", m, math.Float64bits(got), math.Float64bits(f))
		}
	}
	for i, f := range specialScores {
		put("sp"+itoa(i), f)
	}
	rng := rand.New(rand.NewPCG(5, 5))
	for i := 0; i < 2000; i++ {
		f := math.Float64frombits(rng.Uint64())
		if math.IsNaN(f) {
			continue
		}
		put("r"+itoa(i), f)
	}
	// Rescore an existing member to -0.0: the tree moves it, the bits keep the sign.
	z.update([]byte("sp0"), math.Copysign(0, -1), flags{})
	got, _ := z.score([]byte("sp0"))
	if math.Float64bits(got) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatalf("rescore to -0.0 lost the sign, bits %#x", math.Float64bits(got))
	}
}

// TestNativeOrderMatchesReference builds a native zset over the specials plus
// randoms and checks the emitted order against the reference total order.
func TestNativeOrderMatchesReference(t *testing.T) {
	z := nativeSeeded(t)
	rng := rand.New(rand.NewPCG(6, 6))
	for i, f := range specialScores {
		z.update([]byte("sp"+itoa(i)), f, flags{})
	}
	for i := 0; i < 1000; i++ {
		f := math.Float64frombits(rng.Uint64())
		if math.IsNaN(f) {
			continue
		}
		z.update([]byte("r"+itoa(i)), f, flags{})
	}
	ev := z.entries()
	for i := 1; i < len(ev); i++ {
		a, b := ev[i-1], ev[i]
		if b.score < a.score || (b.score == a.score && bytes.Compare(b.member, a.member) <= 0) {
			t.Fatalf("order broken at %d: (%v,%q) then (%v,%q)", i, a.score, a.member, b.score, b.member)
		}
	}
}

// TestNaNRejectedBeforeStructures: a literal nan dies in parseScore and a
// NaN-producing increment dies in update, both before any structure is touched.
func TestNaNRejectedBeforeStructures(t *testing.T) {
	for _, s := range []string{"nan", "NaN", "NAN", "-nan"} {
		if _, ok := parseScore([]byte(s)); ok {
			t.Fatalf("parseScore(%q) accepted NaN", s)
		}
	}
	z := nativeSeeded(t)
	z.update([]byte("inf"), math.Inf(1), flags{})
	before := z.card()
	_, _, _, applied, nan := z.update([]byte("inf"), math.Inf(-1), flags{incr: true})
	if !nan || applied {
		t.Fatal("inf + -inf increment must reject as NaN, not apply")
	}
	if z.card() != before {
		t.Fatal("NaN rejection changed the structure")
	}
	if got, _ := z.score([]byte("inf")); !math.IsInf(got, 1) {
		t.Fatalf("member score changed by a rejected increment: %v", got)
	}
}

// TestGeoIntegerExactness: GEOADD stores 52-bit interleaved cells as scores;
// every integer up to 2^52 is representable and the codec must keep both the
// value and the order exact.
func TestGeoIntegerExactness(t *testing.T) {
	max := uint64(1) << 52
	cells := []uint64{0, 1, 2, 3, max - 2, max - 1, max, max / 2, max/2 + 1}
	rng := rand.New(rand.NewPCG(7, 7))
	for i := 0; i < 5000; i++ {
		cells = append(cells, rng.Uint64N(max+1))
	}
	for _, c := range cells {
		f := float64(c)
		if uint64(f) != c {
			t.Fatalf("52-bit cell %d not exact as float64", c)
		}
		if got := scoreFromKey(scoreKey(f)); got != f {
			t.Fatalf("cell %d round-trips to %v", c, got)
		}
	}
	for i := 1; i < len(cells); i++ {
		a, b := cells[i-1], cells[i]
		ka, kb := scoreKey(float64(a)), scoreKey(float64(b))
		if (a < b) != (ka < kb) || (a == b) != (ka == kb) {
			t.Fatalf("cell order disagrees: %d vs %d, keys %#x vs %#x", a, b, ka, kb)
		}
	}
	// Through the structure: adjacent cells stay distinct and ordered.
	z := nativeSeeded(t)
	z.update([]byte("gb"), float64(max-1), flags{})
	z.update([]byte("ga"), float64(max), flags{})
	ra, _, _ := z.rank([]byte("ga"))
	rb, _, _ := z.rank([]byte("gb"))
	if rb >= ra {
		t.Fatalf("adjacent geo cells misordered: rank(max-1)=%d, rank(max)=%d", rb, ra)
	}
}

// nativeSeeded returns a zset already promoted to the native band with a
// throwaway seed population, so the codec tests exercise the tree and member
// hash rather than the inline blob.
func nativeSeeded(t *testing.T) *zset {
	t.Helper()
	z := newZset()
	for i := 0; i <= maxListpackEntries; i++ {
		z.update([]byte("seed"+itoa(i)), 1e9+float64(i), flags{})
	}
	if z.enc != encSkiplist {
		t.Fatalf("seed population did not promote, enc = %s", z.enc)
	}
	return z
}
