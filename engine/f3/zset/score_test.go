package zset

import (
	"math"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// Scores round-trip through the inline blob the way they do through Redis's
// listpack: the raw bits survive for everything except -0.0, which the
// listpack collapses to an integer zero (Redis loses the sign there too, so
// ZSCORE answers "0" on both engines), and the infinities format as inf and
// -inf (spec 2064/f3/12 sections 3.1, 4).
func TestScoreRoundTrip(t *testing.T) {
	cases := []struct {
		in   float64
		want string
		bits float64 // the stored value, in == bits except for -0.0
	}{
		{0.0, "0", 0.0},
		{math.Copysign(0, -1), "0", 0.0},
		{math.Inf(1), "inf", math.Inf(1)},
		{math.Inf(-1), "-inf", math.Inf(-1)},
		{3.5, "3.5", 3.5},
		{-2, "-2", -2},
		{17, "17", 17},
	}
	for _, tc := range cases {
		z := newZset()
		z.update([]byte("m"), tc.in, flags{})
		got, ok := z.score([]byte("m"))
		if !ok {
			t.Fatalf("%v: absent after add", tc.in)
		}
		gotStr := string(resp.FormatScore(nil, got))
		if gotStr != tc.want {
			t.Errorf("score %v round-tripped to %q, want %q", tc.in, gotStr, tc.want)
		}
		if math.Float64bits(got) != math.Float64bits(tc.bits) {
			t.Errorf("score %v stored bits of %v, want %v", tc.in, got, tc.bits)
		}
	}
}

// The same must hold across the conversion: an inline -0.0 was already
// collapsed to +0.0 at the listpack write, so the promoted native zset holds
// +0.0 too, exactly what a promoted Redis listpack hands its skiplist.
func TestScoreSurvivesConversion(t *testing.T) {
	z := newZset()
	z.update([]byte("negzero"), math.Copysign(0, -1), flags{})
	for i := 0; i < maxListpackEntries+5; i++ {
		z.update([]byte("pad"+string(rune('A'+i%26))+string(rune('a'+i/26))), float64(i+1), flags{})
	}
	if z.enc != encSkiplist {
		t.Fatalf("enc = %s, want skiplist", z.enc)
	}
	got, ok := z.score([]byte("negzero"))
	if !ok {
		t.Fatal("negzero lost after conversion")
	}
	if math.Signbit(got) || got != 0 {
		t.Fatalf("negzero score = %v (signbit %v), want the collapsed +0.0", got, math.Signbit(got))
	}
}
