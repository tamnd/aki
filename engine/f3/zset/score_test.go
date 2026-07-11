package zset

import (
	"math"
	"testing"

	"github.com/tamnd/aki/f3srv/resp"
)

// Scores round-trip exactly through the inline blob: the special doubles
// (signed zero and the infinities) must format the way Redis does after a store
// and reload, because ZSCORE echoes them (spec 2064/f3/12 sections 3.1, 4). The
// blob keeps raw IEEE-754 bits precisely so signed zero survives.
func TestScoreRoundTrip(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0.0, "0"},
		{math.Copysign(0, -1), "-0"},
		{math.Inf(1), "inf"},
		{math.Inf(-1), "-inf"},
		{3.5, "3.5"},
		{-2, "-2"},
		{17, "17"},
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
		// The stored bits must be identical, including the sign of zero.
		if math.Float64bits(got) != math.Float64bits(tc.in) {
			t.Errorf("score %v changed bits to %v", tc.in, got)
		}
	}
}

// The same must hold across the conversion: a signed-zero score survives band
// promotion.
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
	if !math.Signbit(got) || got != 0 {
		t.Fatalf("negzero score = %v (signbit %v), want -0", got, math.Signbit(got))
	}
}
