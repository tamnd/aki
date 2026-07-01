package f1srv

import (
	"math"
	"testing"
)

// formatScore must match Redis d2string byte-for-byte. The expected column here was captured
// from a live Redis 8.8 via ZADD/ZSCORE over the same double (Go parses these literals to the
// same IEEE-754 value strtod does), so a change that drifts from Redis fails here. The battery
// walks the integer path (double2ll), the fixed/scientific grisu2 boundary, tiny magnitudes,
// and the extremes.
func TestFormatScoreParity(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{3, "3"},
		{3.14, "3.14"},
		{3.14159, "3.14159"},
		{0.1, "0.1"},
		{0.2, "0.2"},
		{0.3, "0.3"},
		{1.5, "1.5"},
		{-2.5, "-2.5"},
		{100, "100"},
		{12345.6789, "12345.6789"},
		{0.0001, "0.0001"},
		{0.00001, "0.00001"},
		{1e10, "10000000000"},
		{1e15, "1000000000000000"},
		{1e16, "10000000000000000"},
		{1e17, "100000000000000000"},
		{2e18, "2000000000000000000"},
		{4e18, "4000000000000000000"},
		{9e18, "9e+18"},
		{1e19, "1e+19"},
		{1e20, "1e+20"},
		{1e21, "1e+21"},
		{1e-7, "1e-7"},
		{1e-8, "1e-8"},
		{1e-10, "1e-10"},
		{42, "42"},
		{255.5, "255.5"},
		{0.5, "0.5"},
		{3.141592653589793, "3.141592653589793"},
		{123456789.123456789, "1.2345678912345679e+8"},
		{1234567890123456, "1234567890123456"},
		{12345678901234567, "12345678901234568"},
		{9223372036854775807, "9223372036854776000"},
		{1.7976931348623157e308, "1.7976931348623157e+308"},
		{5e-324, "5e-324"},
		{math.Inf(1), "inf"},
		{math.Inf(-1), "-inf"},
		{math.NaN(), "nan"},
	}
	for _, tc := range cases {
		got := string(formatScore(nil, tc.in))
		if got != tc.want {
			t.Errorf("formatScore(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// formatScore spells signed zero the way d2string does: +0.0 is "0", -0.0 is "-0". zset score
// ingest normalizes -0.0 to +0.0 before it ever reaches formatScore (matching Redis's default
// listpack encoding, which collapses -0 to integer 0), so scores never surface as "-0"; this
// test pins the raw formatter's behavior, which INCRBYFLOAT and other float replies may reuse.
func TestFormatScoreSignedZero(t *testing.T) {
	if got := string(formatScore(nil, math.Copysign(0, 1))); got != "0" {
		t.Errorf("formatScore(+0) = %q, want \"0\"", got)
	}
	if got := string(formatScore(nil, math.Copysign(0, -1))); got != "-0" {
		t.Errorf("formatScore(-0) = %q, want \"-0\"", got)
	}
}

// formatScore appends to dst rather than overwriting it, so it composes with a caller's buffer.
func TestFormatScoreAppends(t *testing.T) {
	dst := []byte("prefix:")
	dst = formatScore(dst, 3.5)
	if string(dst) != "prefix:3.5" {
		t.Errorf("append form = %q, want \"prefix:3.5\"", string(dst))
	}
}
