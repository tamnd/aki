package store

import (
	"math"
	"testing"
)

// ParseRedisFloat is the string2ld reconciliation both INCRBYFLOAT and
// HINCRBYFLOAT read through, so its accept/reject boundary is pinned here at its
// shared home rather than only through the two commands. Each case names the
// branch it exercises.
func TestParseRedisFloat(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"", 0, false},         // empty rejects
		{"0", 0, true},         // zero
		{"1.5", 1.5, true},     // plain decimal
		{"-2.25", -2.25, true}, // signed
		{"3.0e3", 3000, true},  // exponent
		{"abc", 0, false},      // not a number
		{"1_000", 0, false},    // underscore separator rejects (Go accepts it, strtold does not)
		{"nan", 0, false},      // NaN rejects
		{"+nan", 0, false},     // signed NaN rejects
		{"0x1p4", 16, true},    // hex float with binary exponent parses straight
		{"0x1.8p1", 3, true},   // hex float, fractional mantissa
		{"0x10", 16, true},     // hex without an exponent retries with an explicit p0
		{"1e-400", 0, false},   // underflow to zero on a nonzero significand rejects
	}
	for _, c := range cases {
		got, ok := ParseRedisFloat([]byte(c.in))
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("ParseRedisFloat(%q) = (%v, %v), want (%v, %v)", c.in, got, ok, c.want, c.ok)
		}
	}

	// Infinity parses cleanly and is left for the caller to reject at the result,
	// the split the two INCRBYFLOAT commands rely on.
	for _, in := range []string{"inf", "+inf", "-inf", "Infinity"} {
		v, ok := ParseRedisFloat([]byte(in))
		if !ok || !math.IsInf(v, 0) {
			t.Errorf("ParseRedisFloat(%q) = (%v, %v), want an infinity", in, v, ok)
		}
	}
}
