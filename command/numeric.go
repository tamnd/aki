package command

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// parseInteger parses a byte string as a signed 64-bit integer using the strict
// rules Redis applies to INCR, DECR, expire arguments and offsets (doc 08 §2.1).
// It rejects an empty string, any non-digit byte, a redundant leading zero such
// as "007", the redundant "-0", and any value outside the int64 range. The bool
// is false on any of those.
func parseInteger(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := b[0] == '-'
	digits := b
	if neg {
		digits = b[1:]
		if len(digits) == 0 {
			return 0, false
		}
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	// "0" is the only value that may begin with a zero. "007" and "-0" are out.
	if len(digits) > 1 && digits[0] == '0' {
		return 0, false
	}
	if neg && len(digits) == 1 && digits[0] == '0' {
		return 0, false
	}

	var u uint64
	for _, c := range digits {
		d := uint64(c - '0')
		if u > (math.MaxUint64-d)/10 {
			return 0, false
		}
		u = u*10 + d
	}
	if neg {
		if u > uint64(math.MaxInt64)+1 {
			return 0, false
		}
		if u == uint64(math.MaxInt64)+1 {
			return math.MinInt64, true
		}
		return -int64(u), true
	}
	if u > uint64(math.MaxInt64) {
		return 0, false
	}
	return int64(u), true
}

// parseFloat parses a byte string as a float64 using Go's parser with the extra
// constraint that NaN and ±Inf are rejected, matching Redis (doc 08 §2.2).
// strconv.ParseFloat already rejects surrounding whitespace, so leading and
// trailing spaces fail here too.
func parseFloat(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// addFloatHuman adds an increment to a current value and renders the result the
// way INCRBYFLOAT and HINCRBYFLOAT report it. Both byte strings must already be
// validated as finite floats (parseFloat does this for the error reply); an empty
// or nil curBytes is treated as zero for the absent-key case.
//
// Redis computes these two commands in C long double and formats with "%.17Lf"
// then trims trailing zeros. long double on the platforms Redis targets carries a
// 64-bit mantissa, which a math/big Float at precision 64 reproduces, so a value
// whose precision exceeds float64 (3.0 + 1.000000000000000005) prints the same
// "4.00000000000000001" rather than collapsing to "4". The arithmetic runs at the
// same width Redis uses, not float64, so the stored string matches byte for byte.
func addFloatHuman(curBytes, incrBytes []byte) string {
	a := new(big.Float).SetPrec(64)
	if len(curBytes) > 0 {
		if v, _, err := big.ParseFloat(string(curBytes), 10, 64, big.ToNearestEven); err == nil {
			a = v
		}
	}
	if b, _, err := big.ParseFloat(string(incrBytes), 10, 64, big.ToNearestEven); err == nil {
		a.Add(a, b)
	}
	// "%.17Lf": fixed notation with 17 fractional digits, then trailing zeros and a
	// bare decimal point are dropped so 10.0 prints "10" and 3.50 prints "3.5".
	s := a.Text('f', 17)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	if s == "-0" {
		s = "0"
	}
	return s
}

// addInt64 adds two signed 64-bit integers and reports whether the result
// overflowed. INCR and friends use it to detect the overflow Redis rejects.
func addInt64(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
}
