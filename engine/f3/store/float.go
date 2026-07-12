package store

import (
	"bytes"
	"math"
	"strconv"
)

// ParseRedisFloat parses b the way Redis's string2ld does (via strtold),
// returning the value and whether it is valid, the string2ld sibling of
// ParseInt's string2ll discipline. strconv.ParseFloat disagrees with strtold
// on a few inputs, and each disagreement is reconciled here so the
// accept/reject boundary matches byte for byte: hex with no binary exponent
// retries with an explicit p0, an underscore separator rejects, NaN rejects,
// and a literal that underflowed to zero rejects. Infinity parses cleanly and
// only fails once it lands in a result the caller rejects. Both INCRBYFLOAT and
// HINCRBYFLOAT read a stored value and an increment through this one function so
// the two bands never drift on which literals they accept.
func ParseRedisFloat(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	if bytes.IndexByte(b, '_') >= 0 {
		return 0, false
	}
	s := string(b)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrSyntax && isHexFloatLiteral(b) {
			v, err = strconv.ParseFloat(s+"p0", 64)
		}
	}
	if err != nil {
		return 0, false
	}
	if math.IsNaN(v) {
		return 0, false
	}
	if v == 0 && underflowedToZero(b) {
		return 0, false
	}
	return v, true
}

// isHexFloatLiteral reports whether b, after an optional sign, begins with the
// 0x/0X hex-float prefix. It gates the p0 retry so only genuine hex input is
// rewritten.
func isHexFloatLiteral(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '+' || b[i] == '-') {
		i++
	}
	return i+1 < len(b) && b[i] == '0' && (b[i+1] == 'x' || b[i+1] == 'X')
}

// underflowedToZero reports whether b has a nonzero significand yet parsed to
// exactly 0.0, which means it underflowed: strtold flags that ERANGE and Redis
// rejects it, while Go returns a clean zero. Only consulted when the parse
// already yielded zero.
func underflowedToZero(b []byte) bool {
	i := 0
	if i < len(b) && (b[i] == '+' || b[i] == '-') {
		i++
	}
	hex := i+1 < len(b) && b[i] == '0' && (b[i+1] == 'x' || b[i+1] == 'X')
	if hex {
		i += 2
	}
	for ; i < len(b); i++ {
		d := b[i]
		if hex {
			if d == 'p' || d == 'P' {
				break
			}
			if (d >= '1' && d <= '9') || (d >= 'a' && d <= 'f') || (d >= 'A' && d <= 'F') {
				return true
			}
		} else {
			if d == 'e' || d == 'E' {
				break
			}
			if d >= '1' && d <= '9' {
				return true
			}
		}
	}
	return false
}
