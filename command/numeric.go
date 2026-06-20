package command

import "math"

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
