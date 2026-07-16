package sqlo1

import (
	"context"
	"errors"
	"math"
)

// LCS is a transcription of Redis's lcsCommand (t_string.c): the full
// (alen+1)*(blen+1) uint32 table, the same mismatch tie-break (move up
// only when strictly larger, else left), and the same range emission
// walking backward from the string tails. The spec keeps this
// deliberately no better than Redis: O(n*m) compute, O(n+m) IO, with
// rope keys streaming their chunks through the ordinary read path.

// lcsMaxAlloc caps the transient DP table exactly where Redis does:
// the proto-max-bulk-len default.
const lcsMaxAlloc = 512 << 20

var (
	errLcsTooLong = errors.New("sqlo1: string too long for LCS")
	errLcsAlloc   = errors.New("sqlo1: LCS transient memory over cap")
)

// lcsMatch is one emitted range pair: a[aStart..aEnd] pairs with
// b[bStart..bEnd], both inclusive, length aEnd-aStart+1.
type lcsMatch struct {
	aStart, aEnd, bStart, bEnd uint32
}

// LcsRead pulls both values whole for the DP. The first is copied into
// the LCS scratch because Get's bytes die on the next store call; the
// second aliases the read scratch, which stays valid because the
// compute that follows makes no store calls. Missing keys read as
// empty strings, per Redis.
func (s *Str) LcsRead(ctx context.Context, key1, key2 []byte) ([]byte, []byte, error) {
	v1, ok, err := s.Get(ctx, key1)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		v1 = nil
	}
	s.lcsBuf = append(s.lcsBuf[:0], v1...)
	v2, ok, err := s.Get(ctx, key2)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		v2 = nil
	}
	return s.lcsBuf, v2, nil
}

// lcsRun fills the DP table and, unless only the length is wanted,
// backtracks for the LCS string and the match ranges. Matches come out
// in Redis's emission order, tail of the strings first, already
// filtered by minMatchLen. The result string is built only when the
// caller shows it (neither LEN nor IDX); the ranges only under IDX.
func lcsRun(a, b []byte, getLen, getIdx bool, minMatchLen int64) (total uint32, result []byte, matches []lcsMatch, err error) {
	if len(a) >= math.MaxUint32-1 || len(b) >= math.MaxUint32-1 {
		return 0, nil, nil, errLcsTooLong
	}
	alen, blen := len(a), len(b)
	w := blen + 1
	if (alen+1)*w*4 > lcsMaxAlloc {
		return 0, nil, nil, errLcsAlloc
	}
	lcs := make([]uint32, (alen+1)*w)
	for i := 1; i <= alen; i++ {
		ai := a[i-1]
		prev := lcs[(i-1)*w : i*w]
		row := lcs[i*w : i*w+w]
		for j := 1; j <= blen; j++ {
			switch {
			case ai == b[j-1]:
				row[j] = prev[j-1] + 1
			case prev[j] > row[j-1]:
				row[j] = prev[j]
			default:
				row[j] = row[j-1]
			}
		}
	}
	total = lcs[alen*w+blen]

	if getLen && !getIdx {
		return total, nil, nil, nil
	}
	if !getIdx {
		result = make([]byte, total)
	}

	// The backtrack mirrors the C loop shape exactly, including the
	// alen sentinel for "no range open" and the forced emit when a
	// range start reaches either string's first byte.
	idx := total
	arangeStart := uint32(alen)
	var arangeEnd, brangeStart, brangeEnd uint32
	i, j := alen, blen
	for i > 0 && j > 0 {
		emitRange := false
		if a[i-1] == b[j-1] {
			if result != nil {
				result[idx-1] = a[i-1]
			}
			if arangeStart == uint32(alen) {
				arangeStart = uint32(i - 1)
				arangeEnd = uint32(i - 1)
				brangeStart = uint32(j - 1)
				brangeEnd = uint32(j - 1)
			} else if arangeStart == uint32(i) && brangeStart == uint32(j) {
				arangeStart--
				brangeStart--
			} else {
				emitRange = true
			}
			if arangeStart == 0 || brangeStart == 0 {
				emitRange = true
			}
			idx--
			i--
			j--
		} else {
			if lcs[(i-1)*w+j] > lcs[i*w+j-1] {
				i--
			} else {
				j--
			}
			if arangeStart != uint32(alen) {
				emitRange = true
			}
		}
		if emitRange {
			matchLen := arangeEnd - arangeStart + 1
			if getIdx && (minMatchLen == 0 || int64(matchLen) >= minMatchLen) {
				matches = append(matches, lcsMatch{arangeStart, arangeEnd, brangeStart, brangeEnd})
			}
			arangeStart = uint32(alen)
		}
	}
	return total, result, matches, nil
}
