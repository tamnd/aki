package str

// LCS computes the longest common subsequence of two string keys (spec
// 2064/f3/09). It runs the classic dynamic-programming table and backtrack: the
// plain form replies the subsequence itself, LEN replies its length, and IDX
// replies the matching ranges in each key, walked from the end of both strings
// toward the start the way Redis reports them. MINMATCHLEN filters short ranges
// out of the IDX reply and WITHMATCHLEN tags each range with its length.
//
// The two keys are read, never written, so the co-located case reads both off
// their shared owner and a split pair takes the F17 intent route, reading each
// value on its own owner under the transaction that holds the two keys. Like GET
// the command serves only the string keyspace, so a value another type owns reads
// as absent (an empty operand), not WRONGTYPE.

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// lcsMaxAlloc caps the transient DP table at the proto-max-bulk-len ceiling, the
// same 512MiB bound Redis refuses an oversized LCS at, so a pair of large values
// errors instead of trying to allocate an astronomically large matrix.
const lcsMaxAlloc = 512 << 20

const errLcsMem = "ERR Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len"
const errLcsBoth = "ERR If you want both the length and indexes, please just use IDX."

// lcsOpts holds the parsed LCS modifiers.
type lcsOpts struct {
	getLen       bool
	getIdx       bool
	withMatchLen bool
	minMatchLen  int
}

// lcsRange is one matching span: an inclusive byte range in each key and its
// length. Ranges are collected during the backtrack, from the tail of the two
// strings toward their head.
type lcsRange struct {
	aStart, aEnd int
	bStart, bEnd int
	matchLen     int
}

// parseLcs parses LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN].
// LEN and IDX together are refused, matching Redis; a negative MINMATCHLEN is
// clamped to zero.
func parseLcs(args [][]byte) (lcsOpts, string) {
	var o lcsOpts
	for i := 2; i < len(args); i++ {
		switch {
		case eqFold(args[i], "LEN"):
			o.getLen = true
		case eqFold(args[i], "IDX"):
			o.getIdx = true
		case eqFold(args[i], "MINMATCHLEN"):
			if i+1 >= len(args) {
				return o, "ERR syntax error"
			}
			n, ok := store.ParseInt(args[i+1])
			if !ok {
				return o, "ERR value is not an integer or out of range"
			}
			if n < 0 {
				n = 0
			}
			o.minMatchLen = int(n)
			i++
		case eqFold(args[i], "WITHMATCHLEN"):
			o.withMatchLen = true
		default:
			return o, "ERR syntax error"
		}
	}
	if o.getLen && o.getIdx {
		return o, errLcsBoth
	}
	return o, ""
}

// lcsReply computes the LCS of a and b and appends the reply for the requested
// mode to dst, returning the appended buffer or a Redis error string. The DP
// table is built only when at least one operand is non-empty, and the result
// string only when the plain form asks for it, so LEN and IDX never materialize
// the subsequence.
func lcsReply(dst, a, b []byte, o lcsOpts) ([]byte, string) {
	alen, blen := len(a), len(b)
	if uint64(alen+1)*uint64(blen+1)*4 > lcsMaxAlloc {
		return dst, errLcsMem
	}

	// LCS(i,j) is the length of the common subsequence of a[:i] and b[:j], laid
	// out as one row-major array with a zero row and column for the empty prefix.
	stride := blen + 1
	lcs := make([]uint32, (alen+1)*stride)
	for i := 1; i <= alen; i++ {
		row := i * stride
		prev := row - stride
		for j := 1; j <= blen; j++ {
			if a[i-1] == b[j-1] {
				lcs[row+j] = lcs[prev+j-1] + 1
			} else if x, y := lcs[prev+j], lcs[row+j-1]; x >= y {
				lcs[row+j] = x
			} else {
				lcs[row+j] = y
			}
		}
	}
	lcsLen := int(lcs[alen*stride+blen])

	buildStr := !o.getLen && !o.getIdx
	var result []byte
	if buildStr {
		result = make([]byte, lcsLen)
	}
	var matches []lcsRange

	// Walk back from the bottom-right corner. A diagonal step is a matched byte;
	// otherwise move toward the larger neighbor. Consecutive diagonal steps grow
	// one range, and a break flushes it, so ranges come out tail-first.
	idx := lcsLen
	i, j := alen, blen
	// aStart == alen marks an unset range: a real start is always below alen.
	rng := lcsRange{aStart: alen}
	for i > 0 && j > 0 {
		if a[i-1] != b[j-1] {
			if lcs[(i-1)*stride+j] > lcs[i*stride+j-1] {
				i--
			} else {
				j--
			}
			continue
		}
		if buildStr {
			result[idx-1] = a[i-1]
		}
		switch {
		case rng.aStart == alen:
			rng = lcsRange{aStart: i - 1, aEnd: i - 1, bStart: j - 1, bEnd: j - 1}
		case rng.aStart == i && rng.bStart == j:
			// The run is contiguous: extend it one byte backward.
			rng.aStart--
			rng.bStart--
		default:
			matches = emitRange(matches, rng, o)
			rng = lcsRange{aStart: i - 1, aEnd: i - 1, bStart: j - 1, bEnd: j - 1}
		}
		i--
		j--
		idx--
	}
	if rng.aStart != alen {
		matches = emitRange(matches, rng, o)
	}

	switch {
	case o.getIdx:
		return appendIdx(dst, matches, lcsLen, o), ""
	case o.getLen:
		return resp.AppendInt(dst, int64(lcsLen)), ""
	default:
		return resp.AppendBulk(dst, result), ""
	}
}

// emitRange appends rng to matches when the IDX reply wants it and it clears the
// MINMATCHLEN filter, filling in its length.
func emitRange(matches []lcsRange, rng lcsRange, o lcsOpts) []lcsRange {
	if !o.getIdx {
		return matches
	}
	rng.matchLen = rng.aEnd - rng.aStart + 1
	if o.minMatchLen != 0 && rng.matchLen < o.minMatchLen {
		return matches
	}
	return append(matches, rng)
}

// appendIdx frames the IDX reply: a flat map of "matches" to the range array and
// "len" to the subsequence length. Each range is [[aStart,aEnd],[bStart,bEnd]],
// with the length appended when WITHMATCHLEN is set.
func appendIdx(dst []byte, matches []lcsRange, lcsLen int, o lcsOpts) []byte {
	dst = resp.AppendArrayHeader(dst, 4)
	dst = resp.AppendBulk(dst, []byte("matches"))
	dst = resp.AppendArrayHeader(dst, len(matches))
	for _, m := range matches {
		if o.withMatchLen {
			dst = resp.AppendArrayHeader(dst, 3)
		} else {
			dst = resp.AppendArrayHeader(dst, 2)
		}
		dst = resp.AppendArrayHeader(dst, 2)
		dst = resp.AppendInt(dst, int64(m.aStart))
		dst = resp.AppendInt(dst, int64(m.aEnd))
		dst = resp.AppendArrayHeader(dst, 2)
		dst = resp.AppendInt(dst, int64(m.bStart))
		dst = resp.AppendInt(dst, int64(m.bEnd))
		if o.withMatchLen {
			dst = resp.AppendInt(dst, int64(m.matchLen))
		}
	}
	dst = resp.AppendBulk(dst, []byte("len"))
	return resp.AppendInt(dst, int64(lcsLen))
}

// Lcs answers LCS on co-located keys: read both values off their shared owner,
// then compute the reply. A missing key reads as an empty operand.
func Lcs(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	o, errMsg := parseLcs(args)
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	a, _ := cx.St.GetString(args[0], cx.NowMs, nil)
	b, _ := cx.St.GetString(args[1], cx.NowMs, nil)
	out, oerr := lcsReply(nil, a, b, o)
	if oerr != "" {
		r.Err(oerr)
		return
	}
	r.Raw(out)
}

// LcsCross is the F17 route for two keys on different shards: read each value on
// its own owner under the intent that holds the pair, then compute the reply on
// the coordinator. Each GetString copies into a fresh buffer, so the operands
// outlive the hop that read them.
func LcsCross(t *shard.Txn, args [][]byte) []byte {
	o, errMsg := parseLcs(args)
	if errMsg != "" {
		return resp.AppendError(nil, errMsg)
	}
	var a, b []byte
	t.Do(args[0], func(cx *shard.Ctx) {
		a, _ = cx.St.GetString(args[0], cx.NowMs, nil)
	})
	t.Do(args[1], func(cx *shard.Ctx) {
		b, _ = cx.St.GetString(args[1], cx.NowMs, nil)
	})
	out, oerr := lcsReply(nil, a, b, o)
	if oerr != "" {
		return resp.AppendError(nil, oerr)
	}
	return out
}
