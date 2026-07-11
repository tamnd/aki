package zset

import (
	"math"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// defaultScanCount is the COUNT hint Redis applies when a client omits it: a page
// examines this many records unless COUNT says otherwise (spec 2064/f3/12 section
// 6.11, COUNT bounds records examined, not members returned).
const defaultScanCount = 10

// Zscan answers ZSCAN key cursor [MATCH pattern] [COUNT count].
//
// The cursor rides the member records downward (spec 2064/f3/12 section 6.11), the
// same opaque downward cursor SSCAN established (set/scan.go): each page examines
// COUNT records from the top of the unscanned region and returns the lower
// boundary as the next cursor, 0 when the scan completes. It is not Redis's
// bit-reversed cursor; it is this engine's own record-array cursor, so a
// differential test compares the set of returned pairs over a full scan, not the
// per-page cursors. A member present for the whole scan is returned at least once
// (skiplist.go scanPage carries the proof); the inline band answers in one page.
// The reply is flat member then score in both RESP2 and RESP3. COUNT is a hint,
// MATCH filters by glob. There is no NOSCORES: HSCAN grew NOVALUES in 7.4 but
// ZSCAN never got the analogue, so a NOSCORES token is a syntax error, same as
// Redis 8.8 (the live differential pins the exact text).
func Zscan(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		r.Err("ERR invalid cursor")
		return
	}
	var match []byte
	count := defaultScanCount
	for i := 2; i < len(args); i++ {
		switch {
		case eqFold(args[i], "MATCH"):
			if i+1 >= len(args) {
				r.Err("ERR syntax error")
				return
			}
			match = args[i+1]
			i++
		case eqFold(args[i], "COUNT"):
			if i+1 >= len(args) {
				r.Err("ERR syntax error")
				return
			}
			c, ok := parseScanCount(args[i+1])
			if !ok {
				r.Err("ERR syntax error")
				return
			}
			count = c
			i++
		default:
			r.Err("ERR syntax error")
			return
		}
	}

	g := registry(cx)
	z, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}

	// The reply is a two-element array: the next cursor and the pair page. The page
	// is built first so its header carries the exact post-filter element count.
	page := cx.Val[:0]
	n := 0
	var next uint64
	var sc [40]byte
	if z != nil {
		next = z.scanPage(cursor, count, match, func(m []byte, bits uint64) {
			page = resp.AppendBulk(page, m)
			page = resp.AppendBulk(page, resp.FormatScore(sc[:0], math.Float64frombits(bits)))
			n += 2
		})
	}
	cx.Val = page

	var cbuf [20]byte
	out := resp.AppendArrayHeader(cx.Aux[:0], 2)
	out = resp.AppendBulk(out, strconv.AppendUint(cbuf[:0], next, 10))
	out = resp.AppendArrayHeader(out, n)
	out = append(out, page...)
	cx.Aux = out
	r.Raw(out)
}

// parseScanCount reads a COUNT argument: a positive integer. Redis rejects zero
// and negatives with a syntax error, so anything not strictly positive is invalid.
func parseScanCount(b []byte) (int, bool) {
	v, err := strconv.Atoi(string(b))
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}

// globMatch reports whether str matches the glob pattern, the same operators
// Redis's stringmatchlen implements: * any run, ? one byte, [...] a class with
// ranges and a leading ^ negation, and \ escaping the next byte. The match is
// byte-oriented and case sensitive, matching SCAN's MATCH. It mirrors the set
// band's copy (set/scan.go); the two surfaces keep their own so neither depends on
// the other's package.
func globMatch(pattern, str []byte) bool {
	p, sIdx := 0, 0
	for p < len(pattern) {
		switch pattern[p] {
		case '*':
			for p+1 < len(pattern) && pattern[p+1] == '*' {
				p++
			}
			if p+1 == len(pattern) {
				return true
			}
			for i := sIdx; i <= len(str); i++ {
				if globMatch(pattern[p+1:], str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if sIdx == len(str) {
				return false
			}
			sIdx++
			p++
		case '[':
			if sIdx == len(str) {
				return false
			}
			p++
			neg := false
			if p < len(pattern) && pattern[p] == '^' {
				neg = true
				p++
			}
			match := false
			for p < len(pattern) && pattern[p] != ']' {
				if pattern[p] == '\\' && p+1 < len(pattern) {
					p++
					if pattern[p] == str[sIdx] {
						match = true
					}
				} else if p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']' {
					lo, hi := pattern[p], pattern[p+2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if str[sIdx] >= lo && str[sIdx] <= hi {
						match = true
					}
					p += 2
				} else if pattern[p] == str[sIdx] {
					match = true
				}
				p++
			}
			if p < len(pattern) {
				p++ // consume ']'
			}
			if match == neg {
				return false
			}
			sIdx++
		case '\\':
			if p+1 < len(pattern) {
				p++
			}
			if sIdx == len(str) || pattern[p] != str[sIdx] {
				return false
			}
			sIdx++
			p++
		default:
			if sIdx == len(str) || pattern[p] != str[sIdx] {
				return false
			}
			sIdx++
			p++
		}
	}
	return sIdx == len(str)
}
