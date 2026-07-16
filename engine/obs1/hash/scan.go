package hash

import (
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// defaultScanCount is the COUNT hint Redis applies when a client omits it: a page
// examines this many draw positions unless COUNT says otherwise (doc 11 section
// 8.3, COUNT bounds positions examined, not pairs returned).
const defaultScanCount = 10

// Hscan answers HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES].
//
// The cursor rides the field table's draw vector downward (field.go scanPage),
// the same downward cursor SSCAN uses over its member vector, so doc 20's
// swap-remove correctness proof carries whole: a field present for the whole scan
// is returned at least once. The inline band has no vector, so it returns the
// whole hash in one page with cursor 0, which satisfies the guarantee vacuously
// and, being insertion-ordered like Redis's listpack, matches Redis byte for byte
// on that band. COUNT is a hint; MATCH filters the emitted fields by glob;
// NOVALUES (Redis 7.4) drops the values, replying a flat field list instead of
// field-value pairs.
func Hscan(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		r.Err("ERR invalid cursor")
		return
	}
	var match []byte
	count := defaultScanCount
	noValues := false
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
			c, ok := parseCount(args[i+1])
			if !ok {
				r.Err("ERR syntax error")
				return
			}
			count = c
			i++
		case eqFold(args[i], "NOVALUES"):
			noValues = true
		default:
			r.Err("ERR syntax error")
			return
		}
	}

	g := registry(cx)
	h, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}

	// The reply is a two-element array: the next cursor and the pair page. The
	// page is built first so its header carries the exact post-filter element
	// count (two per field with values, one without).
	page := cx.Val[:0]
	n := 0
	var next uint64
	if h != nil {
		next = h.scanPage(cursor, count, match, func(f, v []byte) {
			page = resp.AppendBulk(page, f)
			n++
			if !noValues {
				page = resp.AppendBulk(page, v)
				n++
			}
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

// scanPage returns one HSCAN page starting from cursor and reports the cursor to
// resume from, 0 when the scan is complete. The inline band is one page; the
// native band walks the downward cursor over its draw vector (field.go scanPage).
func (h *hash) scanPage(cursor uint64, count int, match []byte, emit func(field, value []byte)) uint64 {
	if h.enc == encListpack {
		// One page, cursor 0. Redis special-cases a small encoding: scanGenericCommand
		// returns the whole container and a zero cursor regardless of the cursor the
		// client passed, so a replayed or fabricated cursor still yields every pair.
		// Match that, byte for byte, rather than the tempting "nonzero cursor is a
		// finished replay, return nothing" which Redis does not do.
		h.eachInline(func(f, v []byte) {
			if match == nil || globMatch(match, f) {
				emit(f, v)
			}
		})
		return 0
	}
	return h.ft.scanPage(cursor, count, match, emit)
}

// parseCount reads a COUNT argument: a positive integer. Redis rejects zero and
// negatives with a syntax error, so anything not strictly positive is invalid.
func parseCount(b []byte) (int, bool) {
	v, err := strconv.Atoi(string(b))
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}

// globMatch reports whether str matches the glob pattern, the same operators
// Redis's stringmatchlen implements and the byte-for-byte twin of set/scan.go's
// matcher: * any run, ? one byte, [...] a class with ranges and a leading ^
// negation, and \ escaping the next byte. The match is byte-oriented and case
// sensitive, matching SCAN's MATCH.
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
