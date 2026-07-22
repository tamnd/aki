package set

import (
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// defaultScanCount is the COUNT hint Redis applies when a client omits it: a
// page examines this many draw slots unless COUNT says otherwise (doc 11
// section 8.3, COUNT bounds elements examined, not returned).
const defaultScanCount = 10

// Sscan answers SSCAN key cursor [MATCH pattern] [COUNT count].
//
// The cursor rides the draw vector downward (doc 11 section 8.2): each page
// examines COUNT slots from the top of the unscanned region and returns the
// lower boundary as the next cursor, 0 when the scan is complete. The port
// carries doc 20's correctness proof whole: swap-remove only moves the vector's
// last element into a vacated slot, so an element present for the whole scan is
// either still below the cursor when the cursor reaches it or was moved there
// from the already-scanned side, and in both cases it is returned at least once
// (scanPage below). The inline bands have no vector, so they return the whole
// set in one page with cursor 0, which satisfies the guarantee vacuously (doc
// 11 section 3.2). COUNT is a hint; MATCH filters the emitted members by glob.
func Sscan(cx *shard.Ctx, args [][]byte, r shard.Reply) {
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
			c, ok := parseCount(args[i+1])
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
	s, home := g.resolveTouch(cx, args[0])
	if home == homeString {
		r.Err(wrongType)
		return
	}

	// The reply is a two-element array: the next cursor and the member page.
	// The page is built first so its header carries the exact post-filter count.
	page := cx.Val[:0]
	n := 0
	var next uint64
	if s != nil {
		next = s.scanPage(cursor, count, match, func(m []byte) {
			page = resp.AppendBulk(page, m)
			n++
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

// parseCount reads a COUNT argument: a positive integer. Redis rejects zero and
// negatives with a syntax error, so anything not strictly positive is invalid.
func parseCount(b []byte) (int, bool) {
	v, err := strconv.Atoi(string(b))
	if err != nil || v < 1 {
		return 0, false
	}
	return v, true
}

// scanPage returns one SSCAN page starting from cursor and reports the cursor to
// resume from, 0 when the scan is complete. It examines at most count draw slots
// and emits each survivor of the MATCH filter. The inline bands are one page;
// the hashtable band walks the downward cursor (member.go, scanPage on htable).
func (s *set) scanPage(cursor uint64, count int, match []byte, emit func(m []byte)) uint64 {
	switch s.enc {
	case encIntset, encListpack:
		// One page, cursor 0. An inline set never converts back down (F4), so a
		// nonzero cursor here is only a client replaying a finished scan; it
		// gets the done cursor and no members.
		if cursor != 0 {
			return 0
		}
		s.each(func(m []byte) {
			if match == nil || globMatch(match, m) {
				emit(m)
			}
		})
		return 0
	case encPartitioned:
		return s.part.scanPage(cursor, count, match, emit)
	default:
		return s.ht.scanPage(cursor, count, match, emit)
	}
}

// scanPage is the hashtable band's downward cursor (doc 11 section 8.2). The
// cursor is the boundary: draw-vector indices [b, len) have been returned by
// earlier pages and [0, b) remain. The page examines up to count slots
// downward from b and returns the new lower boundary, or 0 when it reaches the
// bottom. A fresh scan (cursor 0) opens with the whole vector unscanned; a
// resumed cursor is clamped to the current length, since a mid-scan shrink can
// only have carried the old boundary past the new end, and additions land above
// the boundary (the already-scanned side) where this walk never revisits them.
func (h *htable) scanPage(cursor uint64, count int, match []byte, emit func(m []byte)) uint64 {
	n := uint64(len(h.vec))
	if n == 0 {
		return 0
	}
	b := n
	if cursor != 0 && cursor < b {
		b = cursor
	}
	lo := uint64(0)
	if b > uint64(count) {
		lo = b - uint64(count)
	}
	for i := b; i > lo; i-- {
		m := h.memberByOrd(h.vec[i-1])
		if match == nil || globMatch(match, m) {
			emit(m)
		}
	}
	return lo
}

// eqFold reports whether b equals the uppercase option name s, ASCII case
// insensitive, without allocating.
func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

// globMatch reports whether str matches the glob pattern, the same operators
// Redis's stringmatchlen implements: * any run, ? one byte, [...] a class with
// ranges and a leading ^ negation, and \ escaping the next byte. The match is
// byte-oriented and case sensitive, matching SCAN's MATCH.
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
