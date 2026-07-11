package set

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// Sscan answers SSCAN key cursor [MATCH pattern] [COUNT count]: the inline
// band returns the whole set in one page with cursor 0 (doc 11 section 3.2),
// which satisfies the SCAN guarantee vacuously. COUNT is accepted and ignored
// because there is only ever one page; MATCH filters the members by glob.
func Sscan(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if _, ok := store.ParseInt(args[1]); !ok {
		r.Err("ERR invalid cursor")
		return
	}
	var match []byte
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
			if _, ok := store.ParseInt(args[i+1]); !ok {
				r.Err("ERR value is not an integer or out of range")
				return
			}
			i++
		default:
			r.Err("ERR syntax error")
			return
		}
	}

	g := registry(cx)
	s, wrong := g.lookup(cx, args[0])
	if wrong {
		r.Err(wrongType)
		return
	}

	// The reply is a two-element array: the next cursor (always "0" inline) and
	// the member page. The page is built first so its header carries the exact
	// post-filter count.
	page := cx.Val[:0]
	n := 0
	if s != nil {
		s.each(func(m []byte) {
			if match == nil || globMatch(match, m) {
				page = resp.AppendBulk(page, m)
				n++
			}
		})
	}
	cx.Val = page

	out := resp.AppendArrayHeader(cx.Aux[:0], 2)
	out = resp.AppendBulk(out, []byte("0"))
	out = resp.AppendArrayHeader(out, n)
	out = append(out, page...)
	cx.Aux = out
	r.Raw(out)
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
