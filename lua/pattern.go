package lua

// This file is the Lua 5.1 pattern matching engine used by string.find, match,
// gmatch, and gsub. It is a direct port of the recursive matcher the reference
// Lua implementation uses (lstrlib.c), covering character classes, sets, the
// quantifiers * + - ?, captures, %b balanced matches, %f frontier patterns, and
// position captures.

const maxCaptures = 32

// capture records one open or closed capture during a match. len == capPosition
// marks a position capture; len == capUnfinished marks one still open.
type capture struct {
	start int
	len   int
}

const (
	capUnfinished = -1
	capPosition   = -2
)

// matcher holds the state for one pattern match attempt against a subject.
type matcher struct {
	src      string
	pat      string
	caps     []capture
	matchErr *Error
}

func newMatcher(src, pat string) *matcher {
	return &matcher{src: src, pat: pat}
}

// match tries the pattern from position init onward, sliding the start forward
// unless the pattern is anchored with ^. It returns the match start and end byte
// offsets, the capture values, and whether a match was found.
func (m *matcher) match(init int) (start, end int, caps []Value, ok bool) {
	anchor := false
	pp := 0
	if len(m.pat) > 0 && m.pat[0] == '^' {
		anchor = true
		pp = 1
	}
	sp := init
	for {
		m.caps = m.caps[:0]
		e := m.doMatch(sp, pp)
		if e >= 0 {
			return sp, e, m.captureValues(), true
		}
		sp++
		if anchor || sp > len(m.src) {
			return 0, 0, nil, false
		}
	}
}

// matchAt tries the pattern only at exactly position pos (used by gmatch and
// gsub which advance the cursor themselves), still honoring a ^ anchor.
func (m *matcher) matchAt(pos int) (start, end int, caps []Value, ok bool) {
	pp := 0
	if len(m.pat) > 0 && m.pat[0] == '^' {
		pp = 1
	}
	m.caps = m.caps[:0]
	e := m.doMatch(pos, pp)
	if e >= 0 {
		return pos, e, m.captureValues(), true
	}
	return 0, 0, nil, false
}

// captureValues converts the recorded captures into Lua values. With no explicit
// captures the whole match is the implicit capture handled by the caller.
func (m *matcher) captureValues() []Value {
	if len(m.caps) == 0 {
		return nil
	}
	out := make([]Value, len(m.caps))
	for i, c := range m.caps {
		if c.len == capPosition {
			out[i] = Number(c.start + 1)
		} else {
			out[i] = String(m.src[c.start : c.start+c.len])
		}
	}
	return out
}

// doMatch is the recursive matcher. sp is the subject position, pp the pattern
// position. It returns the subject end position on success or -1 on failure.
func (m *matcher) doMatch(sp, pp int) int {
	if m.matchErr != nil {
		return -1
	}
	if pp >= len(m.pat) {
		return sp
	}
	switch m.pat[pp] {
	case '(':
		if pp+1 < len(m.pat) && m.pat[pp+1] == ')' {
			return m.startCapture(sp, pp+2, capPosition)
		}
		return m.startCapture(sp, pp+1, capUnfinished)
	case ')':
		return m.endCapture(sp, pp+1)
	case '$':
		if pp+1 == len(m.pat) {
			if sp == len(m.src) {
				return sp
			}
			return -1
		}
	case '%':
		if pp+1 < len(m.pat) {
			switch m.pat[pp+1] {
			case 'b':
				return m.matchBalance(sp, pp+2)
			case 'f':
				return m.matchFrontier(sp, pp+2)
			default:
				if m.pat[pp+1] >= '0' && m.pat[pp+1] <= '9' {
					return m.matchBackref(sp, pp)
				}
			}
		}
	}
	return m.matchDefault(sp, pp)
}

// matchDefault handles a single pattern item followed by an optional quantifier.
func (m *matcher) matchDefault(sp, pp int) int {
	ep := m.classEnd(pp)
	matches := sp < len(m.src) && m.singleMatch(m.src[sp], pp, ep)
	if ep < len(m.pat) {
		switch m.pat[ep] {
		case '?':
			if matches {
				if r := m.doMatch(sp+1, ep+1); r >= 0 {
					return r
				}
			}
			return m.doMatch(sp, ep+1)
		case '+':
			if matches {
				return m.maxExpand(sp+1, pp, ep)
			}
			return -1
		case '*':
			return m.maxExpand(sp, pp, ep)
		case '-':
			return m.minExpand(sp, pp, ep)
		}
	}
	if !matches {
		return -1
	}
	return m.doMatch(sp+1, ep)
}

// maxExpand matches as many repetitions as possible then backtracks.
func (m *matcher) maxExpand(sp, pp, ep int) int {
	i := 0
	for sp+i < len(m.src) && m.singleMatch(m.src[sp+i], pp, ep) {
		i++
	}
	for i >= 0 {
		if r := m.doMatch(sp+i, ep+1); r >= 0 {
			return r
		}
		i--
	}
	return -1
}

// minExpand matches as few repetitions as possible then grows.
func (m *matcher) minExpand(sp, pp, ep int) int {
	for {
		if r := m.doMatch(sp, ep+1); r >= 0 {
			return r
		}
		if sp < len(m.src) && m.singleMatch(m.src[sp], pp, ep) {
			sp++
		} else {
			return -1
		}
	}
}

func (m *matcher) startCapture(sp, pp, what int) int {
	if len(m.caps) >= maxCaptures {
		m.matchErr = runtimeErr("too many captures")
		return -1
	}
	m.caps = append(m.caps, capture{start: sp, len: what})
	r := m.doMatch(sp, pp)
	if r < 0 {
		m.caps = m.caps[:len(m.caps)-1]
	}
	return r
}

func (m *matcher) endCapture(sp, pp int) int {
	idx := m.lastUnfinished()
	if idx < 0 {
		m.matchErr = runtimeErr("invalid pattern capture")
		return -1
	}
	m.caps[idx].len = sp - m.caps[idx].start
	r := m.doMatch(sp, pp)
	if r < 0 {
		m.caps[idx].len = capUnfinished
	}
	return r
}

func (m *matcher) lastUnfinished() int {
	for i := len(m.caps) - 1; i >= 0; i-- {
		if m.caps[i].len == capUnfinished {
			return i
		}
	}
	return -1
}

// matchBalance handles %bxy: matches a balanced run starting with x and ending
// with the matching y.
func (m *matcher) matchBalance(sp, pp int) int {
	if pp+1 >= len(m.pat) {
		m.matchErr = runtimeErr("malformed pattern (missing arguments to '%%b')")
		return -1
	}
	if sp >= len(m.src) || m.src[sp] != m.pat[pp] {
		return -1
	}
	open, close := m.pat[pp], m.pat[pp+1]
	depth := 1
	i := sp + 1
	for i < len(m.src) {
		switch m.src[i] {
		case close:
			depth--
			if depth == 0 {
				return m.doMatch(i+1, pp+2)
			}
		case open:
			depth++
		}
		i++
	}
	return -1
}

// matchFrontier handles %f[set]: a zero-width match at a boundary where the
// previous byte is not in the set and the current byte is.
func (m *matcher) matchFrontier(sp, pp int) int {
	if pp >= len(m.pat) || m.pat[pp] != '[' {
		m.matchErr = runtimeErr("missing '[' after '%%f' in pattern")
		return -1
	}
	ep := m.classEnd(pp)
	var prev byte
	if sp > 0 {
		prev = m.src[sp-1]
	}
	var cur byte
	if sp < len(m.src) {
		cur = m.src[sp]
	}
	if !m.matchClass2(prev, pp, ep) && m.matchClass2(cur, pp, ep) {
		return m.doMatch(sp, ep)
	}
	return -1
}

// matchBackref handles %1 to %9: matches the text of a previous capture.
func (m *matcher) matchBackref(sp, pp int) int {
	idx := int(m.pat[pp+1] - '1')
	if idx < 0 || idx >= len(m.caps) || m.caps[idx].len == capUnfinished {
		m.matchErr = runtimeErr("invalid capture index %%%c", m.pat[pp+1])
		return -1
	}
	c := m.caps[idx]
	text := m.src[c.start : c.start+c.len]
	if sp+len(text) <= len(m.src) && m.src[sp:sp+len(text)] == text {
		return m.doMatch(sp+len(text), pp+2)
	}
	return -1
}

// classEnd returns the pattern position just past a single pattern item starting
// at pp (a literal, a %class, or a [set]).
func (m *matcher) classEnd(pp int) int {
	c := m.pat[pp]
	pp++
	if c == '%' {
		return pp + 1
	}
	if c == '[' {
		if pp < len(m.pat) && m.pat[pp] == '^' {
			pp++
		}
		// A ] immediately after [ or [^ is a literal.
		for {
			if pp >= len(m.pat) {
				return pp
			}
			cc := m.pat[pp]
			pp++
			switch cc {
			case '%':
				pp++
			case ']':
				return pp
			}
		}
	}
	return pp
}

// singleMatch reports whether byte b matches the pattern item in pp..ep.
func (m *matcher) singleMatch(b byte, pp, ep int) bool {
	switch m.pat[pp] {
	case '.':
		return true
	case '%':
		return matchClassChar(b, m.pat[pp+1])
	case '[':
		return m.matchClass2(b, pp, ep)
	default:
		return m.pat[pp] == b
	}
}

// matchClass2 tests a byte against a [set] running from pp (the '[') to ep.
func (m *matcher) matchClass2(b byte, pp, ep int) bool {
	pp++ // skip [
	negate := false
	if pp < len(m.pat) && m.pat[pp] == '^' {
		negate = true
		pp++
	}
	res := false
	for pp < ep-1 {
		if m.pat[pp] == '%' && pp+1 < ep-1+1 {
			pp++
			if matchClassChar(b, m.pat[pp]) {
				res = true
			}
			pp++
		} else if pp+2 < ep-1+1 && m.pat[pp+1] == '-' && pp+2 < ep-1 {
			if m.pat[pp] <= b && b <= m.pat[pp+2] {
				res = true
			}
			pp += 3
		} else {
			if m.pat[pp] == b {
				res = true
			}
			pp++
		}
	}
	if negate {
		return !res
	}
	return res
}

// matchClassChar tests a byte against a single %class letter.
func matchClassChar(b, class byte) bool {
	var res bool
	switch lower(class) {
	case 'a':
		res = isLetter(b)
	case 'd':
		res = b >= '0' && b <= '9'
	case 's':
		res = b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\v' || b == '\f'
	case 'w':
		res = isLetter(b) || (b >= '0' && b <= '9')
	case 'l':
		res = b >= 'a' && b <= 'z'
	case 'u':
		res = b >= 'A' && b <= 'Z'
	case 'p':
		res = isPunct(b)
	case 'c':
		res = b < 32 || b == 127
	case 'x':
		res = isHexDig(b)
	default:
		// A non-letter after % is the literal character itself.
		return class == b
	}
	if class >= 'A' && class <= 'Z' {
		return !res
	}
	return res
}

func lower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 32
	}
	return b
}

func isLetter(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') }

func isPunct(b byte) bool {
	return (b >= '!' && b <= '/') || (b >= ':' && b <= '@') ||
		(b >= '[' && b <= '`') || (b >= '{' && b <= '~')
}
