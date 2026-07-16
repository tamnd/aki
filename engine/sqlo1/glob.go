package sqlo1

// globMatch is the Redis pattern matcher, an exact port of the
// case-sensitive form of stringmatchlen_impl (util.c, Redis 8.0,
// BSD): * any run, ? any byte, [...] classes with ^ negation and a-z
// ranges (reversed ranges swap), backslash escapes inside and outside
// classes. Quirks port too: an unclosed class matches as if the ]
// were there, and a lone trailing backslash matches a literal one.
// HSCAN MATCH filters through it; SCAN and KEYS will share it when
// they land.
//
// The skip flag is upstream's guard against the exponential
// backtrack a pattern like a*a*a*a*b invites: once a * fails against
// every suffix from some position, a longer match for the pattern
// before it cannot help, so the failure propagates straight out. The
// nesting cap is upstream's too.
func globMatch(pattern, s []byte) bool {
	skip := false
	return globMatchImpl(pattern, s, &skip, 0)
}

func globMatchImpl(pattern, s []byte, skip *bool, nesting int) bool {
	if nesting > 1000 {
		return false
	}
	for len(pattern) > 0 && len(s) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) >= 2 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true
			}
			for len(s) > 0 {
				if globMatchImpl(pattern[1:], s, skip, nesting+1) {
					return true
				}
				if *skip {
					return false
				}
				s = s[1:]
			}
			*skip = true
			return false
		case '?':
			s = s[1:]
		case '[':
			pattern = pattern[1:]
			not := len(pattern) > 0 && pattern[0] == '^'
			if not {
				pattern = pattern[1:]
			}
			match := false
			for {
				if len(pattern) >= 2 && pattern[0] == '\\' {
					pattern = pattern[1:]
					if pattern[0] == s[0] {
						match = true
					}
				} else if len(pattern) == 0 {
					// Unclosed class: upstream backs up one byte so
					// the shared advance below lands on empty; a
					// synthetic ] does the same here.
					pattern = closingBracket
					break
				} else if pattern[0] == ']' {
					break
				} else if len(pattern) >= 3 && pattern[1] == '-' {
					start, end := pattern[0], pattern[2]
					if start > end {
						start, end = end, start
					}
					if s[0] >= start && s[0] <= end {
						match = true
					}
					pattern = pattern[2:]
				} else if pattern[0] == s[0] {
					match = true
				}
				pattern = pattern[1:]
			}
			if not {
				match = !match
			}
			if !match {
				return false
			}
			s = s[1:]
		case '\\':
			if len(pattern) >= 2 {
				pattern = pattern[1:]
			}
			fallthrough
		default:
			if pattern[0] != s[0] {
				return false
			}
			s = s[1:]
		}
		pattern = pattern[1:]
		if len(s) == 0 {
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			break
		}
	}
	return len(pattern) == 0 && len(s) == 0
}

var closingBracket = []byte{']'}
