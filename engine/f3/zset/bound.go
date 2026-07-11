package zset

// Range-bound grammars for the ZRANGE family (spec 2064/f3/12 section 6.5). A
// score bound is a float with an optional exclusive "(" prefix and the -inf/+inf
// literals; a lex bound is "[member" (inclusive), "(member" (exclusive), or the
// "-"/"+" lex infinities. Both parse into small value types the range executors
// turn into a rank window over the counted tree, so the bound grammar lives here
// and the seek logic lives in the band code.

// scoreBound is one end of a ZRANGEBYSCORE band: the float value and whether the
// bound is exclusive. -inf and +inf arrive as the float infinities, so an
// infinite bound is just a clamp to a key extreme (codec.go maps the infinities
// to the u64 key extremes) with no special case in the seek.
type scoreBound struct {
	value     float64
	exclusive bool
}

// parseScoreBound reads a ZRANGEBYSCORE min or max item: an optional "(" for the
// exclusive form, then a float that parseScore accepts (ordinary decimals, the
// inf spellings; NaN and garbage reject). A lone "(" rejects because parseScore
// refuses the empty string, matching Redis.
func parseScoreBound(b []byte) (scoreBound, bool) {
	ex := false
	if len(b) > 0 && b[0] == '(' {
		ex = true
		b = b[1:]
	}
	v, ok := parseScore(b)
	if !ok {
		return scoreBound{}, false
	}
	return scoreBound{value: v, exclusive: ex}, true
}

// lexInf names the lex infinities: -1 for "-" (below every member), +1 for "+"
// (above every member), 0 for an ordinary member bound.
const (
	lexNegInf = -1
	lexFinite = 0
	lexPosInf = 1
)

// lexBound is one end of a ZRANGEBYLEX band. When inf is lexFinite, value is the
// member bytes (aliasing the argument, valid for the command's duration) and
// exclusive says whether the bound itself is excluded; the "-"/"+" forms set inf
// and carry no value.
type lexBound struct {
	inf       int
	value     []byte
	exclusive bool
}

// parseLexBound reads a ZRANGEBYLEX min or max item exactly as Redis's
// zslParseLexRangeItem does: "-" and "+" (each length one) are the lex
// infinities, "[x" is inclusive and "(x" is exclusive with an empty member
// allowed, anything else is invalid. The returned value aliases b.
func parseLexBound(b []byte) (lexBound, bool) {
	if len(b) == 0 {
		return lexBound{}, false
	}
	switch b[0] {
	case '-':
		if len(b) == 1 {
			return lexBound{inf: lexNegInf}, true
		}
	case '+':
		if len(b) == 1 {
			return lexBound{inf: lexPosInf}, true
		}
	case '[':
		return lexBound{inf: lexFinite, value: b[1:]}, true
	case '(':
		return lexBound{inf: lexFinite, value: b[1:], exclusive: true}, true
	}
	return lexBound{}, false
}
