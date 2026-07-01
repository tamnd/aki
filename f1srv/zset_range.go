package f1srv

import (
	"encoding/binary"
	"strconv"
)

// The by-score and by-lex cursor forms are the zset's range reads that seek by value instead of
// by rank (spec 2064/f1_rewrite_ltm/07 section 2, the order view): ZRANGEBYSCORE and ZCOUNT walk
// the score-family rows, ZRANGEBYLEX and ZLEXCOUNT walk the member-family rows, and ZRANGE with
// BYSCORE/BYLEX routes into the same two paths. Every one of them turns its value bounds into two
// order-statistic rank lookups on the family's ordered index and reads only the window between
// them, so a count is two O(log n) descents with no scan and a range is one positional seek plus a
// bounded forward walk of exactly the elements returned. A million-element board answers a
// hundred-element window in a bounded handful of row reads and never materializes the collection.
//
// The window is a half-open index interval [startIdx, endIdx) into the family's order. Both ends
// come from one primitive: the count of rows sorting at or before a value boundary. For the score
// family a boundary is the 8-byte sortable score code, and "strictly below score s" versus "at or
// below s" is the difference between ranking prefix|sortable(s) and prefix|sortable(s)+1 (the code
// incremented as a big-endian integer, which lands just past every member sharing score s because
// the member bytes follow the code). For the member family a boundary is the member bytes, and "at
// or below m" ranks prefix|m|0x00 (0x00 sorts before any real continuation byte, so it counts m
// itself and nothing longer). Inclusive and exclusive bounds pick which of the two rank forms each
// end uses; the infinities short-circuit to 0 or the cardinality.

// inc8 returns b (an 8-byte big-endian value) plus one, and whether that overflowed past all-ones.
// It turns "rows with score < s" into "rows with score <= s": the incremented sortable code sorts
// immediately after every score-family key carrying score s, because those keys are the code
// followed by member bytes and the increment outranks any continuation. No finite score or
// infinity encodes to all-ones (that bit pattern is a NaN, which ingest rejects), so overflow
// never fires for a real bound; the guard keeps the arithmetic total anyway.
func inc8(b []byte) (out [8]byte, overflow bool) {
	copy(out[:], b)
	for i := 7; i >= 0; i-- {
		out[i]++
		if out[i] != 0 {
			return out, false
		}
	}
	return out, true
}

// parseScoreSpec parses a ZRANGEBYSCORE-style bound: an optional leading '(' marks it exclusive,
// and the rest is a score the way parseScore reads one (including inf/-inf). A bare "(" is invalid.
func parseScoreSpec(b []byte) (score float64, exclusive bool, err error) {
	if len(b) > 0 && b[0] == '(' {
		exclusive = true
		b = b[1:]
	}
	if len(b) == 0 {
		return 0, false, errInvalidFloat
	}
	score, err = parseScore(b)
	return score, exclusive, err
}

// lex bound kinds: a normal '[' or '(' member, or the '-'/'+' infinities that bound the whole
// member space regardless of the elements present.
const (
	lexNormal = 0
	lexMinInf = -1 // the "-" token: before every member
	lexMaxInf = 1  // the "+" token: after every member
)

// parseLexSpec parses a ZRANGEBYLEX-style bound: "-" and "+" are the member-space infinities,
// "[member" is inclusive, "(member" is exclusive. Any other leading byte is Redis's "not valid
// string range item" error. The returned member subslices the argument, valid for the call.
func parseLexSpec(b []byte) (kind int, member []byte, exclusive bool, err error) {
	if len(b) == 1 {
		switch b[0] {
		case '-':
			return lexMinInf, nil, false, nil
		case '+':
			return lexMaxInf, nil, false, nil
		}
	}
	if len(b) >= 1 {
		switch b[0] {
		case '[':
			return lexNormal, b[1:], false, nil
		case '(':
			return lexNormal, b[1:], true, nil
		}
	}
	return 0, nil, false, errInvalidFloat
}

// zscoreRankBoundary counts the score-family rows sorting below a score boundary: below the score
// when includeEqual is false, at or below it when includeEqual is true. It builds the boundary key
// in kbuf from the prefix held in pbuf and rides the ordered index's order-statistic rank.
func (c *connState) zscoreRankBoundary(prefix []byte, score float64, includeEqual bool, card int) int {
	var sc [8]byte
	encodeSortableScore(sc[:], score)
	b := c.kbuf[:0]
	b = append(b, prefix...)
	if includeEqual {
		inc, overflow := inc8(sc[:])
		if overflow {
			c.kbuf = b
			return card
		}
		b = append(b, inc[:]...)
	} else {
		b = append(b, sc[:]...)
	}
	c.kbuf = b
	r := c.srv.store.CollRankOf(prefix, b)
	if r > card {
		r = card
	}
	return r
}

// zlexRankBoundary counts the member-family rows sorting below a member boundary: below the member
// when includeEqual is false, at or below it when includeEqual is true (achieved by appending a
// 0x00 that sorts after the member and before any longer member). It builds the boundary in kbuf
// from the member prefix held in pbuf.
func (c *connState) zlexRankBoundary(prefix, member []byte, includeEqual bool, card int) int {
	b := c.kbuf[:0]
	b = append(b, prefix...)
	b = append(b, member...)
	if includeEqual {
		b = append(b, 0x00)
	}
	c.kbuf = b
	r := c.srv.store.CollRankOf(prefix, b)
	if r > card {
		r = card
	}
	return r
}

// zmemberPrefix builds the member-family enumeration prefix for zkey into pbuf:
// uvarint(len(zkey)) | zkey | 'm'. It is the by-lex counterpart of zscorePrefix, bounding a rank or
// scan to exactly the member rows of the zset in member-byte order.
func (c *connState) zmemberPrefix(zkey []byte) []byte {
	b := c.pbuf[:0]
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(zkey)))
	b = append(b, tmp[:n]...)
	b = append(b, zkey...)
	b = append(b, zsetMemberTag)
	c.pbuf = b
	return b
}

// windowLimit narrows a matched index window [startIdx, endIdx) by an optional LIMIT offset/count.
// A forward walk drops offset elements from the front and keeps count; a reverse walk (rev) drops
// offset from the back, because ZREVRANGEBYSCORE and REV apply the offset in traversal order. A
// negative count means "to the end", a negative offset empties the result (Redis's behavior). The
// returned window is still in ascending index order; a reverse caller emits it back to front.
func windowLimit(startIdx, endIdx, offset, count int, rev bool) (int, int) {
	if offset < 0 {
		return startIdx, startIdx
	}
	if !rev {
		lo := startIdx + offset
		hi := endIdx
		if count >= 0 && lo+count < hi {
			hi = lo + count
		}
		if lo > endIdx {
			lo = endIdx
		}
		return lo, hi
	}
	hi := endIdx - offset
	lo := startIdx
	if count >= 0 && hi-count > lo {
		lo = hi - count
	}
	if hi < startIdx {
		hi = startIdx
	}
	return lo, hi
}

// collectWindow gathers the family keys at ascending indices [lo, hi) under prefix: one positional
// seek to the first, then a bounded forward scan for the rest. It reuses c.zkeys and returns the
// grown slice. hi-lo is the exact element count, so the cost tracks the window, not the family.
func (c *connState) collectWindow(prefix []byte, lo, hi int) [][]byte {
	keys := c.zkeys[:0]
	if lo >= hi {
		c.zkeys = keys
		return keys
	}
	first, ok := c.srv.store.CollSelectAt(prefix, lo)
	if !ok {
		c.zkeys = keys
		return keys
	}
	keys = append(keys, first)
	if hi-lo > 1 {
		keys, _ = c.srv.store.CollScan(prefix, first, hi-lo-1, keys)
	}
	c.zkeys = keys
	return keys
}

// zByScore answers ZRANGEBYSCORE (rev=false) and ZREVRANGEBYSCORE (rev=true), and the BYSCORE form
// of ZRANGE. loArg and hiArg are the numeric-order bounds (the caller swaps them for a reverse
// command, whose wire order is max then min). It ranks both bounds on the score family, applies the
// optional LIMIT, reads the window, and emits it ascending (forward) or descending (rev), with
// scores when withScores is set.
func (c *connState) zByScore(zkey, loArg, hiArg []byte, rev, withScores, hasLimit bool, offset, count int) {
	if c.stringConflict(zkey) {
		c.writeErr(wrongType)
		return
	}
	lo, loExcl, err1 := parseScoreSpec(loArg)
	hi, hiExcl, err2 := parseScoreSpec(hiArg)
	if err1 != nil || err2 != nil {
		c.writeErr("ERR min or max is not a float")
		return
	}
	card := int(c.zsetCard(zkey))
	if card == 0 {
		c.writeArrayHeader(0)
		return
	}
	prefix := c.zscorePrefix(zkey)
	plen := len(prefix)
	// Inclusive min counts rows below lo; exclusive min counts rows at or below lo. Inclusive max
	// counts rows at or below hi; exclusive max counts rows below hi.
	startIdx := c.zscoreRankBoundary(prefix, lo, loExcl, card)
	endIdx := c.zscoreRankBoundary(prefix, hi, !hiExcl, card)
	if startIdx >= endIdx {
		c.writeArrayHeader(0)
		return
	}
	lo2, hi2 := startIdx, endIdx
	if hasLimit {
		lo2, hi2 = windowLimit(startIdx, endIdx, offset, count, rev)
	}
	keys := c.collectWindow(prefix, lo2, hi2)
	mult := 1
	if withScores {
		mult = 2
	}
	c.writeArrayHeader(len(keys) * mult)
	if rev {
		for i := len(keys) - 1; i >= 0; i-- {
			c.emitZrangeMember(keys[i], plen, withScores)
		}
		return
	}
	for _, k := range keys {
		c.emitZrangeMember(k, plen, withScores)
	}
}

// zByLex answers ZRANGEBYLEX (rev=false) and ZREVRANGEBYLEX (rev=true), and the BYLEX form of
// ZRANGE. It ranks both bounds on the member family (member bytes, one shared score assumed by the
// caller as Redis documents) and emits the members of the window; by-lex never carries scores.
func (c *connState) zByLex(zkey, loArg, hiArg []byte, rev, hasLimit bool, offset, count int) {
	if c.stringConflict(zkey) {
		c.writeErr(wrongType)
		return
	}
	loKind, loMember, loExcl, err1 := parseLexSpec(loArg)
	hiKind, hiMember, hiExcl, err2 := parseLexSpec(hiArg)
	if err1 != nil || err2 != nil {
		c.writeErr("ERR min or max not valid string range item")
		return
	}
	card := int(c.zsetCard(zkey))
	if card == 0 {
		c.writeArrayHeader(0)
		return
	}
	prefix := c.zmemberPrefix(zkey)
	plen := len(prefix)

	startIdx := 0
	switch loKind {
	case lexMinInf:
		startIdx = 0
	case lexMaxInf:
		startIdx = card
	default:
		// Inclusive '[' min wants member >= m (rows below m); exclusive '(' min wants member > m
		// (rows at or below m).
		startIdx = c.zlexRankBoundary(prefix, loMember, loExcl, card)
	}
	endIdx := card
	switch hiKind {
	case lexMaxInf:
		endIdx = card
	case lexMinInf:
		endIdx = 0
	default:
		// Inclusive '[' max wants member <= m (rows at or below m); exclusive '(' max wants
		// member < m (rows below m).
		endIdx = c.zlexRankBoundary(prefix, hiMember, !hiExcl, card)
	}
	if startIdx >= endIdx {
		c.writeArrayHeader(0)
		return
	}
	lo2, hi2 := startIdx, endIdx
	if hasLimit {
		lo2, hi2 = windowLimit(startIdx, endIdx, offset, count, rev)
	}
	keys := c.collectWindow(prefix, lo2, hi2)
	c.writeArrayHeader(len(keys))
	if rev {
		for i := len(keys) - 1; i >= 0; i-- {
			c.writeBulk(keys[i][plen:])
		}
		return
	}
	for _, k := range keys {
		c.writeBulk(k[plen:])
	}
}

// cmdZRangeByScore serves ZRANGEBYSCORE and, with rev, ZREVRANGEBYSCORE. The reverse command's
// wire order is key max min, so the bounds are swapped before the shared path sees them.
func (c *connState) cmdZRangeByScore(argv [][]byte, rev bool) {
	name := "zrangebyscore"
	if rev {
		name = "zrevrangebyscore"
	}
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	withScores, hasLimit, offset, count, ok := c.parseRangeTail(argv[4:], true)
	if !ok {
		return
	}
	loArg, hiArg := argv[2], argv[3]
	if rev {
		loArg, hiArg = argv[3], argv[2]
	}
	c.zByScore(argv[1], loArg, hiArg, rev, withScores, hasLimit, offset, count)
}

// cmdZRangeByLex serves ZRANGEBYLEX and, with rev, ZREVRANGEBYLEX. The reverse command's wire
// order is key max min, so the bounds are swapped before the shared path sees them.
func (c *connState) cmdZRangeByLex(argv [][]byte, rev bool) {
	name := "zrangebylex"
	if rev {
		name = "zrevrangebylex"
	}
	if len(argv) < 4 {
		c.writeErr("ERR wrong number of arguments for '" + name + "' command")
		return
	}
	withScores, hasLimit, offset, count, ok := c.parseRangeTail(argv[4:], false)
	if !ok {
		return
	}
	if withScores {
		c.writeErr("ERR syntax error")
		return
	}
	loArg, hiArg := argv[2], argv[3]
	if rev {
		loArg, hiArg = argv[3], argv[2]
	}
	c.zByLex(argv[1], loArg, hiArg, rev, hasLimit, offset, count)
}

// cmdZCount answers ZCOUNT key min max: the size of the score window, two rank lookups and no
// scan. It shares the boundary arithmetic with zByScore.
func (c *connState) cmdZCount(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'zcount' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	lo, loExcl, err1 := parseScoreSpec(argv[2])
	hi, hiExcl, err2 := parseScoreSpec(argv[3])
	if err1 != nil || err2 != nil {
		c.writeErr("ERR min or max is not a float")
		return
	}
	card := int(c.zsetCard(argv[1]))
	if card == 0 {
		c.writeInt(0)
		return
	}
	prefix := c.zscorePrefix(argv[1])
	startIdx := c.zscoreRankBoundary(prefix, lo, loExcl, card)
	endIdx := c.zscoreRankBoundary(prefix, hi, !hiExcl, card)
	if startIdx >= endIdx {
		c.writeInt(0)
		return
	}
	c.writeInt(int64(endIdx - startIdx))
}

// cmdZLexCount answers ZLEXCOUNT key min max: the size of the member window, two rank lookups and
// no scan. It shares the boundary arithmetic with zByLex.
func (c *connState) cmdZLexCount(argv [][]byte) {
	if len(argv) != 4 {
		c.writeErr("ERR wrong number of arguments for 'zlexcount' command")
		return
	}
	if c.stringConflict(argv[1]) {
		c.writeErr(wrongType)
		return
	}
	loKind, loMember, loExcl, err1 := parseLexSpec(argv[2])
	hiKind, hiMember, hiExcl, err2 := parseLexSpec(argv[3])
	if err1 != nil || err2 != nil {
		c.writeErr("ERR min or max not valid string range item")
		return
	}
	card := int(c.zsetCard(argv[1]))
	if card == 0 {
		c.writeInt(0)
		return
	}
	prefix := c.zmemberPrefix(argv[1])
	startIdx := 0
	switch loKind {
	case lexMinInf:
		startIdx = 0
	case lexMaxInf:
		startIdx = card
	default:
		startIdx = c.zlexRankBoundary(prefix, loMember, loExcl, card)
	}
	endIdx := card
	switch hiKind {
	case lexMaxInf:
		endIdx = card
	case lexMinInf:
		endIdx = 0
	default:
		endIdx = c.zlexRankBoundary(prefix, hiMember, !hiExcl, card)
	}
	if startIdx >= endIdx {
		c.writeInt(0)
		return
	}
	c.writeInt(int64(endIdx - startIdx))
}

// parseRangeTail reads the option tail shared by the by-score and by-lex commands: an optional
// WITHSCORES (only meaningful to by-score, but parsed either way so the by-lex caller can reject
// it with the right message) and an optional LIMIT offset count. It returns ok=false and writes
// the wire error itself on a malformed tail.
func (c *connState) parseRangeTail(opts [][]byte, allowWithScores bool) (withScores, hasLimit bool, offset, count int, ok bool) {
	for i := 0; i < len(opts); i++ {
		switch {
		case allowWithScores && eqFold(opts[i], "WITHSCORES"):
			withScores = true
		case eqFold(opts[i], "LIMIT"):
			if i+2 >= len(opts) {
				c.writeErr("ERR syntax error")
				return false, false, 0, 0, false
			}
			o, err1 := strconv.Atoi(string(opts[i+1]))
			n, err2 := strconv.Atoi(string(opts[i+2]))
			if err1 != nil || err2 != nil {
				c.writeErr("ERR value is not an integer or out of range")
				return false, false, 0, 0, false
			}
			hasLimit = true
			offset = o
			count = n
			i += 2
		default:
			c.writeErr("ERR syntax error")
			return false, false, 0, 0, false
		}
	}
	return withScores, hasLimit, offset, count, true
}
