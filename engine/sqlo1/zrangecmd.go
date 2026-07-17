package sqlo1

// The zset range family's command half: ZRANGE with its BY forms,
// the legacy BYSCORE/BYLEX/REV spellings, ZCOUNT, ZLEXCOUNT, and
// ZRANGESTORE. Every form resolves its bound grammar into a forward
// rank interval [lo, hi) over the two zrange.go primitives: a score
// bound becomes an insertion-rank seek of (sortable, ""), a lex bound
// a seek of (first sortable, member) with a low byte appended for the
// exclusive side, and the index form plain ZCard arithmetic. REV and
// LIMIT are rank arithmetic on the interval, so a reversed or
// offset-limited range still reads only the runs the final window
// spans. Forward replies stream straight from the walk with the array
// header known upfront; REV replies buffer copies (walk-emitted bytes
// die as the walk advances) and replay them backward.

import (
	"context"
	"math"
	"strconv"
	"strings"
)

// The BY forms of ZRANGE and ZRANGESTORE.
const (
	zrangeByIndex = iota
	zrangeByScore
	zrangeByLex
)

// The lex bound kinds of zslParseLexRangeItem.
const (
	zlexNegInf = iota // -
	zlexPosInf        // +
	zlexIncl          // [member
	zlexExcl          // (member
)

// parseZScoreBound parses a score range bound: an optional ( for
// exclusive, then a float, with the infinities spelled as Redis's
// strtod accepts them. NaN is not a bound.
func parseZScoreBound(arg []byte) (f float64, excl, ok bool) {
	if len(arg) > 0 && arg[0] == '(' {
		excl = true
		arg = arg[1:]
	}
	f, err := strconv.ParseFloat(string(arg), 64)
	if err != nil || math.IsNaN(f) {
		return 0, false, false
	}
	return f, excl, true
}

// parseZLexBound parses a lex range bound: -, +, [member, or
// (member.
func parseZLexBound(arg []byte) (kind int, member []byte, ok bool) {
	if len(arg) == 0 {
		return 0, nil, false
	}
	switch arg[0] {
	case '-':
		if len(arg) == 1 {
			return zlexNegInf, nil, true
		}
	case '+':
		if len(arg) == 1 {
			return zlexPosInf, nil, true
		}
	case '[':
		return zlexIncl, arg[1:], true
	case '(':
		return zlexExcl, arg[1:], true
	}
	return 0, nil, false
}

// zscoreInterval resolves parsed score bounds to forward ranks. An
// inclusive bound seeks its own sortable, an exclusive one the next:
// the seek member is empty, so the rank counts exactly the entries
// with a smaller score (or a smaller-or-equal one for the successor).
// The successor never wraps because the largest legal sortable is
// +inf's.
func (s *Server) zscoreInterval(ctx context.Context, key []byte, minF float64, minEx bool, maxF float64, maxEx bool) (lo, hi int64, err error) {
	if minF > maxF || (minF == maxF && (minEx || maxEx)) {
		return 0, 0, nil
	}
	smin := zScoreSortable(minF)
	if minEx {
		smin++
	}
	smax := zScoreSortable(maxF)
	if !maxEx {
		smax++
	}
	if lo, _, err = s.z.zseekRank(ctx, key, smin, nil); err != nil {
		return 0, 0, err
	}
	if hi, _, err = s.z.zseekRank(ctx, key, smax, nil); err != nil {
		return 0, 0, err
	}
	if hi < lo {
		hi = lo
	}
	return lo, hi, nil
}

// zlexInterval resolves parsed lex bounds to forward ranks. Every
// member bound pairs with the zset's first sortable (the shared score
// the lex forms assume), and the exclusive side of a member appends a
// low byte: no member orders between m and m plus "\x00", so the seek
// counts exactly the members up to and including m.
func (s *Server) zlexInterval(ctx context.Context, key []byte, minKind int, minM []byte, maxKind int, maxM []byte) (lo, hi int64, err error) {
	if minKind == zlexPosInf || maxKind == zlexNegInf {
		return 0, 0, nil
	}
	s0, ok, err := s.z.zfirstSortable(ctx, key)
	if err != nil || !ok {
		return 0, 0, err
	}
	seek := func(kind int, m []byte, isStart bool) (int64, error) {
		if kind == zlexNegInf {
			return 0, nil
		}
		if kind == zlexPosInf {
			return s.z.ZCard(ctx, key)
		}
		if (isStart && kind == zlexExcl) || (!isStart && kind == zlexIncl) {
			s.zlexbuf = append(s.zlexbuf[:0], m...)
			s.zlexbuf = append(s.zlexbuf, 0)
			m = s.zlexbuf
		}
		r, _, err := s.z.zseekRank(ctx, key, s0, m)
		return r, err
	}
	if lo, err = seek(minKind, minM, true); err != nil {
		return 0, 0, err
	}
	if hi, err = seek(maxKind, maxM, false); err != nil {
		return 0, 0, err
	}
	if hi < lo {
		hi = lo
	}
	return lo, hi, nil
}

// zindexInterval resolves index bounds against the cardinality:
// negatives count from the end, out-of-range clamps, and a REV pair
// mirrors onto forward ranks.
func zindexInterval(card, start, stop int64, rev bool) (lo, hi int64) {
	if start < 0 {
		start += card
	}
	if stop < 0 {
		stop += card
	}
	if start < 0 {
		start = 0
	}
	if stop >= card {
		stop = card - 1
	}
	if card == 0 || start > stop {
		return 0, 0
	}
	if rev {
		return card - 1 - stop, card - start
	}
	return start, stop + 1
}

// zlimitFold applies LIMIT offset count to a forward interval: the
// offset consumes from the direction's near end, the count caps what
// remains (negative meaning all), and a negative offset empties the
// range.
func zlimitFold(lo, hi, offset, count int64, rev bool) (int64, int64) {
	if offset < 0 || offset >= hi-lo {
		return hi, hi
	}
	if rev {
		hi -= offset
		if count >= 0 && count < hi-lo {
			lo = hi - count
		}
		return lo, hi
	}
	lo += offset
	if count >= 0 && count < hi-lo {
		hi = lo + count
	}
	return lo, hi
}

// zrangeInterval resolves one parsed range form to forward ranks
// [lo, hi). A non-empty errMsg is the wire error the bound grammar
// produced.
func (s *Server) zrangeInterval(ctx context.Context, key, startArg, stopArg []byte, by int, rev bool) (lo, hi int64, errMsg string, err error) {
	switch by {
	case zrangeByIndex:
		start, e1 := strconv.ParseInt(string(startArg), 10, 64)
		stop, e2 := strconv.ParseInt(string(stopArg), 10, 64)
		if e1 != nil || e2 != nil {
			return 0, 0, "ERR value is not an integer or out of range", nil
		}
		card, err := s.z.ZCard(ctx, key)
		if err != nil {
			return 0, 0, "", err
		}
		lo, hi = zindexInterval(card, start, stop, rev)
		return lo, hi, "", nil
	case zrangeByScore:
		minArg, maxArg := startArg, stopArg
		if rev {
			minArg, maxArg = stopArg, startArg
		}
		minF, minEx, ok1 := parseZScoreBound(minArg)
		maxF, maxEx, ok2 := parseZScoreBound(maxArg)
		if !ok1 || !ok2 {
			return 0, 0, "ERR min or max is not a float", nil
		}
		lo, hi, err = s.zscoreInterval(ctx, key, minF, minEx, maxF, maxEx)
		return lo, hi, "", err
	default:
		minArg, maxArg := startArg, stopArg
		if rev {
			minArg, maxArg = stopArg, startArg
		}
		minKind, minM, ok1 := parseZLexBound(minArg)
		maxKind, maxM, ok2 := parseZLexBound(maxArg)
		if !ok1 || !ok2 {
			return 0, 0, "ERR min or max not valid string range item", nil
		}
		lo, hi, err = s.zlexInterval(ctx, key, minKind, minM, maxKind, maxM)
		return lo, hi, "", err
	}
}

// zrangeEmit replies the forward rank window: streamed straight from
// the walk when forward (the interval length is the array header), or
// buffered and replayed backward under REV.
func (s *Server) zrangeEmit(ctx context.Context, reply []byte, key []byte, lo, hi int64, rev, withScores bool) []byte {
	n := hi - lo
	if n <= 0 {
		return AppendArray(reply, 0)
	}
	vals := 1
	if withScores {
		vals = 2
	}
	mark := len(reply)
	reply = AppendArray(reply, int(n)*vals)
	var sb [32]byte
	if !rev {
		err := s.z.zwalkRank(ctx, key, lo, hi, func(su uint64, m []byte) bool {
			reply = AppendBulk(reply, m)
			if withScores {
				reply = AppendBulk(reply, appendScore(sb[:0], zScoreFromSortable(su)))
			}
			return true
		})
		if err != nil {
			return storeErr(reply[:mark], err)
		}
		return reply
	}
	s.zrarena = s.zrarena[:0]
	s.zrpairs = s.zrpairs[:0]
	err := s.z.zwalkRank(ctx, key, lo, hi, func(su uint64, m []byte) bool {
		off := len(s.zrarena)
		s.zrarena = append(s.zrarena, m...)
		s.zrpairs = append(s.zrpairs, zbuildPair{s: su, off: off, end: len(s.zrarena)})
		return true
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	for i := len(s.zrpairs) - 1; i >= 0; i-- {
		p := s.zrpairs[i]
		reply = AppendBulk(reply, s.zrarena[p.off:p.end])
		if withScores {
			reply = AppendBulk(reply, appendScore(sb[:0], zScoreFromSortable(p.s)))
		}
	}
	return reply
}

// zrangeCmd is ZRANGE with the full Redis 8 surface: the index form,
// BYSCORE, BYLEX, REV, LIMIT (BY forms only), WITHSCORES (not with
// BYLEX).
func (s *Server) zrangeCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "ZRANGE")
	}
	by := zrangeByIndex
	rev, withScores, haveLimit := false, false, false
	var offset, count int64
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "BYSCORE":
			by = zrangeByScore
		case "BYLEX":
			by = zrangeByLex
		case "REV":
			rev = true
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return syntaxErr(reply)
			}
			var e1, e2 error
			offset, e1 = strconv.ParseInt(string(args[i+1]), 10, 64)
			count, e2 = strconv.ParseInt(string(args[i+2]), 10, 64)
			if e1 != nil || e2 != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			haveLimit = true
			i += 2
		default:
			return syntaxErr(reply)
		}
	}
	if haveLimit && by == zrangeByIndex {
		return AppendError(reply, "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
	}
	if withScores && by == zrangeByLex {
		return AppendError(reply, "ERR syntax error, WITHSCORES not supported in combination with BYLEX")
	}
	lo, hi, errMsg, err := s.zrangeInterval(ctx, args[1], args[2], args[3], by, rev)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	if haveLimit {
		lo, hi = zlimitFold(lo, hi, offset, count, rev)
	}
	return s.zrangeEmit(ctx, reply, args[1], lo, hi, rev, withScores)
}

// zrangebyscoreCmd is ZRANGEBYSCORE and ZREVRANGEBYSCORE, the legacy
// spellings: WITHSCORES and LIMIT, bounds already in the direction's
// order.
func (s *Server) zrangebyscoreCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, rev bool) []byte {
	if len(args) < 4 {
		return arityErr(reply, cmd)
	}
	withScores, haveLimit := false, false
	var offset, count int64
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return syntaxErr(reply)
			}
			var e1, e2 error
			offset, e1 = strconv.ParseInt(string(args[i+1]), 10, 64)
			count, e2 = strconv.ParseInt(string(args[i+2]), 10, 64)
			if e1 != nil || e2 != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			haveLimit = true
			i += 2
		default:
			return syntaxErr(reply)
		}
	}
	lo, hi, errMsg, err := s.zrangeInterval(ctx, args[1], args[2], args[3], zrangeByScore, rev)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	if haveLimit {
		lo, hi = zlimitFold(lo, hi, offset, count, rev)
	}
	return s.zrangeEmit(ctx, reply, args[1], lo, hi, rev, withScores)
}

// zrangebylexCmd is ZRANGEBYLEX and ZREVRANGEBYLEX: LIMIT only.
func (s *Server) zrangebylexCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, rev bool) []byte {
	if len(args) < 4 {
		return arityErr(reply, cmd)
	}
	haveLimit := false
	var offset, count int64
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "LIMIT":
			if i+2 >= len(args) {
				return syntaxErr(reply)
			}
			var e1, e2 error
			offset, e1 = strconv.ParseInt(string(args[i+1]), 10, 64)
			count, e2 = strconv.ParseInt(string(args[i+2]), 10, 64)
			if e1 != nil || e2 != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			haveLimit = true
			i += 2
		default:
			return syntaxErr(reply)
		}
	}
	lo, hi, errMsg, err := s.zrangeInterval(ctx, args[1], args[2], args[3], zrangeByLex, rev)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	if haveLimit {
		lo, hi = zlimitFold(lo, hi, offset, count, rev)
	}
	return s.zrangeEmit(ctx, reply, args[1], lo, hi, rev, false)
}

// zrevrangeCmd is legacy ZREVRANGE: the index form reversed, with
// WITHSCORES.
func (s *Server) zrevrangeCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "ZREVRANGE")
	}
	withScores := false
	if len(args) == 5 && strings.EqualFold(string(args[4]), "WITHSCORES") {
		withScores = true
	} else if len(args) >= 5 {
		return syntaxErr(reply)
	}
	lo, hi, errMsg, err := s.zrangeInterval(ctx, args[1], args[2], args[3], zrangeByIndex, true)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	return s.zrangeEmit(ctx, reply, args[1], lo, hi, true, withScores)
}

// zcountCmd is ZCOUNT: the score interval's width, two insertion-rank
// seeks and no run streaming.
func (s *Server) zcountCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 4 {
		return arityErr(reply, "ZCOUNT")
	}
	minF, minEx, ok1 := parseZScoreBound(args[2])
	maxF, maxEx, ok2 := parseZScoreBound(args[3])
	if !ok1 || !ok2 {
		return AppendError(reply, "ERR min or max is not a float")
	}
	lo, hi, err := s.zscoreInterval(ctx, args[1], minF, minEx, maxF, maxEx)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, hi-lo)
}

// zlexcountCmd is ZLEXCOUNT: ZCOUNT over lex bounds.
func (s *Server) zlexcountCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 4 {
		return arityErr(reply, "ZLEXCOUNT")
	}
	minKind, minM, ok1 := parseZLexBound(args[2])
	maxKind, maxM, ok2 := parseZLexBound(args[3])
	if !ok1 || !ok2 {
		return AppendError(reply, "ERR min or max not valid string range item")
	}
	lo, hi, err := s.zlexInterval(ctx, args[1], minKind, minM, maxKind, maxM)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, hi-lo)
}

// zrangestoreCmd is ZRANGESTORE: the ZRANGE surface minus WITHSCORES,
// resolved on the source and landed through the bulk build. The
// stored window is direction-free; REV only steers which end LIMIT
// consumes.
func (s *Server) zrangestoreCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 5 {
		return arityErr(reply, "ZRANGESTORE")
	}
	by := zrangeByIndex
	rev, haveLimit := false, false
	var offset, count int64
	for i := 5; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "BYSCORE":
			by = zrangeByScore
		case "BYLEX":
			by = zrangeByLex
		case "REV":
			rev = true
		case "LIMIT":
			if i+2 >= len(args) {
				return syntaxErr(reply)
			}
			var e1, e2 error
			offset, e1 = strconv.ParseInt(string(args[i+1]), 10, 64)
			count, e2 = strconv.ParseInt(string(args[i+2]), 10, 64)
			if e1 != nil || e2 != nil {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			haveLimit = true
			i += 2
		default:
			return syntaxErr(reply)
		}
	}
	if haveLimit && by == zrangeByIndex {
		return AppendError(reply, "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX")
	}
	lo, hi, errMsg, err := s.zrangeInterval(ctx, args[2], args[3], args[4], by, rev)
	if err != nil {
		return storeErr(reply, err)
	}
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	if haveLimit {
		lo, hi = zlimitFold(lo, hi, offset, count, rev)
	}
	n, err := s.z.ZRangeStore(ctx, args[1], args[2], lo, hi)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}
