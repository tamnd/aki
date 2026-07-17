package sqlo1

// The zset algebra family's wire half. The doors below are Redis
// 8.8.0's, probed live rather than recalled, and three differ from
// the set family's SINTERCARD: numkeys that does not parse answers
// the integer error (not greater-than-0), numkeys below one answers
// the at-least-1 text with the command's own lowercase name, and
// numkeys past the argument count is a plain syntax error, ZINTERCARD
// included.

import (
	"context"
	"math"
	"strconv"
	"strings"
)

// zsetopCmd serves ZUNION, ZINTER, ZUNIONSTORE, and ZINTERSTORE, the
// four commands sharing the numkeys grammar and the WEIGHTS,
// AGGREGATE, WITHSCORES option loop. Redis's loop is order-free and
// last-wins, WEIGHTS demands numkeys values in place, and WITHSCORES
// exists only on the read forms; any token that fits no branch is a
// syntax error.
func (s *Server) zsetopCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, inter, store bool) []byte {
	minArgs, nkIdx := 3, 1
	if store {
		minArgs, nkIdx = 4, 2
	}
	if len(args) < minArgs {
		return arityErr(reply, cmd)
	}
	nk64, ok := parseCanonicalInt(args[nkIdx])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if nk64 < 1 {
		return AppendError(reply, "ERR at least 1 input key is needed for '"+strings.ToLower(cmd)+"' command")
	}
	if nk64 > int64(len(args)-nkIdx-1) {
		return syntaxErr(reply)
	}
	nk := int(nk64)
	keys := args[nkIdx+1 : nkIdx+1+nk]
	var weights []float64
	agg := zaggSum
	withscores := false
	for j := nkIdx + 1 + nk; j < len(args); {
		remaining := len(args) - j
		tok := string(args[j])
		switch {
		case remaining >= nk+1 && strings.EqualFold(tok, "WEIGHTS"):
			j++
			if weights == nil {
				weights = make([]float64, nk)
			}
			for i := range nk {
				w, err := strconv.ParseFloat(string(args[j]), 64)
				if err != nil || math.IsNaN(w) {
					return AppendError(reply, "ERR weight value is not a float")
				}
				weights[i] = w
				j++
			}
		case remaining >= 2 && strings.EqualFold(tok, "AGGREGATE"):
			switch strings.ToUpper(string(args[j+1])) {
			case "SUM":
				agg = zaggSum
			case "MIN":
				agg = zaggMin
			case "MAX":
				agg = zaggMax
			default:
				return syntaxErr(reply)
			}
			j += 2
		case !store && strings.EqualFold(tok, "WITHSCORES"):
			withscores = true
			j++
		default:
			return syntaxErr(reply)
		}
	}
	if store {
		var n int64
		var err error
		if inter {
			n, err = s.z.ZInterStore(ctx, args[1], keys, weights, agg)
		} else {
			n, err = s.z.ZUnionStore(ctx, args[1], keys, weights, agg)
		}
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	}
	var pairs []zbuildPair
	var arena []byte
	var err error
	if inter {
		pairs, arena, err = s.z.ZInter(ctx, keys, weights, agg)
	} else {
		pairs, arena, err = s.z.ZUnion(ctx, keys, weights, agg)
	}
	if err != nil {
		return storeErr(reply, err)
	}
	return zpairsReply(reply, pairs, arena, withscores)
}

// zdiffCmd serves ZDIFF and ZDIFFSTORE: the same numkeys doors, but
// the only legal tail is a lone WITHSCORES on the read form.
func (s *Server) zdiffCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, store bool) []byte {
	minArgs, nkIdx := 3, 1
	if store {
		minArgs, nkIdx = 4, 2
	}
	if len(args) < minArgs {
		return arityErr(reply, cmd)
	}
	nk64, ok := parseCanonicalInt(args[nkIdx])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if nk64 < 1 {
		return AppendError(reply, "ERR at least 1 input key is needed for '"+strings.ToLower(cmd)+"' command")
	}
	if nk64 > int64(len(args)-nkIdx-1) {
		return syntaxErr(reply)
	}
	nk := int(nk64)
	keys := args[nkIdx+1 : nkIdx+1+nk]
	tail := args[nkIdx+1+nk:]
	withscores := false
	switch {
	case len(tail) == 0:
	case !store && len(tail) == 1 && strings.EqualFold(string(tail[0]), "WITHSCORES"):
		withscores = true
	default:
		return syntaxErr(reply)
	}
	if store {
		n, err := s.z.ZDiffStore(ctx, args[1], keys)
		if err != nil {
			return storeErr(reply, err)
		}
		return AppendInt(reply, n)
	}
	pairs, arena, err := s.z.ZDiff(ctx, keys)
	if err != nil {
		return storeErr(reply, err)
	}
	return zpairsReply(reply, pairs, arena, withscores)
}

// zintercardCmd is ZINTERCARD numkeys key [key ...] [LIMIT limit]:
// the family's numkeys doors, then a tail that is empty or exactly
// LIMIT n with n >= 0 (a limit that does not parse answers the
// negative-limit text, Redis's door), and LIMIT 0 means unlimited.
func (s *Server) zintercardCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "ZINTERCARD")
	}
	nk, ok := parseCanonicalInt(args[1])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if nk < 1 {
		return AppendError(reply, "ERR at least 1 input key is needed for 'zintercard' command")
	}
	if nk > int64(len(args)-2) {
		return syntaxErr(reply)
	}
	limit := int64(0)
	switch {
	case int64(len(args)) == nk+2:
	case int64(len(args)) == nk+4 && strings.EqualFold(string(args[nk+2]), "LIMIT"):
		l, ok := parseCanonicalInt(args[nk+3])
		if !ok || l < 0 {
			return AppendError(reply, "ERR LIMIT can't be negative")
		}
		limit = l
	default:
		return syntaxErr(reply)
	}
	card, err := s.z.ZInterCard(ctx, args[2:2+nk], limit)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, card)
}

// zpairsReply emits an algebra result: members in (score, member)
// order, scores riding along under WITHSCORES through the shared
// formatter.
func zpairsReply(reply []byte, pairs []zbuildPair, arena []byte, withscores bool) []byte {
	n := len(pairs)
	if withscores {
		reply = AppendArray(reply, n*2)
	} else {
		reply = AppendArray(reply, n)
	}
	var sb [32]byte
	for _, p := range pairs {
		reply = AppendBulk(reply, arena[p.off:p.end])
		if withscores {
			reply = AppendBulk(reply, appendScore(sb[:0], zScoreFromSortable(p.s)))
		}
	}
	return reply
}
