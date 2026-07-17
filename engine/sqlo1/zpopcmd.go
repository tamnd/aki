package sqlo1

// The zset pop family's command half: ZPOPMIN, ZPOPMAX, ZMPOP, and
// ZRANDMEMBER over the zpop.go primitives, plus the blocking forms.
// Blocking is the SRV layer's job (doc 12: storage only sees a pop),
// and here that layer is this server: a blocked client waits on a
// condition over the command mutex, every dispatch broadcasts on its
// way out, and per-key FIFO ticket queues decide who may take a key,
// so the longest-blocked client is served first, Redis's order. The
// timeout timer only broadcasts; the woken waiter itself chooses
// between serving, sleeping again, and the timeout reply.

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"
)

// zpopCmd is ZPOPMIN and ZPOPMAX: no count pops one, a count pops up
// to that many, and the reply is the flat member-score alternation in
// nearest-end-first order. The doors are Redis's: a non-integer and a
// negative count both answer out-of-range-must-be-positive, and any
// argument past the count is the family's own syntax error.
func (s *Server) zpopCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, maxSide bool) []byte {
	if len(args) < 2 {
		return arityErr(reply, cmd)
	}
	if len(args) > 3 {
		return AppendError(reply, "ERR syntax error, ZPOPMIN/ZPOPMAX only support a single count argument")
	}
	count := int64(1)
	if len(args) == 3 {
		l, ok := parseCanonicalInt(args[2])
		if !ok || l < 0 {
			return AppendError(reply, "ERR value is out of range, must be positive")
		}
		count = l
	}
	mark := len(reply)
	var sb [32]byte
	err := s.z.ZPopCount(ctx, args[1], count, maxSide, func(n int64) {
		reply = AppendArray(reply, int(n)*2)
	}, func(score float64, member []byte) {
		reply = AppendBulk(reply, member)
		reply = AppendBulk(reply, appendScore(sb[:0], score))
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}

// parseMPopTail parses the tail every MPOP form shares, starting at
// numkeys: numkeys, the keys, a direction token, and an optional
// COUNT. ZMPOP passes MIN and MAX, LMPOP passes LEFT and RIGHT, and hi
// reports the second token. A non-empty errMsg is the wire error,
// Redis's doors in Redis's order.
func parseMPopTail(args [][]byte, loTok, hiTok string) (keys [][]byte, hi bool, count int64, errMsg string) {
	nk, ok := parseCanonicalInt(args[0])
	if !ok || nk <= 0 {
		return nil, false, 0, "ERR numkeys should be greater than 0"
	}
	if int64(len(args)) < nk+2 {
		return nil, false, 0, "ERR syntax error"
	}
	keys = args[1 : 1+nk]
	switch strings.ToUpper(string(args[1+nk])) {
	case loTok:
	case hiTok:
		hi = true
	default:
		return nil, false, 0, "ERR syntax error"
	}
	count = 1
	rest := args[2+nk:]
	switch {
	case len(rest) == 0:
	case len(rest) == 2 && strings.EqualFold(string(rest[0]), "COUNT"):
		c, ok := parseCanonicalInt(rest[1])
		if !ok || c <= 0 {
			return nil, false, 0, "ERR count should be greater than 0"
		}
		count = c
	default:
		return nil, false, 0, "ERR syntax error"
	}
	return keys, hi, count, ""
}

// zmpopServe tries one ZMPOP-shaped pop on key: served is false on an
// empty or absent key with the reply untouched, and a served reply is
// the two-element array of the key and its popped pairs. BZMPOP's
// blocked loop retries this same door.
func (s *Server) zmpopServe(ctx context.Context, reply, key []byte, maxSide bool, count int64) ([]byte, bool, error) {
	mark := len(reply)
	card, err := s.z.ZCard(ctx, key)
	if err != nil || card == 0 {
		return reply, false, err
	}
	var sb [32]byte
	reply = AppendArray(reply, 2)
	reply = AppendBulk(reply, key)
	err = s.z.ZPopCount(ctx, key, count, maxSide, func(n int64) {
		reply = AppendArray(reply, int(n))
	}, func(score float64, member []byte) {
		reply = AppendArray(reply, 2)
		reply = AppendBulk(reply, member)
		reply = AppendBulk(reply, appendScore(sb[:0], score))
	})
	if err != nil {
		return reply[:mark], false, err
	}
	return reply, true, nil
}

// zmpopCmd is ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]: the
// first non-empty key in listed order answers, and none answers the
// null array.
func (s *Server) zmpopCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "ZMPOP")
	}
	keys, maxSide, count, errMsg := parseMPopTail(args[1:], "MIN", "MAX")
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	for _, k := range keys {
		out, served, err := s.zmpopServe(ctx, reply, k, maxSide, count)
		if err != nil {
			return storeErr(out, err)
		}
		if served {
			return out
		}
	}
	return AppendNullArray(reply)
}

// parseZTimeout parses a blocking timeout in seconds: a float, zero
// meaning forever. A non-empty errMsg is the wire error.
func parseZTimeout(arg []byte) (time.Duration, string) {
	f, err := strconv.ParseFloat(string(arg), 64)
	if err != nil || math.IsNaN(f) || f > float64(math.MaxInt64)/float64(time.Second) {
		return 0, "ERR timeout is not a float or out of range"
	}
	if f < 0 {
		return 0, "ERR timeout is negative"
	}
	return time.Duration(f * float64(time.Second)), ""
}

// zblock runs one blocking pop attempt loop. serve runs with the
// command mutex held and reports whether it replied; between attempts
// the client waits on the dispatch-exit broadcast. FIFO rides the
// per-key ticket queues: a client may take a key only while its
// ticket heads that key's queue, so a fresh arrival cannot jump a
// longer-blocked one, and a served or lapsed client broadcasts on the
// way out so the next head re-checks. The timer arms only after the
// ticket state is registered, and its callback touches nothing but
// the condition.
func (s *Server) zblock(reply []byte, keys [][]byte, timeout time.Duration, timeoutReply func([]byte) []byte, serve func(reply, key []byte) ([]byte, bool, error)) []byte {
	ticket := s.zbnext
	s.zbnext++
	uniq := make([]string, 0, len(keys))
	for _, k := range keys {
		ks := string(k)
		q := s.zbwait[ks]
		dup := false
		for _, t := range q {
			if t == ticket {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		uniq = append(uniq, ks)
		s.zbwait[ks] = append(q, ticket)
	}
	unregister := func() {
		for _, ks := range uniq {
			q := s.zbwait[ks]
			for i, t := range q {
				if t == ticket {
					q = append(q[:i], q[i+1:]...)
					break
				}
			}
			if len(q) == 0 {
				delete(s.zbwait, ks)
			} else {
				s.zbwait[ks] = q
			}
		}
		s.zbcond.Broadcast()
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		tm := time.AfterFunc(timeout, s.zbcond.Broadcast)
		defer tm.Stop()
	}
	for {
		for _, k := range keys {
			if q := s.zbwait[string(k)]; len(q) == 0 || q[0] != ticket {
				continue
			}
			out, served, err := serve(reply, k)
			if err != nil {
				unregister()
				return storeErr(out, err)
			}
			if served {
				unregister()
				return out
			}
		}
		if timeout > 0 && !time.Now().Before(deadline) {
			unregister()
			return timeoutReply(reply)
		}
		s.zbcond.Wait()
	}
}

// bzpopCmd is BZPOPMIN and BZPOPMAX: key [key ...] timeout, the
// three-element key-member-score reply, and the null array on
// timeout.
func (s *Server) bzpopCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, maxSide bool) []byte {
	if len(args) < 3 {
		return arityErr(reply, cmd)
	}
	timeout, errMsg := parseZTimeout(args[len(args)-1])
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	keys := args[1 : len(args)-1]
	return s.zblock(reply, keys, timeout, AppendNullArray, func(reply, key []byte) ([]byte, bool, error) {
		mark := len(reply)
		card, err := s.z.ZCard(ctx, key)
		if err != nil || card == 0 {
			return reply, false, err
		}
		var sb [32]byte
		reply = AppendArray(reply, 3)
		reply = AppendBulk(reply, key)
		err = s.z.ZPopCount(ctx, key, 1, maxSide, func(int64) {}, func(score float64, member []byte) {
			reply = AppendBulk(reply, member)
			reply = AppendBulk(reply, appendScore(sb[:0], score))
		})
		if err != nil {
			return reply[:mark], false, err
		}
		return reply, true, nil
	})
}

// bzmpopCmd is BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT
// count]: ZMPOP's reply behind the blocking loop.
func (s *Server) bzmpopCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 5 {
		return arityErr(reply, "BZMPOP")
	}
	timeout, errMsg := parseZTimeout(args[1])
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	keys, maxSide, count, errMsg := parseMPopTail(args[2:], "MIN", "MAX")
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	return s.zblock(reply, keys, timeout, AppendNullArray, func(reply, key []byte) ([]byte, bool, error) {
		return s.zmpopServe(ctx, reply, key, maxSide, count)
	})
}

// zrandCmd is ZRANDMEMBER key [count [WITHSCORES]], Redis's grammar:
// no count is one draw with a nil bulk on a missing key, a negative
// count draws with replacement, a positive one draws distinct capped
// at the cardinality, and WITHSCORES interleaves the scores.
func (s *Server) zrandCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return arityErr(reply, "ZRANDMEMBER")
	}
	if len(args) == 2 {
		served := false
		err := s.z.ZRandMemberCount(ctx, args[1], 1, true, func(int64) {}, func(_ float64, m []byte) {
			reply = AppendBulk(reply, m)
			served = true
		})
		if err != nil {
			return storeErr(reply, err)
		}
		if !served {
			return AppendNullBulk(reply)
		}
		return reply
	}
	if len(args) > 4 || (len(args) == 4 && !strings.EqualFold(string(args[3]), "WITHSCORES")) {
		return syntaxErr(reply)
	}
	withScores := len(args) == 4
	l, ok := parseCanonicalInt(args[2])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if l == math.MinInt64 {
		return AppendError(reply, "ERR value is out of range")
	}
	count, withReplacement := l, false
	if l < 0 {
		count, withReplacement = -l, true
	}
	vals := 1
	if withScores {
		vals = 2
	}
	mark := len(reply)
	var sb [32]byte
	err := s.z.ZRandMemberCount(ctx, args[1], count, withReplacement, func(n int64) {
		reply = AppendArray(reply, int(n)*vals)
	}, func(score float64, member []byte) {
		reply = AppendBulk(reply, member)
		if withScores {
			reply = AppendBulk(reply, appendScore(sb[:0], score))
		}
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}
