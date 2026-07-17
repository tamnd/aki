package sqlo1

// The list deque surface: LPUSH, RPUSH, the X variants, LPOP and RPOP
// with counts, LMPOP, and the blocking forms over the same SRV
// machinery the zset pops built (doc 12: storage only sees a push or a
// pop). A push needs no signalling code of its own: every dispatch
// broadcasts the blocking condition on its way out, so the waiters
// re-check after any command that could have fed their keys.

import (
	"context"
	"math"
	"strings"
)

// lpushCmd is the four push forms. Elements land one at a time in
// argument order, so a multi-element left push reads back reversed,
// and the X variants leave a missing key missing and answer 0.
func (s *Server) lpushCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, left, xOnly bool) []byte {
	if len(args) < 3 {
		return arityErr(reply, cmd)
	}
	n, err := s.l.Push(ctx, args[1], left, xOnly, args[2:]...)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}

// lpopCmd is LPOP and RPOP, Redis's two-shape grammar: no count pops
// one and answers a bulk or nil, a count answers an array (empty for
// count 0 on a live key, null for a missing key), and a bad or
// negative count is the positive-range text.
func (s *Server) lpopCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, left bool) []byte {
	if len(args) < 2 || len(args) > 3 {
		return arityErr(reply, cmd)
	}
	if len(args) == 2 {
		vals, ok, err := s.l.Pop(ctx, args[1], left, 1)
		if err != nil {
			return storeErr(reply, err)
		}
		if !ok {
			return AppendNullBulk(reply)
		}
		return AppendBulk(reply, vals[0])
	}
	c, ok := parseCanonicalInt(args[2])
	if !ok || c < 0 {
		return AppendError(reply, "ERR value is out of range, must be positive")
	}
	vals, exists, err := s.l.Pop(ctx, args[1], left, int(c))
	if err != nil {
		return storeErr(reply, err)
	}
	if !exists {
		return AppendNullArray(reply)
	}
	reply = AppendArray(reply, len(vals))
	for _, v := range vals {
		reply = AppendBulk(reply, v)
	}
	return reply
}

// lmpopServe tries one LMPOP-shaped pop on key: served is false on a
// missing key with the reply untouched, and a served reply is the
// two-element array of the key and its popped elements. BLMPOP's
// blocked loop retries this same door. The parser guarantees count is
// positive, so a live key always serves at least one element.
func (s *Server) lmpopServe(ctx context.Context, reply, key []byte, left bool, count int64) ([]byte, bool, error) {
	vals, ok, err := s.l.Pop(ctx, key, left, int(count))
	if err != nil || !ok {
		return reply, false, err
	}
	reply = AppendArray(reply, 2)
	reply = AppendBulk(reply, key)
	reply = AppendArray(reply, len(vals))
	for _, v := range vals {
		reply = AppendBulk(reply, v)
	}
	return reply, true, nil
}

// lmpopCmd is LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]:
// the first non-empty key in listed order answers, and none answers
// the null array.
func (s *Server) lmpopCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "LMPOP")
	}
	keys, right, count, errMsg := parseMPopTail(args[1:], "LEFT", "RIGHT")
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	for _, k := range keys {
		out, served, err := s.lmpopServe(ctx, reply, k, !right, count)
		if err != nil {
			return storeErr(out, err)
		}
		if served {
			return out
		}
	}
	return AppendNullArray(reply)
}

// blpopCmd is BLPOP and BRPOP: key [key ...] timeout, the two-element
// key-element reply, and the null array on timeout.
func (s *Server) blpopCmd(ctx context.Context, reply []byte, args [][]byte, cmd string, left bool) []byte {
	if len(args) < 3 {
		return arityErr(reply, cmd)
	}
	timeout, errMsg := parseZTimeout(args[len(args)-1])
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	keys := args[1 : len(args)-1]
	return s.zblock(reply, keys, timeout, AppendNullArray, func(reply, key []byte) ([]byte, bool, error) {
		vals, ok, err := s.l.Pop(ctx, key, left, 1)
		if err != nil || !ok {
			return reply, false, err
		}
		reply = AppendArray(reply, 2)
		reply = AppendBulk(reply, key)
		return AppendBulk(reply, vals[0]), true, nil
	})
}

// lindexCmd is LINDEX key index: the element or the nil bulk for a
// missing key or an out-of-range index.
func (s *Server) lindexCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 3 {
		return arityErr(reply, "LINDEX")
	}
	idx, ok := parseCanonicalInt(args[2])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	e, found, err := s.l.Index(ctx, args[1], idx)
	if err != nil {
		return storeErr(reply, err)
	}
	if !found {
		return AppendNullBulk(reply)
	}
	return AppendBulk(reply, e)
}

// lsetCmd is LSET key index element: OK, or the layer's no-such-key
// and index-range errors through storeErr's ERR prefix.
func (s *Server) lsetCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 4 {
		return arityErr(reply, "LSET")
	}
	idx, ok := parseCanonicalInt(args[2])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if err := s.l.Set(ctx, args[1], idx, args[3]); err != nil {
		return storeErr(reply, err)
	}
	return AppendSimple(reply, "OK")
}

// lrangeCmd is LRANGE key start stop, streamed: the exact-count begin
// puts the array header down and the emits follow node by node. An
// error after the header truncates back to the mark, HGETALL's rule.
func (s *Server) lrangeCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 4 {
		return arityErr(reply, "LRANGE")
	}
	start, ok1 := parseCanonicalInt(args[2])
	stop, ok2 := parseCanonicalInt(args[3])
	if !ok1 || !ok2 {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	mark := len(reply)
	err := s.l.Range(ctx, args[1], start, stop, func(n int) {
		reply = AppendArray(reply, n)
	}, func(e []byte) {
		reply = AppendBulk(reply, e)
	})
	if err != nil {
		return storeErr(reply[:mark], err)
	}
	return reply
}

// ltrimCmd is LTRIM key start stop: always OK, a missing key
// included, and an empty window deletes the key.
func (s *Server) ltrimCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 4 {
		return arityErr(reply, "LTRIM")
	}
	start, ok1 := parseCanonicalInt(args[2])
	stop, ok2 := parseCanonicalInt(args[3])
	if !ok1 || !ok2 {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	if err := s.l.Trim(ctx, args[1], start, stop); err != nil {
		return storeErr(reply, err)
	}
	return AppendSimple(reply, "OK")
}

// linsertCmd is LINSERT key BEFORE|AFTER pivot element: the new
// length, -1 when the pivot is missing, 0 when the key is.
func (s *Server) linsertCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 5 {
		return arityErr(reply, "LINSERT")
	}
	var before bool
	switch {
	case strings.EqualFold(string(args[2]), "BEFORE"):
		before = true
	case strings.EqualFold(string(args[2]), "AFTER"):
	default:
		return AppendError(reply, "ERR syntax error")
	}
	n, err := s.l.Insert(ctx, args[1], before, args[3], args[4])
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}

// lremCmd is LREM key count element: the number removed, with the
// count's sign picking the scan direction and zero meaning all.
func (s *Server) lremCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) != 4 {
		return arityErr(reply, "LREM")
	}
	c, ok := parseCanonicalInt(args[2])
	if !ok {
		return AppendError(reply, "ERR value is not an integer or out of range")
	}
	n, err := s.l.Rem(ctx, args[1], c, args[3])
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}

// lposCmd is LPOS key element [RANK rank] [COUNT num] [MAXLEN len]:
// without COUNT the first surviving match's index or the nil bulk,
// with COUNT an array of up to num indexes (0 meaning all).
func (s *Server) lposCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return arityErr(reply, "LPOS")
	}
	rank, num, maxlen := int64(1), int64(1), int64(0)
	hasCount := false
	for i := 3; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return AppendError(reply, "ERR syntax error")
		}
		v, ok := parseCanonicalInt(args[i+1])
		switch {
		case strings.EqualFold(string(args[i]), "RANK"):
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if v == 0 {
				return AppendError(reply, "ERR RANK can't be zero. Use 1 to start searching from the first matching element in the head, or a negative rank to start searching backward from the tail.")
			}
			rank = v
		case strings.EqualFold(string(args[i]), "COUNT"):
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if v < 0 {
				return AppendError(reply, "ERR COUNT can't be negative")
			}
			hasCount = true
			num = v
			if v == 0 {
				num = math.MaxInt64
			}
		case strings.EqualFold(string(args[i]), "MAXLEN"):
			if !ok {
				return AppendError(reply, "ERR value is not an integer or out of range")
			}
			if v < 0 {
				return AppendError(reply, "ERR MAXLEN can't be negative")
			}
			maxlen = v
		default:
			return AppendError(reply, "ERR syntax error")
		}
	}
	if !hasCount {
		found, idx := false, int64(0)
		err := s.l.Pos(ctx, args[1], args[2], rank, 1, maxlen, func(i int64) {
			found, idx = true, i
		})
		if err != nil {
			return storeErr(reply, err)
		}
		if !found {
			return AppendNullBulk(reply)
		}
		return AppendInt(reply, idx)
	}
	var idxs []int64
	if err := s.l.Pos(ctx, args[1], args[2], rank, num, maxlen, func(i int64) {
		idxs = append(idxs, i)
	}); err != nil {
		return storeErr(reply, err)
	}
	reply = AppendArray(reply, len(idxs))
	for _, i := range idxs {
		reply = AppendInt(reply, i)
	}
	return reply
}

// blmpopCmd is BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT
// count]: LMPOP's reply behind the blocking loop.
func (s *Server) blmpopCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 5 {
		return arityErr(reply, "BLMPOP")
	}
	timeout, errMsg := parseZTimeout(args[1])
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	keys, right, count, errMsg := parseMPopTail(args[2:], "LEFT", "RIGHT")
	if errMsg != "" {
		return AppendError(reply, errMsg)
	}
	return s.zblock(reply, keys, timeout, AppendNullArray, func(reply, key []byte) ([]byte, bool, error) {
		return s.lmpopServe(ctx, reply, key, !right, count)
	})
}
