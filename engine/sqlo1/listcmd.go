package sqlo1

// The list deque surface: LPUSH, RPUSH, the X variants, LPOP and RPOP
// with counts, LMPOP, and the blocking forms over the same SRV
// machinery the zset pops built (doc 12: storage only sees a push or a
// pop). A push needs no signalling code of its own: every dispatch
// broadcasts the blocking condition on its way out, so the waiters
// re-check after any command that could have fed their keys.

import "context"

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
