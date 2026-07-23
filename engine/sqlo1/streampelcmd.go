package sqlo1

// XREADGROUP and XACK, the PEL slice's command surface. The option
// scan, the per-key error order, and every wire shape here are pinned
// against Redis 8.8: keys resolve one at a time with the type check
// outranking NOGROUP outranking that key's ID parse, the > form omits
// empty streams while the history form always echoes its key, and the
// whole reply is nil only when every stream was a > with nothing new.

import (
	"context"
	"strings"
)

const (
	errXreadgroupMissing    = "ERR Missing GROUP option for XREADGROUP"
	errXreadgroupUnbalanced = "ERR Unbalanced 'xreadgroup' list of streams: for each stream key an ID or '>' must be specified."
	errXreadgroupTimeout    = "ERR timeout is not an integer or out of range"
	errXreadgroupNegTimeout = "ERR timeout is negative"
	errXreadgroupBlock      = "ERR XREADGROUP BLOCK is not supported until the blocking slice"
)

// xreadgroupNoGroupErr is the NOGROUP text XREADGROUP shares between a
// missing key and a missing group, unlike XGROUP's key-specific split.
func xreadgroupNoGroupErr(reply []byte, key, group []byte) []byte {
	return AppendError(reply, "NOGROUP No such key '"+string(key)+"' or consumer group '"+string(group)+"' in XREADGROUP with GROUP option")
}

// meaninglessIDErr is the $ and + special case: both parse as stream
// IDs elsewhere, so XREADGROUP names the token in its own error.
func meaninglessIDErr(reply []byte, tok byte) []byte {
	return AppendError(reply, "ERR The "+string(tok)+" ID is meaningless in the context of XREADGROUP: you want to read the history of this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID would just return an empty result set.")
}

// xreadgroupCmd is XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK]
// STREAMS key... id.... Options scan in any order before STREAMS,
// which claims the whole tail; BLOCK validates its integer and then
// refuses until the blocking slice, the temporary-refusal precedent.
func (s *Server) xreadgroupCmd(ctx context.Context, reply []byte, args [][]byte, now int64) []byte {
	if len(args) < 7 {
		return arityErr(reply, "XREADGROUP")
	}
	var group, consumer []byte
	haveGroup, haveBlock, noack := false, false, false
	count := int64(-1)
	streamsAt := -1
	for i := 1; i < len(args); {
		switch {
		case strings.EqualFold(string(args[i]), "GROUP") && i+2 < len(args):
			group, consumer = args[i+1], args[i+2]
			haveGroup = true
			i += 3
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errNotInteger)
			}
			// COUNT 0 and negative counts both read as unlimited, the
			// pinned 8.8 shape.
			count = n
			if count <= 0 {
				count = -1
			}
			i += 2
		case strings.EqualFold(string(args[i]), "BLOCK") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errXreadgroupTimeout)
			}
			if n < 0 {
				return AppendError(reply, errXreadgroupNegTimeout)
			}
			haveBlock = true
			i += 2
		case strings.EqualFold(string(args[i]), "NOACK"):
			noack = true
			i++
		case strings.EqualFold(string(args[i]), "STREAMS"):
			streamsAt = i + 1
			i = len(args)
		default:
			return syntaxErr(reply)
		}
	}
	if !haveGroup {
		return AppendError(reply, errXreadgroupMissing)
	}
	if streamsAt < 0 {
		return syntaxErr(reply)
	}
	rest := args[streamsAt:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return AppendError(reply, errXreadgroupUnbalanced)
	}
	if haveBlock {
		return AppendError(reply, errXreadgroupBlock)
	}
	nk := len(rest) / 2
	keys, idArgs := rest[:nk], rest[nk:]

	// Keys resolve one at a time: this key's type check, then its
	// group, then its ID, before the next key is looked at, and no
	// stream serves until every key passed.
	news := make([]bool, nk)
	afters := make([]streamID, nk)
	for i := range nk {
		if err := s.x.ReadGroupCheck(ctx, keys[i], group); err != nil {
			if err == errStreamNoGroup {
				return xreadgroupNoGroupErr(reply, keys[i], group)
			}
			return storeErr(reply, err)
		}
		a := idArgs[i]
		if len(a) == 1 && a[0] == '>' {
			news[i] = true
			continue
		}
		if len(a) == 1 && (a[0] == '$' || a[0] == '+') {
			return meaninglessIDErr(reply, a[0])
		}
		mode, id, ok := parseStreamXaddID(a)
		if !ok || mode != xidExplicit {
			return AppendError(reply, errInvalidStreamID)
		}
		afters[i] = id
	}

	// Rows render into scratch: a > stream with nothing new drops its
	// row, so the outer array's size is only known at the end.
	var rows []byte
	nrows := 0
	for i := range nk {
		mark := len(rows)
		rows = AppendArray(rows, 2)
		rows = AppendBulk(rows, keys[i])
		if news[i] {
			n := 0
			err := s.x.ReadGroupNew(ctx, keys[i], group, consumer, count, noack, now, func(k int) {
				n = k
				rows = AppendArray(rows, k)
			}, func(id streamID, fv [][]byte) {
				rows = appendStreamEntry(rows, id, fv)
			})
			if err != nil {
				return storeErr(reply, err)
			}
			if n == 0 {
				rows = rows[:mark]
				continue
			}
			nrows++
			continue
		}
		err := s.x.ReadGroupHistory(ctx, keys[i], group, consumer, afters[i], count, now, func(k int) {
			rows = AppendArray(rows, k)
		}, func(id streamID, fv [][]byte, missing bool) {
			if missing {
				// A pending ID whose entry was deleted or trimmed
				// echoes as [id, nil], the pinned history row.
				rows = AppendArray(rows, 2)
				rows = appendStreamIDBulk(rows, id)
				rows = AppendNullArray(rows)
				return
			}
			rows = appendStreamEntry(rows, id, fv)
		})
		if err != nil {
			return storeErr(reply, err)
		}
		nrows++
	}
	if nrows == 0 {
		return AppendNullArray(reply)
	}
	reply = AppendArray(reply, nrows)
	return append(reply, rows...)
}

// xackCmd is XACK key group id...: the key's type check outranks the
// ID parse, which outranks the zero replies a missing key or group
// answers, the pinned 8.8 order. Duplicate IDs count once.
func (s *Server) xackCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "XACK")
	}
	exists, err := s.x.AckPrecheck(ctx, args[1])
	if err != nil {
		return storeErr(reply, err)
	}
	ids := make([]streamID, 0, len(args)-3)
	for _, a := range args[3:] {
		mode, id, ok := parseStreamXaddID(a)
		if !ok || mode != xidExplicit {
			return AppendError(reply, errInvalidStreamID)
		}
		ids = append(ids, id)
	}
	if !exists {
		return AppendInt(reply, 0)
	}
	n, err := s.x.Ack(ctx, args[1], args[2], ids)
	if err != nil {
		return storeErr(reply, err)
	}
	return AppendInt(reply, n)
}
