package sqlo1

// XREAD and the stream blocking loop, pinned against Redis 8.8. XREAD
// resolves each key left to right with the type check before that
// key's ID parse, freezes $ to the last generated ID and rewinds +
// behind the newest live entry, and omits empty streams from the
// reply. The blocking loop is zblock's shape over the same ticket
// queues and dispatch-exit broadcast, generalized to serve several
// keys into one reply; a special ID stays frozen at its registration
// reading, so a stream deleted and refilled below it cannot wake the
// reader, the pinned once-resolved rule.

import (
	"context"
	"math"
	"slices"
	"strings"
	"time"
)

const errXreadUnbalanced = "ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified."

// xreadCmd is XREAD [COUNT n] [BLOCK ms] STREAMS key... id.... A
// missing STREAMS and an empty tail are both the bare syntax error,
// while an odd tail is XREAD's own unbalanced text naming '+' and '$'.
func (s *Server) xreadCmd(ctx context.Context, reply []byte, args [][]byte) []byte {
	if len(args) < 4 {
		return arityErr(reply, "XREAD")
	}
	count := int64(-1)
	haveBlock := false
	blockMs := int64(0)
	streamsAt := -1
	for i := 1; i < len(args); {
		switch {
		case strings.EqualFold(string(args[i]), "COUNT") && i+1 < len(args):
			n, ok := parseCanonicalInt(args[i+1])
			if !ok {
				return AppendError(reply, errNotInteger)
			}
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
			haveBlock, blockMs = true, n
			i += 2
		case strings.EqualFold(string(args[i]), "STREAMS"):
			streamsAt = i + 1
			i = len(args)
		default:
			return syntaxErr(reply)
		}
	}
	if streamsAt < 0 {
		return syntaxErr(reply)
	}
	rest := args[streamsAt:]
	if len(rest) == 0 {
		return syntaxErr(reply)
	}
	if len(rest)%2 != 0 {
		return AppendError(reply, errXreadUnbalanced)
	}
	nk := len(rest) / 2
	keys, idArgs := rest[:nk], rest[nk:]

	// Every ID resolves to a frozen exclusive-start before any stream
	// serves: $ is the last generated ID, + rewinds one step behind the
	// newest live entry so that entry itself serves, and + on an empty
	// or missing stream falls back to the generated ID like $.
	afters := make([]streamID, nk)
	for i := range nk {
		_, lastGen, lastEntry, hasEntry, err := s.x.ReadPrep(ctx, keys[i])
		if err != nil {
			return storeErr(reply, err)
		}
		a := idArgs[i]
		if len(a) == 1 && a[0] == '$' {
			afters[i] = lastGen
			continue
		}
		if len(a) == 1 && a[0] == '+' {
			if hasEntry {
				prev, _ := streamIDPrev(lastEntry)
				afters[i] = prev
			} else {
				afters[i] = lastGen
			}
			continue
		}
		mode, id, ok := parseStreamXaddID(a)
		if !ok || mode != xidExplicit {
			return AppendError(reply, errInvalidStreamID)
		}
		afters[i] = id
	}

	serve := func(reply []byte, elig []int) ([]byte, bool, error) {
		var rows []byte
		nrows := 0
		for _, i := range elig {
			start, ok := streamIDNext(afters[i])
			if !ok {
				continue
			}
			full := streamID{ms: math.MaxUint64, seq: math.MaxUint64}
			mark := len(rows)
			rows = AppendArray(rows, 2)
			rows = AppendBulk(rows, keys[i])
			n := 0
			err := s.x.Range(ctx, keys[i], start, full, count, false, func(k int) {
				n = k
				rows = AppendArray(rows, k)
			}, func(id streamID, fv [][]byte) {
				rows = appendStreamEntry(rows, id, fv)
			})
			if err != nil {
				return reply, false, err
			}
			if n == 0 {
				rows = rows[:mark]
				continue
			}
			nrows++
		}
		if nrows == 0 {
			return reply, false, nil
		}
		out := AppendArray(reply, nrows)
		return append(out, rows...), true, nil
	}

	all := make([]int, nk)
	for i := range all {
		all[i] = i
	}
	out, served, err := serve(reply, all)
	if err != nil {
		return storeErr(reply, err)
	}
	if served {
		return out
	}
	if !haveBlock {
		return AppendNullArray(reply)
	}
	return s.xblock(reply, keys, time.Duration(blockMs)*time.Millisecond, serve)
}

// xblock is the stream side of the blocking machinery: zblock's ticket
// queues, deadline, and dispatch-exit broadcast, with one difference,
// serve takes every key the ticket currently heads so one wake can
// answer several streams in a single reply. Streams never consume on
// read, so a served head unregisters, broadcasts, and the next ticket
// serves the same data; the FIFO order only matters to XREADGROUP,
// where a delivery is consumed into the PEL. The timeout reply is the
// null array both commands share.
func (s *Server) xblock(reply []byte, keys [][]byte, timeout time.Duration, serve func(reply []byte, elig []int) ([]byte, bool, error)) []byte {
	ticket := s.zbnext
	s.zbnext++
	uniq := make([]string, 0, len(keys))
	for _, k := range keys {
		ks := string(k)
		q := s.zbwait[ks]
		if slices.Contains(q, ticket) {
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
	elig := make([]int, 0, len(keys))
	for {
		elig = elig[:0]
		for i, k := range keys {
			if q := s.zbwait[string(k)]; len(q) > 0 && q[0] == ticket {
				elig = append(elig, i)
			}
		}
		if len(elig) > 0 {
			out, served, err := serve(reply, elig)
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
			return AppendNullArray(reply)
		}
		s.zbcond.Wait()
	}
}
