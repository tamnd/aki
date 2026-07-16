package stream

import (
	"strconv"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// XREAD, the forward read (spec 2064/f3/14 sections 6.3 and 6.4). Each named
// stream carries an after-ID, an exclusive lower bound, and the read returns the
// live entries above it, oldest first, capped by COUNT. This is XRANGE with an
// open lower bound and an unbounded top, so it rides the same directory seek and
// contiguous decode: a recent after-ID lands in the tail block without a
// directory descent (section 3.5), a deep one descends once.
//
// The special IDs resolve as Redis defines them: "$" is the stream's current
// lastID, so a read above it returns nothing until a later XADD; "+" (the Redis
// 7.4 form) is the last live entry. A stream that does not exist contributes
// nothing. With BLOCK the command parks when no stream yields an entry now: the
// waiter records each stream's resolved after-ID and a later XADD, or the timeout,
// completes it (section 6.4).

// Xread answers XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]: for
// each key, the live entries after its ID. The reply is an array of [key, entries]
// pairs, one per stream that produced entries, or a null array when none did (the
// non-blocking miss, or a BLOCK timeout). Streams with no new entries are omitted,
// matching Redis.
func Xread(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	opts, i, msg := parseReadOpts(args)
	if msg != "" {
		r.Err(msg)
		return
	}
	if i >= len(args) || !eqFold(args[i], "STREAMS") {
		r.Err("ERR syntax error")
		return
	}
	i++
	rest := args[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		r.Err("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.")
		return
	}
	nk := len(rest) / 2
	keys, ids := rest[:nk], rest[nk:]

	g := registry(cx)
	// Collect every stream's entries first, so the outer array header can carry
	// the count of non-empty streams; the field views stay valid because no
	// mutation runs between the collection and the emit on this owner goroutine.
	// afters holds each stream's resolved lower bound, kept for a possible park so
	// a woken read repeats exactly this scan.
	results := make([]readResult, 0, nk)
	afters := make([]streamID, nk)
	for j := 0; j < nk; j++ {
		s, wrong := g.lookup(cx, keys[j])
		if wrong {
			r.Err(wrongType)
			return
		}
		after, ok := readAfterID(s, ids[j])
		if !ok {
			r.Err(errInvalidID)
			return
		}
		afters[j] = after
		if entries := immediateRead(s, ids[j], after, opts.count); len(entries) > 0 {
			results = append(results, readResult{key: keys[j], entries: entries})
		}
	}

	if len(results) == 0 && opts.block {
		parkRead(cx, g, keys, afters, opts)
		r.Park()
		return
	}

	out := frameReadResults(cx.Aux[:0], results)
	cx.Aux = out
	r.Raw(out)
}

// frameReadResults appends the XREAD reply for the streams that produced entries:
// the array of [key, entries] pairs, or the RESP2 null array when none did. Both
// the immediate reply and a woken park share it, so the two paths never drift.
func frameReadResults(dst []byte, results []readResult) []byte {
	if len(results) == 0 {
		return resp.AppendNullArray(dst)
	}
	dst = resp.AppendArrayHeader(dst, len(results))
	for _, rr := range results {
		dst = resp.AppendArrayHeader(dst, 2)
		dst = resp.AppendBulk(dst, rr.key)
		dst = resp.AppendArrayHeader(dst, len(rr.entries))
		for k := range rr.entries {
			dst = appendEntryReply(dst, rr.entries[k].id, rr.entries[k].fields)
		}
	}
	return dst
}

// parkRead blocks the connection on every named stream. It clones the keys and
// their resolved after-IDs into one shared request, parks a node per key, and arms
// the timeout on the sibling-ring head for a finite BLOCK; BLOCK 0 blocks forever
// and arms nothing. A later XADD on any key, or the timer, completes the deferred
// reply through the waiter set.
func parkRead(cx *shard.Ctx, g *reg, keys [][]byte, afters []streamID, opts readOpts) {
	ck := make([][]byte, len(keys))
	for j := range keys {
		ck[j] = append([]byte(nil), keys[j]...)
	}
	ca := append([]streamID(nil), afters...)
	req := &xreadWait{keys: ck, afters: ca, count: opts.count}
	head := parkWaiter(g, req, cx.CurConn(), cx.CurSeq())
	if opts.blockMs > 0 {
		deadline := cx.NowMs + opts.blockMs
		g.wpool.nodes[head].timer = cx.ArmTimer(deadline, makeReadFire(g, head))
	}
}

// makeReadFire builds the timeout callback for the blocked read whose ring head is
// head. It runs on the owner when the deadline passes with no serving XADD. The
// live guard makes it idempotent against a serve that already tore the waiter down.
// A timed-out XREAD replies the RESP2 null array (*-1), the shape Redis sends.
func makeReadFire(g *reg, head uint32) func(*shard.Ctx) {
	return func(cx *shard.Ctx) {
		nd := &g.wpool.nodes[head]
		if !nd.live {
			return
		}
		conn := nd.conn
		seq := nd.seq
		nd.timer = nil // the firing timer is off the heap already
		g.unlinkAll(cx, head)
		conn.CompleteBlocked(seq, resp.AppendNullArray(nil))
	}
}

// readResult pairs a stream's key with the entries a read produced, held until
// the reply is framed.
type readResult struct {
	key     []byte
	entries []rangeEntry
}

// ReadKeys extracts an XREAD tail's stream keys (the argument run after the verb)
// for the dispatcher's co-location check, returning nil on a malformed tail so
// the handler answers the exact error in place. ReadKeyAt returns the index
// within the tail of the first stream key, the single-shard routing key, or -1.
// Both skip the optional COUNT and BLOCK clauses to find the STREAMS token.
func ReadKeys(tail [][]byte) [][]byte {
	at, ok := streamsAt(tail)
	if !ok {
		return nil
	}
	rest := tail[at:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil
	}
	return rest[:len(rest)/2]
}

// ReadKeyAt returns the tail index of the first stream key, or -1 on a malformed
// tail.
func ReadKeyAt(tail [][]byte) int {
	at, ok := streamsAt(tail)
	if !ok {
		return -1
	}
	rest := tail[at:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return -1
	}
	return at
}

// streamsAt returns the index just past the STREAMS token, skipping the two-token
// COUNT and BLOCK clauses. ok is false when no well-formed STREAMS token is
// found.
func streamsAt(tail [][]byte) (int, bool) {
	for i := 0; i < len(tail); {
		switch {
		case (eqFold(tail[i], "COUNT") || eqFold(tail[i], "BLOCK")) && i+1 < len(tail):
			i += 2
		case eqFold(tail[i], "STREAMS"):
			return i + 1, true
		default:
			return 0, false
		}
	}
	return 0, false
}

// readOpts is the parsed XREAD option prefix: the COUNT cap (-1 unbounded, the
// Redis COUNT 0 meaning) and the BLOCK clause (block set, blockMs the timeout in
// milliseconds, 0 for an unbounded wait).
type readOpts struct {
	count   int
	block   bool
	blockMs int64
}

// parseReadOpts reads the COUNT and BLOCK clauses that precede STREAMS, in either
// order, and returns the options, the index of the first argument past them, and a
// Redis error text (empty on success). It stops at the first token that is neither
// clause, which the caller checks is STREAMS.
func parseReadOpts(args [][]byte) (opts readOpts, next int, msg string) {
	opts.count = -1
	i := 0
	for i < len(args) {
		switch {
		case eqFold(args[i], "COUNT"):
			if i+1 >= len(args) {
				return opts, i, "ERR syntax error"
			}
			n, ok := parseUint(args[i+1])
			if !ok {
				return opts, i, "ERR value is not an integer or out of range"
			}
			opts.count = int(n)
			if opts.count == 0 {
				opts.count = -1 // XREAD COUNT 0 is unbounded, unlike XRANGE
			}
			i += 2
		case eqFold(args[i], "BLOCK"):
			if i+1 >= len(args) {
				return opts, i, "ERR syntax error"
			}
			ms, ok := parseBlockMs(args[i+1])
			if !ok {
				return opts, i, "ERR timeout is not an integer or out of range"
			}
			if ms < 0 {
				return opts, i, "ERR timeout is negative"
			}
			opts.block = true
			opts.blockMs = ms
			i += 2
		default:
			return opts, i, ""
		}
	}
	return opts, i, ""
}

// parseBlockMs parses a BLOCK timeout as a signed base-10 integer of milliseconds,
// the grammar Redis's getTimeoutFromObject accepts for the integer form. A
// negative value parses (the caller reports it separately); a non-integer does
// not.
func parseBlockMs(b []byte) (int64, bool) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// readAfterID resolves one stream's XREAD id argument to the exclusive lower bound
// a read scans above. "$" and "+" resolve to the stream's current last ID (0-0 for
// a missing or empty stream), so a read waits for the next XADD; an explicit ID
// resolves to itself. ok is false only on a malformed explicit ID.
func readAfterID(s *stream, idArg []byte) (after streamID, ok bool) {
	if len(idArg) == 1 && (idArg[0] == '$' || idArg[0] == '+') {
		if s != nil {
			return s.lastID, true
		}
		return streamID{}, true
	}
	id, idok := parseStreamID(idArg)
	if !idok {
		return streamID{}, false
	}
	return id, true
}

// immediateRead gathers a stream's entries for the non-blocking answer. The "+"
// form returns the single last live entry; every other form returns the live
// entries above the resolved after-ID, capped by count. A missing stream yields
// nothing.
func immediateRead(s *stream, idArg []byte, after streamID, count int) []rangeEntry {
	if s == nil {
		return nil
	}
	if len(idArg) == 1 && idArg[0] == '+' {
		return s.lastEntry()
	}
	return s.readAfter(after, count)
}

// readAfter returns up to count live entries with IDs strictly above afterID,
// oldest first (count -1 is unbounded). It is XRANGE over the open window
// (afterID, +inf].
func (s *stream) readAfter(afterID streamID, count int) []rangeEntry {
	lo := bound{id: afterID, excl: true}
	hi := bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}}
	return s.collectRange(lo, hi, false, count)
}

// lastEntry returns the single greatest live entry (the XREAD "+" form), or none
// when every entry is tombstoned or the stream is empty.
func (s *stream) lastEntry() []rangeEntry {
	lo := bound{id: streamID{ms: 0, seq: 0}}
	hi := bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}}
	return s.collectRange(lo, hi, true, 1)
}
