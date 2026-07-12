package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XREAD, the non-blocking forward read (spec 2064/f3/14 section 6.3). Each named
// stream carries an after-ID, an exclusive lower bound, and the read returns the
// live entries above it, oldest first, capped by COUNT. This is XRANGE with an
// open lower bound and an unbounded top, so it rides the same directory seek and
// contiguous decode: a recent after-ID lands in the tail block without a
// directory descent (section 3.5), a deep one descends once.
//
// The special IDs resolve as Redis defines them: "$" is the stream's current
// lastID, so a non-blocking read above it returns nothing (its purpose is the
// blocking form, a later slice); "+" (the Redis 7.4 form) is the last live
// entry. A stream that does not exist contributes nothing, and BLOCK is refused
// here rather than silently ignored until the blocking slice wires the waiter
// sets.

// Xread answers XREAD [COUNT n] STREAMS key [key ...] id [id ...]: for each key,
// the live entries after its ID. The reply is an array of [key, entries] pairs,
// one per stream that produced entries, or a null array when none did (the
// non-blocking miss). Streams with no new entries are omitted, matching Redis.
func Xread(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	count, i, ok := parseReadCount(args)
	if !ok {
		r.Err("ERR value is not an integer or out of range")
		return
	}
	if i < len(args) && eqFold(args[i], "BLOCK") {
		r.Err("ERR stream blocking is not supported yet")
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
	results := make([]readResult, 0, nk)
	for j := 0; j < nk; j++ {
		s, wrong := g.lookup(cx, keys[j])
		if wrong {
			r.Err(wrongType)
			return
		}
		entries, ok := readOne(s, ids[j], count)
		if !ok {
			r.Err(errInvalidID)
			return
		}
		if len(entries) > 0 {
			results = append(results, readResult{key: keys[j], entries: entries})
		}
	}

	out := cx.Aux[:0]
	if len(results) == 0 {
		out = resp.AppendNullArray(out)
		cx.Aux = out
		r.Raw(out)
		return
	}
	out = resp.AppendArrayHeader(out, len(results))
	for _, rr := range results {
		out = resp.AppendArrayHeader(out, 2)
		out = resp.AppendBulk(out, rr.key)
		out = resp.AppendArrayHeader(out, len(rr.entries))
		for k := range rr.entries {
			out = appendEntryReply(out, rr.entries[k].id, rr.entries[k].fields)
		}
	}
	cx.Aux = out
	r.Raw(out)
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

// parseReadCount reads the optional leading COUNT clause and returns the entry
// cap (-1 for unbounded, including the Redis COUNT 0 which means no limit) and
// the index of the first argument past it.
func parseReadCount(args [][]byte) (count, next int, ok bool) {
	if len(args) >= 1 && eqFold(args[0], "COUNT") {
		if len(args) < 2 {
			return 0, 0, false
		}
		n, nok := parseUint(args[1])
		if !nok {
			return 0, 0, false
		}
		c := int(n)
		if c == 0 {
			c = -1 // XREAD COUNT 0 is unbounded, unlike XRANGE
		}
		return c, 2, true
	}
	return -1, 0, true
}

// readOne resolves one stream's ID argument and gathers its entries. ok is false
// only on a malformed explicit ID; a missing stream (s nil) or a "$"/"+" against
// one contributes no entries without erroring.
func readOne(s *stream, idArg []byte, count int) (entries []rangeEntry, ok bool) {
	if len(idArg) == 1 {
		switch idArg[0] {
		case '$':
			// Entries after the current tail: nothing without blocking.
			if s != nil {
				return s.readAfter(s.lastID, count), true
			}
			return nil, true
		case '+':
			if s != nil {
				return s.lastEntry(), true
			}
			return nil, true
		}
	}
	id, idok := parseStreamID(idArg)
	if !idok {
		return nil, false
	}
	if s == nil {
		return nil, true
	}
	return s.readAfter(id, count), true
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
