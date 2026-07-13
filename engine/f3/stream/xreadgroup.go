package stream

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XREADGROUP, the consumer-group delivery (spec 2064/f3/14 section 7.5). It reads
// on behalf of a named consumer in a group and, unlike XREAD, records what it
// hands out: the `>` form walks the entries above the group's shared cursor,
// delivers them, and (unless NOACK) inserts one pending entry per delivered ID
// into the group PEL, so a later XACK can retire them and a crashed consumer can
// re-read its own unacked history. The explicit-ID form (`STREAMS s 0`) is that
// recovery read: it walks this consumer's PEL from the given ID, joining each
// pending ID to its live log entry, and returns `[id, nil]` where an XDEL has
// since removed the entry the PEL still tracks.
//
// This slice serves the non-blocking forms. XREADGROUP BLOCK parks a `>` reader
// the way XREAD does, but the wake path must deliver into the PEL on serve, so it
// lands as its own sub-slice; a BLOCK given here parses and, when the read is
// empty, replies the null array immediately rather than parking.

// Xreadgroup answers XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS
// key [key ...] id [id ...]. The reply mirrors XREAD: an array of [key, entries]
// pairs. A `>` stream with no new entries is omitted; an explicit-ID stream is
// always present (an empty entry list is the "your history is drained" answer),
// so the null array appears only when every stream read `>` and none advanced.
func Xreadgroup(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	grpName, conName, opts, keys, ids, msg := parseGroupRead(args)
	if msg != "" {
		r.Err(msg)
		return
	}

	g := registry(cx)
	results := make([]groupResult, 0, len(keys))
	for j := range keys {
		s, wrong := g.lookup(cx, keys[j])
		if wrong {
			r.Err(wrongType)
			return
		}
		grp := groupOf(s, grpName)
		if grp == nil {
			r.Err(nogroupRead(keys[j], grpName))
			return
		}
		con := grp.ensureConsumer(conName)
		if isGreaterToken(ids[j]) {
			if entries := grp.deliverNew(s, con, opts.count, opts.noack, cx.NowMs); len(entries) > 0 {
				results = append(results, groupResult{key: keys[j], entries: entries})
			}
			continue
		}
		start, ok := parseStreamID(ids[j])
		if !ok {
			r.Err(errInvalidID)
			return
		}
		results = append(results, groupResult{key: keys[j], entries: grp.history(s, con, start, opts.count)})
	}

	if len(results) == 0 {
		cx.Aux = resp.AppendNullArray(cx.Aux[:0])
		r.Raw(cx.Aux)
		return
	}
	cx.Aux = frameGroupResults(cx.Aux[:0], results)
	r.Raw(cx.Aux)
}

// deliveredEntry is one entry an XREADGROUP reply carries: its ID and, when the
// log entry is live, its fields. A history read of a pending ID whose log entry an
// XDEL has removed carries live=false, which frames as [id, nil].
type deliveredEntry struct {
	id     streamID
	fields []field
	live   bool
}

// groupResult pairs a stream key with the entries a group read produced, held
// until the reply is framed.
type groupResult struct {
	key     []byte
	entries []deliveredEntry
}

// deliverNew serves the `>` form: the live entries above the group cursor, capped
// by count. Each delivered entry becomes a pending entry owned by the consumer
// unless NOACK, and the group cursor and lag advance past the last one. The PEL is
// created on the first non-NOACK delivery. A missing or empty stream delivers
// nothing.
func (grp *streamGroup) deliverNew(s *stream, con *streamConsumer, count int, noack bool, now int64) []deliveredEntry {
	if s == nil {
		return nil
	}
	entries := s.readAfter(grp.lastDeliveredID, count)
	if len(entries) == 0 {
		return nil
	}
	out := make([]deliveredEntry, len(entries))
	for i := range entries {
		out[i] = deliveredEntry{id: entries[i].id, fields: entries[i].fields, live: true}
		if noack {
			continue
		}
		if grp.pel == nil {
			grp.pel = newPEL()
		}
		grp.pel.insert(entries[i].id, now, con.ord)
		grp.pelCount++
		con.pelCount++
	}
	grp.lastDeliveredID = entries[len(entries)-1].id
	grp.entriesRead += uint64(len(entries))
	return out
}

// history serves the explicit-ID form: this consumer's pending entries with IDs at
// or above start, capped by count, each joined to its live log entry (nil fields
// where the entry was XDEL'd). It reads the PEL, never advancing the cursor or
// touching delivery counts, the post-crash recovery walk. An empty or absent PEL
// yields nothing.
func (grp *streamGroup) history(s *stream, con *streamConsumer, start streamID, count int) []deliveredEntry {
	if grp.pel == nil {
		return nil
	}
	var out []deliveredEntry
	grp.pel.walkFrom(start, func(pe *pelEntry) bool {
		if pe.consumerOrd != con.ord {
			return true
		}
		fields, live := s.entryAt(pe.id)
		out = append(out, deliveredEntry{id: pe.id, fields: fields, live: live})
		return count <= 0 || len(out) < count
	})
	return out
}

// frameGroupResults appends the XREADGROUP reply, the array of [key, entries]
// pairs, each entry a [id, fields] pair or [id, nil] for a pending entry whose log
// entry is gone.
func frameGroupResults(dst []byte, results []groupResult) []byte {
	dst = resp.AppendArrayHeader(dst, len(results))
	for _, rr := range results {
		dst = resp.AppendArrayHeader(dst, 2)
		dst = resp.AppendBulk(dst, rr.key)
		dst = resp.AppendArrayHeader(dst, len(rr.entries))
		for _, e := range rr.entries {
			dst = resp.AppendArrayHeader(dst, 2)
			dst = appendIDBulk(dst, e.id)
			if !e.live {
				dst = resp.AppendNullArray(dst)
				continue
			}
			dst = resp.AppendArrayHeader(dst, 2*len(e.fields))
			for i := range e.fields {
				dst = resp.AppendBulk(dst, e.fields[i].name)
				dst = resp.AppendBulk(dst, e.fields[i].value)
			}
		}
	}
	return dst
}

// groupReadOpts is the parsed XREADGROUP option prefix: the COUNT cap (-1
// unbounded) and NOACK. BLOCK is parsed and validated but does not yet park.
type groupReadOpts struct {
	count int
	noack bool
}

// parseGroupRead reads the GROUP g c prefix, the option clauses, and the STREAMS
// key/id lists. It returns the group and consumer names, the options, the key and
// id slices, and a Redis error text (empty on success). The GROUP keyword and its
// two names are mandatory and lead the command.
func parseGroupRead(args [][]byte) (grp, con []byte, opts groupReadOpts, keys, ids [][]byte, msg string) {
	opts.count = -1
	if len(args) < 3 || !eqFold(args[0], "GROUP") {
		return nil, nil, opts, nil, nil, "Missing GROUP keyword or consumer/group name in XREADGROUP with the GROUP option"
	}
	grp, con = args[1], args[2]
	for i := 3; i < len(args); {
		switch {
		case eqFold(args[i], "COUNT") && i+1 < len(args):
			n, ok := parseUint(args[i+1])
			if !ok {
				return nil, nil, opts, nil, nil, "ERR value is not an integer or out of range"
			}
			opts.count = int(n)
			if opts.count == 0 {
				opts.count = -1
			}
			i += 2
		case eqFold(args[i], "BLOCK") && i+1 < len(args):
			ms, ok := parseBlockMs(args[i+1])
			if !ok {
				return nil, nil, opts, nil, nil, "ERR timeout is not an integer or out of range"
			}
			if ms < 0 {
				return nil, nil, opts, nil, nil, "ERR timeout is negative"
			}
			i += 2
		case eqFold(args[i], "NOACK"):
			opts.noack = true
			i++
		case eqFold(args[i], "STREAMS"):
			rest := args[i+1:]
			if len(rest) == 0 || len(rest)%2 != 0 {
				return nil, nil, opts, nil, nil, "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '>' must be specified."
			}
			nk := len(rest) / 2
			return grp, con, opts, rest[:nk], rest[nk:], ""
		default:
			return nil, nil, opts, nil, nil, "ERR syntax error"
		}
	}
	return nil, nil, opts, nil, nil, "ERR syntax error"
}

// groupOf returns the named group on s, tolerating a nil stream (a missing key) by
// reporting no group, so the caller answers NOGROUP either way.
func groupOf(s *stream, name []byte) *streamGroup {
	if s == nil {
		return nil
	}
	return s.group(name)
}

// isGreaterToken reports whether an XREADGROUP id argument is the ">" new-messages
// token rather than an explicit ID.
func isGreaterToken(idArg []byte) bool {
	return len(idArg) == 1 && idArg[0] == '>'
}

// nogroupRead builds the NOGROUP error XREADGROUP gives for a missing key or group,
// the wording Redis uses on the read path (distinct from XGROUP's).
func nogroupRead(key, name []byte) string {
	return "NOGROUP No such key '" + string(key) + "' or consumer group '" + string(name) + "' in XREADGROUP with GROUP option"
}

// GroupReadKeys extracts an XREADGROUP tail's stream keys for the dispatcher's
// co-location check, returning nil on a malformed tail so the handler answers the
// exact error in place. GroupReadKeyAt returns the tail index of the first stream
// key, the single-shard routing key, or -1.
func GroupReadKeys(tail [][]byte) [][]byte {
	at, ok := groupStreamsAt(tail)
	if !ok {
		return nil
	}
	rest := tail[at:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil
	}
	return rest[:len(rest)/2]
}

// GroupReadKeyAt returns the tail index of the first stream key, or -1 on a
// malformed tail.
func GroupReadKeyAt(tail [][]byte) int {
	at, ok := groupStreamsAt(tail)
	if !ok {
		return -1
	}
	rest := tail[at:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return -1
	}
	return at
}

// groupStreamsAt returns the index just past the STREAMS token in an XREADGROUP
// tail, skipping the mandatory GROUP g c prefix and the COUNT, BLOCK, and NOACK
// clauses. ok is false when the tail is not a well-formed group read.
func groupStreamsAt(tail [][]byte) (int, bool) {
	if len(tail) < 3 || !eqFold(tail[0], "GROUP") {
		return 0, false
	}
	for i := 3; i < len(tail); {
		switch {
		case (eqFold(tail[i], "COUNT") || eqFold(tail[i], "BLOCK")) && i+1 < len(tail):
			i += 2
		case eqFold(tail[i], "NOACK"):
			i++
		case eqFold(tail[i], "STREAMS"):
			return i + 1, true
		default:
			return 0, false
		}
	}
	return 0, false
}
