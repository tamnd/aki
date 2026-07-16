package stream

import (
	"sort"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/obs1srv/resp"
)

// XINFO, the stream introspection surface (spec 2064/f3/14 sections 6.4 and 7).
// GROUPS walks the group table reading maintained fields, O(groups). STREAM reads
// the header and two end peeks, O(log C). CONSUMERS reads each consumer's
// maintained fields and clocks, O(consumers). STREAM FULL dumps the header, a
// COUNT-bounded window of entries, and every group with a COUNT-bounded sample of
// its PEL and its consumers, the one report that walks the pending set (bounded by
// COUNT, plus an O(pel) count for the nacked total, the sole full-PEL scan and only
// on this debug command).

// errNoSuchKey is Redis's reply for XINFO on a key that does not exist.
const errNoSuchKey = "ERR no such key"

// fullDefaultCount is XINFO STREAM FULL's default entry and PEL sample size when no
// COUNT is given, matching Redis; COUNT 0 means unbounded.
const fullDefaultCount = 10

// Xinfo dispatches the XINFO subcommands: GROUPS key, CONSUMERS key group, and
// STREAM key [FULL [COUNT n]]. An unknown subcommand or a wrong argument count is
// Redis's shared XINFO error.
func Xinfo(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	switch {
	case eqFold(args[0], "GROUPS") && len(args) == 2:
		xinfoGroups(cx, args[1], r)
	case eqFold(args[0], "CONSUMERS") && len(args) == 3:
		xinfoConsumers(cx, args[1], args[2], r)
	case eqFold(args[0], "STREAM") && len(args) >= 2:
		xinfoStream(cx, args, r)
	default:
		r.Err(unknownXinfo(args[0]))
	}
}

// unknownXinfo builds the shared XINFO error naming the offending subcommand token.
func unknownXinfo(sub []byte) string {
	return "ERR Unknown XINFO subcommand or wrong number of arguments for '" + string(sub) + "'"
}

// xinfoGroups answers XINFO GROUPS key: one flat map per group with its name,
// consumer count, pending total, last-delivered-id, entries-read, and lag. A
// missing key errors; a stream with no groups (including any inline stream)
// replies the empty array. entries-read and lag are the RESP2 null bulk when the
// group's lag basis is not tracked (section 7.8).
func xinfoGroups(cx *shard.Ctx, key []byte, r shard.Reply) {
	s, wrong := registry(cx).lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Err(errNoSuchKey)
		return
	}

	// Emit groups in name order so the reply is deterministic across the map's
	// unspecified iteration order, the way a client comparing runs expects.
	names := groupNamesSorted(s)

	out := resp.AppendArrayHeader(cx.Aux[:0], len(names))
	for _, name := range names {
		grp := s.groups[name]
		out = resp.AppendArrayHeader(out, 12)
		out = resp.AppendBulk(out, []byte("name"))
		out = resp.AppendBulk(out, []byte(name))
		out = resp.AppendBulk(out, []byte("consumers"))
		out = resp.AppendInt(out, int64(len(grp.consumers)))
		out = resp.AppendBulk(out, []byte("pending"))
		out = resp.AppendInt(out, int64(grp.pending()))
		out = resp.AppendBulk(out, []byte("last-delivered-id"))
		out = appendIDBulk(out, grp.lastDeliveredID)
		out = resp.AppendBulk(out, []byte("entries-read"))
		out = appendReadCount(out, grp.entriesRead, grp.readValid)
		out = resp.AppendBulk(out, []byte("lag"))
		out = appendReadCount(out, s.entriesAdded-grp.entriesRead, grp.readValid)
	}
	cx.Aux = out
	r.Raw(out)
}

// xinfoStream answers XINFO STREAM key [FULL [COUNT n]]. The summary form reports
// the header fields and the first and last live entries; the FULL form adds a
// COUNT-bounded entry window and every group with a COUNT-bounded PEL and consumer
// sample. A missing key errors; a wrong-typed key is WRONGTYPE; a malformed FULL or
// COUNT tail is the shared XINFO error.
func xinfoStream(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	key := args[1]
	full, count, ok := parseStreamInfoOpts(args[2:])
	if !ok {
		r.Err(unknownXinfo(args[0]))
		return
	}
	s, wrong := registry(cx).lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Err(errNoSuchKey)
		return
	}
	if full {
		cx.Aux = appendStreamFull(cx.Aux[:0], s, count)
	} else {
		cx.Aux = appendStreamSummary(cx.Aux[:0], s)
	}
	r.Raw(cx.Aux)
}

// parseStreamInfoOpts reads the [FULL [COUNT n]] tail of XINFO STREAM. An empty tail
// is the summary form; FULL alone defaults COUNT to 10; FULL COUNT n bounds the
// samples, with 0 meaning unbounded and a negative folded to the default, as Redis
// does. ok is false for any other tail.
func parseStreamInfoOpts(tail [][]byte) (full bool, count int, ok bool) {
	if len(tail) == 0 {
		return false, 0, true
	}
	if !eqFold(tail[0], "FULL") {
		return false, 0, false
	}
	if len(tail) == 1 {
		return true, fullDefaultCount, true
	}
	if len(tail) == 3 && eqFold(tail[1], "COUNT") {
		n, nok := parseInt(tail[2])
		if !nok {
			return false, 0, false
		}
		if n < 0 {
			n = fullDefaultCount
		}
		return true, int(n), true
	}
	return false, 0, false
}

// appendStreamSummary emits the XINFO STREAM header map: length, the synthesized
// radix-tree geometry, the id and counter fields, the group count, and the first and
// last live entries (null when the stream is empty). The radix-tree-keys and -nodes
// are derived from the live block count, monotone and plausible since nothing depends
// on their exact values (section 6.4).
func appendStreamSummary(dst []byte, s *stream) []byte {
	first := s.collectRange(fullLo, fullHi, false, 1)
	last := s.collectRange(fullLo, fullHi, true, 1)
	var recorded streamID
	if len(first) > 0 {
		recorded = first[0].id
	}

	dst = resp.AppendArrayHeader(dst, 20)
	dst = appendStreamHeaderFields(dst, s, recorded)
	dst = resp.AppendBulk(dst, []byte("groups"))
	dst = resp.AppendInt(dst, int64(len(s.groups)))
	dst = resp.AppendBulk(dst, []byte("first-entry"))
	dst = appendEntryOrNull(dst, first)
	dst = resp.AppendBulk(dst, []byte("last-entry"))
	dst = appendEntryOrNull(dst, last)
	return dst
}

// appendStreamFull emits the XINFO STREAM FULL report: the same header, a
// COUNT-bounded entry window, then every group with its metadata, a COUNT-bounded
// sample of the group PEL, and each consumer with a COUNT-bounded sample of its own
// pending entries. Groups and consumers are emitted in name order for a deterministic
// reply.
func appendStreamFull(dst []byte, s *stream, count int) []byte {
	first := s.collectRange(fullLo, fullHi, false, 1)
	var recorded streamID
	if len(first) > 0 {
		recorded = first[0].id
	}

	dst = resp.AppendArrayHeader(dst, 18)
	dst = appendStreamHeaderFields(dst, s, recorded)

	dst = resp.AppendBulk(dst, []byte("entries"))
	entries := s.collectRange(fullLo, fullHi, false, entryLimit(count))
	dst = resp.AppendArrayHeader(dst, len(entries))
	for i := range entries {
		dst = appendEntryReply(dst, entries[i].id, entries[i].fields)
	}

	dst = resp.AppendBulk(dst, []byte("groups"))
	names := groupNamesSorted(s)
	dst = resp.AppendArrayHeader(dst, len(names))
	for _, name := range names {
		dst = appendGroupFull(dst, s.groups[name], name, s.entriesAdded, count)
	}
	return dst
}

// appendStreamHeaderFields emits the seven header key-value pairs the summary and
// FULL forms share: length, radix-tree-keys, radix-tree-nodes, last-generated-id,
// max-deleted-entry-id, entries-added, and recorded-first-entry-id.
func appendStreamHeaderFields(dst []byte, s *stream, recorded streamID) []byte {
	keys := int64(len(s.blocks))
	dst = resp.AppendBulk(dst, []byte("length"))
	dst = resp.AppendInt(dst, int64(s.length))
	dst = resp.AppendBulk(dst, []byte("radix-tree-keys"))
	dst = resp.AppendInt(dst, keys)
	dst = resp.AppendBulk(dst, []byte("radix-tree-nodes"))
	dst = resp.AppendInt(dst, keys+1)
	dst = resp.AppendBulk(dst, []byte("last-generated-id"))
	dst = appendIDBulk(dst, s.lastID)
	dst = resp.AppendBulk(dst, []byte("max-deleted-entry-id"))
	dst = appendIDBulk(dst, s.maxDeletedID)
	dst = resp.AppendBulk(dst, []byte("entries-added"))
	dst = resp.AppendInt(dst, int64(s.entriesAdded))
	dst = resp.AppendBulk(dst, []byte("recorded-first-entry-id"))
	dst = appendIDBulk(dst, recorded)
	return dst
}

// appendGroupFull emits one group's FULL map: name, cursor, the lag basis, the two
// PEL counts, a COUNT-bounded sample of the group PEL (each row id, owner-or-empty,
// delivery time, delivery count), and the group's consumers.
func appendGroupFull(dst []byte, grp *streamGroup, name string, entriesAdded uint64, count int) []byte {
	dst = resp.AppendArrayHeader(dst, 16)
	dst = resp.AppendBulk(dst, []byte("name"))
	dst = resp.AppendBulk(dst, []byte(name))
	dst = resp.AppendBulk(dst, []byte("last-delivered-id"))
	dst = appendIDBulk(dst, grp.lastDeliveredID)
	dst = resp.AppendBulk(dst, []byte("entries-read"))
	dst = appendReadCount(dst, grp.entriesRead, grp.readValid)
	dst = resp.AppendBulk(dst, []byte("lag"))
	dst = appendReadCount(dst, entriesAdded-grp.entriesRead, grp.readValid)
	dst = resp.AppendBulk(dst, []byte("pel-count"))
	dst = resp.AppendInt(dst, int64(grp.pelCount))
	dst = resp.AppendBulk(dst, []byte("nacked-count"))
	dst = resp.AppendInt(dst, grp.nackedCount())

	dst = resp.AppendBulk(dst, []byte("pending"))
	rows := grp.pelSample(count, nil)
	dst = resp.AppendArrayHeader(dst, len(rows))
	for _, pe := range rows {
		dst = resp.AppendArrayHeader(dst, 4)
		dst = appendIDBulk(dst, pe.id)
		if pe.consumerOrd == noOwner {
			dst = resp.AppendBulk(dst, nil)
		} else {
			dst = resp.AppendBulk(dst, grp.consumerByOrd[pe.consumerOrd].name)
		}
		dst = resp.AppendInt(dst, pe.deliveryTime)
		dst = resp.AppendInt(dst, int64(pe.deliveryCount))
	}

	dst = resp.AppendBulk(dst, []byte("consumers"))
	cons := consumersSorted(grp)
	dst = resp.AppendArrayHeader(dst, len(cons))
	for _, con := range cons {
		dst = appendConsumerFull(dst, grp, con, count)
	}
	return dst
}

// appendConsumerFull emits one consumer's FULL map: name, the two absolute clocks,
// its pending count, and a COUNT-bounded sample of the entries it owns (each row id,
// delivery time, delivery count).
func appendConsumerFull(dst []byte, grp *streamGroup, con *streamConsumer, count int) []byte {
	dst = resp.AppendArrayHeader(dst, 10)
	dst = resp.AppendBulk(dst, []byte("name"))
	dst = resp.AppendBulk(dst, con.name)
	dst = resp.AppendBulk(dst, []byte("seen-time"))
	dst = resp.AppendInt(dst, con.seenTime)
	dst = resp.AppendBulk(dst, []byte("active-time"))
	dst = resp.AppendInt(dst, con.activeTime)
	dst = resp.AppendBulk(dst, []byte("pel-count"))
	dst = resp.AppendInt(dst, int64(con.pelCount))
	dst = resp.AppendBulk(dst, []byte("pending"))
	rows := grp.pelSample(count, func(pe *pelEntry) bool { return pe.consumerOrd == con.ord })
	dst = resp.AppendArrayHeader(dst, len(rows))
	for _, pe := range rows {
		dst = resp.AppendArrayHeader(dst, 3)
		dst = appendIDBulk(dst, pe.id)
		dst = resp.AppendInt(dst, pe.deliveryTime)
		dst = resp.AppendInt(dst, int64(pe.deliveryCount))
	}
	return dst
}

// xinfoConsumers answers XINFO CONSUMERS key group: one map per consumer in name
// order with its name, pending count, idle (now-seenTime, floored at zero), and
// inactive (now-activeTime, or -1 when the consumer has never fetched). A missing key
// errors; an unknown group is NOGROUP.
func xinfoConsumers(cx *shard.Ctx, key, group []byte, r shard.Reply) {
	s, wrong := registry(cx).lookup(cx, key)
	if wrong {
		r.Err(wrongType)
		return
	}
	if s == nil {
		r.Err(errNoSuchKey)
		return
	}
	grp := s.group(group)
	if grp == nil {
		r.Err(nogroup(group, key))
		return
	}

	cons := consumersSorted(grp)
	out := resp.AppendArrayHeader(cx.Aux[:0], len(cons))
	for _, con := range cons {
		idle := cx.NowMs - con.seenTime
		if idle < 0 {
			idle = 0
		}
		inactive := int64(-1)
		if con.activeTime != -1 {
			inactive = cx.NowMs - con.activeTime
		}
		out = resp.AppendArrayHeader(out, 8)
		out = resp.AppendBulk(out, []byte("name"))
		out = resp.AppendBulk(out, con.name)
		out = resp.AppendBulk(out, []byte("pending"))
		out = resp.AppendInt(out, int64(con.pelCount))
		out = resp.AppendBulk(out, []byte("idle"))
		out = resp.AppendInt(out, idle)
		out = resp.AppendBulk(out, []byte("inactive"))
		out = resp.AppendInt(out, inactive)
	}
	cx.Aux = out
	r.Raw(out)
}

// fullLo and fullHi are the full-stream range bounds, the [0-0, max] window XINFO's
// entry peeks and dump read over.
var (
	fullLo = bound{id: streamID{ms: 0, seq: 0}}
	fullHi = bound{id: streamID{ms: ^uint64(0), seq: ^uint64(0)}}
)

// entryLimit maps an XINFO FULL count to a collectRange limit: a positive count caps
// the window, and 0 (COUNT 0, unbounded) becomes -1, since collectRange reads 0 as
// "return nothing" rather than "no limit".
func entryLimit(count int) int {
	if count == 0 {
		return -1
	}
	return count
}

// appendEntryOrNull emits the single-entry range as an [id, [field value ...]] pair,
// or the null bulk when the range is empty, the shape XINFO STREAM's first-entry and
// last-entry fields take.
func appendEntryOrNull(dst []byte, entries []rangeEntry) []byte {
	if len(entries) == 0 {
		return resp.AppendNull(dst)
	}
	return appendEntryReply(dst, entries[0].id, entries[0].fields)
}

// groupNamesSorted returns the stream's group names in ascending order, the
// deterministic iteration XINFO GROUPS and STREAM FULL emit groups in.
func groupNamesSorted(s *stream) []string {
	names := make([]string, 0, len(s.groups))
	for name := range s.groups {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// consumersSorted returns the group's live consumers in name order, skipping the nil
// holes a DELCONSUMER leaves in the ordinal table. XINFO CONSUMERS and STREAM FULL
// emit consumers this way.
func consumersSorted(grp *streamGroup) []*streamConsumer {
	cons := make([]*streamConsumer, 0, len(grp.consumers))
	for _, con := range grp.consumers {
		cons = append(cons, con)
	}
	sort.Slice(cons, func(i, j int) bool { return string(cons[i].name) < string(cons[j].name) })
	return cons
}

// pending is the group's total pending-entry count, the maintained pelCount read
// O(1) without a PEL walk.
func (grp *streamGroup) pending() int { return int(grp.pelCount) }

// appendIDBulk appends id as a bulk string in Redis's "ms-seq" form.
func appendIDBulk(dst []byte, id streamID) []byte {
	return resp.AppendBulk(dst, formatID(nil, id))
}

// appendReadCount appends n as an integer when valid, or the RESP2 null bulk when
// the group cannot track the value, matching Redis's entries-read and lag fields.
func appendReadCount(dst []byte, n uint64, valid bool) []byte {
	if !valid {
		return resp.AppendNull(dst)
	}
	return resp.AppendInt(dst, int64(n))
}
