package stream

import (
	"sort"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// XINFO, the stream introspection surface (spec 2064/f3/14 sections 6.4 and 7).
// This slice lands XINFO GROUPS, the group-metadata read the consumer-group
// lifecycle needs to be observable: it walks the group table reading maintained
// fields, O(groups), never a PEL walk. XINFO STREAM, XINFO CONSUMERS, and the
// FULL report arrive with the delivery and claim slices that fill the fields they
// add.

// errNoSuchKey is Redis's reply for XINFO on a key that does not exist.
const errNoSuchKey = "ERR no such key"

// Xinfo dispatches the XINFO subcommands. Only GROUPS is served in this slice;
// the others answer Redis's shared unknown-subcommand error until their fields
// exist.
func Xinfo(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	if eqFold(args[0], "GROUPS") && len(args) == 2 {
		xinfoGroups(cx, args[1], r)
		return
	}
	r.Err("ERR Unknown XINFO subcommand or wrong number of arguments for '" + string(args[0]) + "'")
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
	names := make([]string, 0, len(s.groups))
	for name := range s.groups {
		names = append(names, name)
	}
	sort.Strings(names)

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
