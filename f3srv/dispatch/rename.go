// RENAME and RENAMENX relocate a key of any type, the generic move DUMP and
// RESTORE make possible: the same dumpPayload/restorePayload pair serializes any
// of the six keyspaces and rebuilds it, so one handler renames a string, a set, a
// zset, a hash, a list, or a stream without a per-type move primitive. A co-located
// pair (the {tag}-hashed and single-shard norm) runs the whole move on one owner
// through the point path; a pair spanning shards rides the tier-two intent path the
// set algebra and SMOVE already use, holding a write intent on both keys while it
// serializes at the source, installs at the destination, and clears the source as
// three hops under the barrier. The destination inherits the source's TTL, Redis's
// rule; RENAMENX refuses when the destination already holds a key.
package dispatch

import (
	"bytes"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// srcDeadline resolves the absolute unix-ms deadline a rename carries from its
// source, 0 for a key with no TTL. It reads the same cross-keyspace keyDeadline
// TTL and its siblings use, so the destination inherits the source's lifetime
// whatever type the source is.
func srcDeadline(cx *shard.Ctx, key []byte) int64 {
	if state, at := keyDeadline(cx, key); state == 0 {
		return at
	}
	return 0
}

// renameCmd answers the co-located RENAME src dst: serialize the source, overwrite
// the destination with it (across every keyspace, since the destination may hold a
// different type), and drop the source. A source present nowhere is the "no such
// key" error. RENAME onto itself is a no-op that still confirms, the Redis contract
// for a source that exists. The destination inherits the source's TTL.
func renameCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	src, dst := args[0], args[1]
	payload, ok := dumpPayload(cx, src)
	if !ok {
		r.Err("ERR no such key")
		return
	}
	if bytes.Equal(src, dst) {
		r.Status("OK")
		return
	}
	at := srcDeadline(cx, src)
	restoreClear(cx, dst)
	if err := restorePayload(cx, dst, payload, at); err != nil {
		r.Err("ERR Bad data format")
		return
	}
	restoreClear(cx, src)
	r.Status("OK")
}

// renamenxCmd answers the co-located RENAMENX src dst: RENAME's conditional twin,
// installing only when the destination is free. A missing source is the same "no
// such key" error; a destination that already holds a key answers 0 and leaves both
// keys untouched; otherwise the move runs and answers 1. RENAMENX onto itself finds
// the destination occupied by the source, so it answers 0.
func renamenxCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	src, dst := args[0], args[1]
	payload, ok := dumpPayload(cx, src)
	if !ok {
		r.Err("ERR no such key")
		return
	}
	if bytes.Equal(src, dst) || keyExistsAnywhere(cx, dst) {
		r.Int(0)
		return
	}
	at := srcDeadline(cx, src)
	if err := restorePayload(cx, dst, payload, at); err != nil {
		r.Err("ERR Bad data format")
		return
	}
	restoreClear(cx, src)
	r.Int(1)
}

// renameCross runs RENAME across shards under a transaction holding both keys: hop
// to the source to serialize it and read its deadline, hop to the destination to
// overwrite it, then hop back to the source to drop it. The three hops are one
// atomic step from every other command's view, the same barrier SMOVE's cross plan
// leans on. src and dst are distinct keys on distinct shards by the dispatch check.
func renameCross(t *shard.Txn, args [][]byte) []byte {
	src, dst := args[0], args[1]
	var payload []byte
	var at int64
	var have bool
	t.Do(src, func(cx *shard.Ctx) {
		p, ok := dumpPayload(cx, src)
		if !ok {
			return
		}
		payload = p
		at = srcDeadline(cx, src)
		have = true
	})
	if !have {
		return resp.AppendError(nil, "ERR no such key")
	}
	var bad bool
	t.Do(dst, func(cx *shard.Ctx) {
		restoreClear(cx, dst)
		if err := restorePayload(cx, dst, payload, at); err != nil {
			bad = true
		}
	})
	if bad {
		return resp.AppendError(nil, "ERR Bad data format")
	}
	t.Do(src, func(cx *shard.Ctx) {
		restoreClear(cx, src)
	})
	return resp.AppendStatus(nil, "OK")
}

// renamenxCross runs RENAMENX across shards, RENAME's conditional twin: it serializes
// the source, then on the destination hop refuses when a key already sits there
// (answering 0 and touching nothing) and otherwise installs before the source drop.
// Reading the destination's occupancy inside the install hop keeps the check and the
// write one atomic decision under the barrier.
func renamenxCross(t *shard.Txn, args [][]byte) []byte {
	src, dst := args[0], args[1]
	var payload []byte
	var at int64
	var have bool
	t.Do(src, func(cx *shard.Ctx) {
		p, ok := dumpPayload(cx, src)
		if !ok {
			return
		}
		payload = p
		at = srcDeadline(cx, src)
		have = true
	})
	if !have {
		return resp.AppendError(nil, "ERR no such key")
	}
	var occupied, bad bool
	t.Do(dst, func(cx *shard.Ctx) {
		if keyExistsAnywhere(cx, dst) {
			occupied = true
			return
		}
		if err := restorePayload(cx, dst, payload, at); err != nil {
			bad = true
		}
	})
	switch {
	case occupied:
		return resp.AppendInt(nil, 0)
	case bad:
		return resp.AppendError(nil, "ERR Bad data format")
	}
	t.Do(src, func(cx *shard.Ctx) {
		restoreClear(cx, src)
	})
	return resp.AppendInt(nil, 1)
}
