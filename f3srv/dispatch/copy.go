// COPY duplicates a key of any type to a destination, RENAME's non-destructive
// sibling over the same DUMP/RESTORE serialize primitive: it serializes the source
// and installs the copy, but leaves the source in place. It answers 1 on a copy and
// 0 when it declines (a missing source, or a destination that already holds a key
// and no REPLACE). A co-located pair runs on one owner through the point path; a
// pair spanning shards rides the tier-two intent path RENAME uses, minus the source
// drop. The copy inherits the source's TTL. f3 keeps one database (SELECT accepts
// only 0), so the DB option is honored only for destination 0 and any other index
// is out of range.
package dispatch

import (
	"bytes"
	"strconv"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// parseCopyOpts walks the COPY option tail: REPLACE is a flag, DB takes a
// destination-database index. f3 has a single database, so a DB index other than 0
// is out of range and a non-integer is the value error, both byte-matched to redis;
// an unknown token is a syntax error. errMsg is empty when the tail is well-formed.
func parseCopyOpts(opts [][]byte) (replace bool, errMsg string) {
	for i := 0; i < len(opts); {
		switch {
		case tokenIs(opts[i], "REPLACE"):
			replace = true
			i++
		case tokenIs(opts[i], "DB"):
			if i+1 >= len(opts) {
				return false, "ERR syntax error"
			}
			n, err := strconv.ParseInt(string(opts[i+1]), 10, 64)
			if err != nil {
				return false, "ERR value is not an integer or out of range"
			}
			if n != 0 {
				return false, "ERR DB index is out of range"
			}
			i += 2
		default:
			return false, "ERR syntax error"
		}
	}
	return replace, ""
}

// copyCmd answers the co-located COPY src dst [DB n] [REPLACE]: serialize the
// source and install the copy at the destination, leaving the source untouched. A
// missing source answers 0; a destination that already holds a key answers 0 unless
// REPLACE clears it first; COPY onto the same key is the "source and destination are
// the same" error. The copy inherits the source's TTL.
func copyCmd(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	src, dst := args[0], args[1]
	replace, errMsg := parseCopyOpts(args[2:])
	if errMsg != "" {
		r.Err(errMsg)
		return
	}
	if bytes.Equal(src, dst) {
		r.Err("ERR source and destination objects are the same")
		return
	}
	payload, ok := dumpPayload(cx, src)
	if !ok {
		r.Int(0)
		return
	}
	if !replace && keyExistsAnywhere(cx, dst) {
		r.Int(0)
		return
	}
	at := srcDeadline(cx, src)
	restoreClear(cx, dst)
	if err := restorePayload(cx, dst, payload, at); err != nil {
		r.Err("ERR Bad data format")
		return
	}
	r.Int(1)
}

// copyCross runs COPY across shards under a transaction holding both keys: hop to
// the source to serialize it and read its deadline, then hop to the destination to
// install the copy. It leaves the source in place, so it is RENAME's cross plan
// without the third drop hop. Reading the destination's occupancy inside the install
// hop keeps the REPLACE decision and the write one atomic step under the barrier.
// src and dst are distinct keys on distinct shards by the dispatch check, so the
// same-key error is answered on the point path and never reaches here.
func copyCross(t *shard.Txn, args [][]byte) []byte {
	src, dst := args[0], args[1]
	replace, errMsg := parseCopyOpts(args[2:])
	if errMsg != "" {
		return resp.AppendError(nil, errMsg)
	}
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
		return resp.AppendInt(nil, 0)
	}
	var installed bool
	t.Do(dst, func(cx *shard.Ctx) {
		if !replace && keyExistsAnywhere(cx, dst) {
			return
		}
		restoreClear(cx, dst)
		if err := restorePayload(cx, dst, payload, at); err == nil {
			installed = true
		}
	})
	if installed {
		return resp.AppendInt(nil, 1)
	}
	return resp.AppendInt(nil, 0)
}
