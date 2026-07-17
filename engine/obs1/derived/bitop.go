package derived

// BITOP (spec 2064/f3/15 section 5): AND, OR, XOR over one or more source
// bitmaps, or NOT over exactly one, with the result stored at the destination
// and its length replied. The co-located case (destination and every source on
// one owner) runs the whole streaming algebra on that owner through the store,
// bounded to (sources + 1) chunks. The cross-shard case rides the F17 intent
// path: the transaction holds an intent on the destination and every source, so
// all of them are frozen at one point, then the coordinator streams the bitmaps
// a chunk at a time, reading each source's chunk from its owner with one hop per
// shard and writing the result chunk to the destination owner, so the same
// (sources + 1) chunk residency holds no matter how the keys are spread.

import (
	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/store"
	"github.com/tamnd/aki/obs1srv/resp"
)

const (
	errBitopNot     = "ERR BITOP NOT must be called with a single source key."
	errBitopTooLong = "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
)

// BitOp answers BITOP <AND|OR|XOR|NOT> destkey srckey [srckey ...] on the owner
// shared by the destination and every source (the co-located fast path). args
// is the tail after the verb: the operation token, the destination, then the
// sources.
func BitOp(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	op, ok := parseBitOp(args[0])
	if !ok {
		r.Err(errSyntax)
		return
	}
	if op == store.BitNot && len(args) != 3 {
		r.Err(errBitopNot)
		return
	}
	// The store deletes the destination itself when every source is empty,
	// so its prior liveness is captured up front to frame that removal; the
	// probe only runs with a log wired.
	existed := false
	if cx.Log != nil {
		_, existed = cx.St.StrLen(args[1], cx.NowMs)
	}
	n, err := cx.St.BitOp(op, args[1], args[2:], cx.NowMs)
	if err != nil {
		if err == store.ErrTooBig {
			r.Err(errBitopTooLong)
			return
		}
		r.Err("ERR " + err.Error())
		return
	}
	if n > 0 {
		err = cx.LogStrReadBack(args[1])
	} else if existed {
		err = cx.LogKeyDel(args[1])
	}
	if err != nil {
		r.Err(err.Error())
		return
	}
	r.Int(n)
}

// BitOpCross is the cross-shard entry dispatch routes to when the destination
// and sources span shards. It runs under the transaction's barrier, so every
// operand is frozen for the whole streaming pass. args is the tail after the
// verb: the operation token, the destination, then the sources.
func BitOpCross(t *shard.Txn, args [][]byte) []byte {
	op, ok := parseBitOp(args[0])
	if !ok {
		return resp.AppendError(nil, errSyntax)
	}
	dest := args[1]
	srcs := args[2:]
	if op == store.BitNot && len(srcs) != 1 {
		return resp.AppendError(nil, errBitopNot)
	}

	// Step 1 (doc 15 section 5): resolve every source length under the barrier,
	// one metadata hop per owning shard, no value copied. The groups built here
	// are reused for the per-chunk read hops below.
	groups := groupSources(t, srcs)
	lens := make([]int64, len(srcs))
	for _, g := range groups {
		idxs := g.idxs
		t.Do(g.key, func(cx *shard.Ctx) {
			for _, j := range idxs {
				lens[j], _ = cx.St.StrLen(srcs[j], cx.NowMs)
			}
		})
	}
	var maxlen, minlen int64
	minlen = -1
	for _, L := range lens {
		if L > maxlen {
			maxlen = L
		}
		if minlen < 0 || L < minlen {
			minlen = L
		}
	}
	// All sources empty: the result is empty, so the destination is deleted and
	// the reply is 0, the same rule the string type follows. The keydel frames
	// only when the delete removed something; the emission runs inside the hop
	// on the destination's owner, which is relaxed-only under the transaction
	// (the closure runs with no current command, so no strict mark can attach).
	if maxlen == 0 {
		var lerr error
		t.Do(dest, func(cx *shard.Ctx) {
			if cx.St.Del(dest, cx.NowMs) {
				lerr = cx.LogKeyDel(dest)
			}
		})
		if lerr != nil {
			return resp.AppendError(nil, lerr.Error())
		}
		return resp.AppendInt(nil, 0)
	}

	// An aliased destination (it is also a source) must not be cleared up front,
	// since it is still being read; every chunk is written in order so the old
	// bytes underneath are overwritten, and maxlen covers the aliased source so
	// no stale tail is left. A fresh destination is dropped first, then only live
	// chunks are written so all-zero interiors fall through to directory holes.
	aliased := false
	for _, k := range srcs {
		if string(k) == string(dest) {
			aliased = true
			break
		}
	}
	if !aliased {
		t.Do(dest, func(cx *shard.Ctx) { cx.St.Del(dest, cx.NowMs) })
	}

	chunk := store.ChunkSize
	// Per-source chunk buffers plus the result buffer: the (sources + 1) chunk
	// residency the memory bound promises, held on the coordinator across every
	// chunk regardless of bitmap length or how many shards the sources sit on.
	bufs := make([][]byte, len(srcs))
	for i := range bufs {
		bufs[i] = make([]byte, chunk)
	}
	res := make([]byte, chunk)
	views := make([][]byte, len(srcs))

	var writeErr error
	nChunks := (maxlen + int64(chunk) - 1) / int64(chunk)
	for k := int64(0); k < nChunks; k++ {
		cs := k * int64(chunk)
		cl := int64(chunk)
		if maxlen-cs < cl {
			cl = maxlen - cs
		}
		out := res[:cl]

		// Past the shortest source every AND byte is zero, so the read hops for
		// this chunk are skipped and the result is a hole (or, when aliased,
		// zeros). Otherwise read chunk k from each source, one hop per source
		// shard, then fold with the word kernel on the coordinator.
		var allZero bool
		if op == store.BitAnd && cs >= minlen {
			allZero = store.CombineChunk(op, out, nil)
		} else {
			for _, g := range groups {
				idxs, off, n := g.idxs, cs, cl
				t.Do(g.key, func(cx *shard.Ctx) {
					for _, j := range idxs {
						cx.St.ReadInto(srcs[j], off, bufs[j][:n], cx.NowMs)
					}
				})
			}
			for i := range views {
				views[i] = bufs[i][:cl]
			}
			allZero = store.CombineChunk(op, out, views)
		}

		last := k == nChunks-1
		// A fresh build skips all-zero interior chunks so they stay holes; the
		// final chunk is always written so the destination reaches maxlen exactly.
		// An aliased build writes every chunk to overwrite the old value beneath.
		if aliased || last || !allZero {
			off, w := int(cs), out
			t.Do(dest, func(cx *shard.Ctx) {
				if _, err := cx.St.SetRange(dest, off, w, cx.NowMs); err != nil {
					writeErr = err
				}
			})
			if writeErr != nil {
				// The destination may be partially written (or freshly
				// deleted with nothing landed yet); frame whatever the store
				// now holds so replay matches RAM even on the error path.
				if lerr := logCrossDest(t, dest); lerr != nil {
					return resp.AppendError(nil, lerr.Error())
				}
				if writeErr == store.ErrTooBig {
					return resp.AppendError(nil, errBitopTooLong)
				}
				return resp.AppendError(nil, "ERR "+writeErr.Error())
			}
		}
	}
	if err := logCrossDest(t, dest); err != nil {
		return resp.AppendError(nil, err.Error())
	}
	return resp.AppendInt(nil, maxlen)
}

// logCrossDest frames the destination's state after a cross-shard bit-op
// pass, on the destination's owner: the whole resulting value read back when
// the key is live, a keydel when it is not (the error path can leave a fresh
// destination deleted before any chunk landed; a keydel replaying over an
// already absent key is an idempotent no-op). Free when no log is wired.
func logCrossDest(t *shard.Txn, dest []byte) error {
	var err error
	t.Do(dest, func(cx *shard.Ctx) {
		if cx.Log == nil {
			return
		}
		if _, live := cx.St.StrLen(dest, cx.NowMs); live {
			err = cx.LogStrReadBack(dest)
		} else {
			err = cx.LogKeyDel(dest)
		}
	})
	return err
}

// srcGroup names the sources that share one shard: the coordinator reads them
// with a single hop to that shard.
type srcGroup struct {
	key  []byte
	idxs []int
}

// groupSources buckets the source keys by owning shard, in first-seen order, so a
// hop to each group's key resolves every source that shard owns. The transaction
// barrier already covers all of them, so the grouping only decides how few hops a
// chunk costs, not correctness.
func groupSources(t *shard.Txn, srcs [][]byte) []srcGroup {
	done := make([]bool, len(srcs))
	var gs []srcGroup
	for i := range srcs {
		if done[i] {
			continue
		}
		sh := t.Shard(srcs[i])
		g := srcGroup{key: srcs[i]}
		for j := i; j < len(srcs); j++ {
			if !done[j] && t.Shard(srcs[j]) == sh {
				g.idxs = append(g.idxs, j)
				done[j] = true
			}
		}
		gs = append(gs, g)
	}
	return gs
}

// parseBitOp maps the operation token to its store code, case-insensitively.
func parseBitOp(tok []byte) (int, bool) {
	switch upper(tok) {
	case "AND":
		return store.BitAnd, true
	case "OR":
		return store.BitOr, true
	case "XOR":
		return store.BitXor, true
	case "NOT":
		return store.BitNot, true
	}
	return 0, false
}
