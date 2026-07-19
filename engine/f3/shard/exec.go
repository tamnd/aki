package shard

import (
	"strconv"

	"github.com/tamnd/aki/engine/f3/store"
	"github.com/tamnd/aki/f3srv/resp"
)

// The EXEC execution seam (spec 2064/f3/17 sections on MULTI/EXEC): the dispatch
// layer queues a connection's commands, then runs the whole queue in one step
// under an F17 intent transaction holding every key the queue touches. The
// transaction body needs to run an arbitrary registered handler on the owner that
// holds a key's lock and capture its reply bytes rather than push them, because
// the EXEC reply is one array gathering every queued command's answer. This file
// is that owner-side capture, kept in the shard package where the worker's handler
// vector, the reply arena, and the streamed-reply drain all live. It rides the
// same opRun control message Txn.Do posts, so a captured step serializes against
// the point traffic exactly like any tier-two hop.

// CallOwner runs fn on shard's owner goroutine and blocks until it completes. It
// posts an opRun with no intent, so the owner drains it off its intent control
// queue independent of the barrier, which is what lets an EXEC step run a keyless
// or unlocked command (INFO's per-shard scatter, a PING) without holding a lock on
// a key it does not name. fn runs single-owner on the target shard, so it may
// touch that shard's owner-only structures exactly like a handler. Reader
// goroutine only (it blocks on the owner's reply).
func (c *Conn) CallOwner(shard int, fn func(*Ctx)) {
	done := make(chan struct{})
	c.rt.workers[shard].postIntent(&intentOp{kind: opRun, fn: fn, done: done})
	<-done
}

// captureOn runs handler h with args against the owner cx belongs to and returns
// the RESP reply bytes it produced. It is the core of every EXEC step: a throwaway
// batch (its conn set so Reply.Resp3 and the null shapes read the negotiated
// protocol) is the reply sink, execNoBlock is set so a blocking verb serves or
// answers its would-block reply instead of parking, and a streamed reply is framed
// inline on the owner where its source may be pumped. The returned bytes are a
// fresh copy, so the caller may hold them after the throwaway batch is dropped.
// Owner goroutine only.
func captureOn(cx *Ctx, conn *Conn, h Handler, args [][]byte) []byte {
	if h == nil {
		return resp.AppendError(nil, "ERR unknown op")
	}
	b := &hopBatch{conn: conn}
	prevBlock, prevConn, prevSeq := cx.execNoBlock, cx.curConn, cx.curSeq
	cx.execNoBlock = true
	cx.curConn = conn
	cx.curSeq = 0
	h(cx, args, Reply{b: b, i: 0})
	cx.execNoBlock, cx.curConn, cx.curSeq = prevBlock, prevConn, prevSeq
	if b.blocked(0) {
		// A blocking verb parked despite execNoBlock (an unguarded park site):
		// answer the RESP2 null array, the would-block reply, so the EXEC element
		// is never empty. The guarded verbs never reach here.
		return resp.AppendNullArray(nil)
	}
	if st := b.stream(0); st != nil {
		return drainStreamCaptured(st)
	}
	return append([]byte(nil), b.reply(0)...)
}

// drainStreamCaptured materializes a streamed reply into one buffer on the owner
// goroutine (where the source may be pumped) and frames it as the EXEC element:
// the bulk header and trailer for a chunked value, or the source's own bytes for a
// self-framed multi-bulk (SMEMBERS over a big set). It releases the source's pins
// through finish, the same owner-side release the streaming writer path makes.
func drainStreamCaptured(st *stream) []byte {
	var out []byte
	if !st.raw {
		out = append(out, '$')
		out = strconv.AppendInt(out, st.total, 10)
		out = append(out, '\r', '\n')
	}
	buf := make([]byte, store.ChunkSize)
	for {
		n, err := st.src.Next(buf)
		if err != nil || n == 0 {
			break
		}
		out = append(out, buf[:n]...)
	}
	if !st.raw {
		out = append(out, '\r', '\n')
	}
	st.finish()
	return out
}

// RunPointCaptured runs a single-key command's handler under the transaction's
// barrier: op's handler executes on the owner that holds key's intent lock through
// Txn.Do, and its reply is captured for the EXEC array. When key is not among the
// transaction's held intents (the union should always include it, so this is a
// safety net) it falls back to a direct owner call so a reply is still produced.
// Reader goroutine only.
func (c *Conn) RunPointCaptured(t *Txn, key []byte, op byte, argv [][]byte) []byte {
	var out []byte
	ran := false
	t.Do(key, func(cx *Ctx) {
		out = captureOn(cx, c, handlerAt(cx, op), argv)
		ran = true
	})
	if !ran {
		c.CallOwner(c.rt.ShardOf(key), func(cx *Ctx) {
			out = captureOn(cx, c, handlerAt(cx, op), argv)
		})
	}
	return out
}

// RunKeylessCaptured runs a keyless command's handler on one owner (shard zero)
// and captures its reply, the EXEC step for a verb that names no key (PING, ECHO,
// TIME). A keyless command is shard-agnostic, so the owner choice is arbitrary.
// Reader goroutine only.
func (c *Conn) RunKeylessCaptured(op byte, argv [][]byte) []byte {
	var out []byte
	c.CallOwner(0, func(cx *Ctx) {
		out = captureOn(cx, c, handlerAt(cx, op), argv)
	})
	return out
}

// RunFanAllCaptured runs a keyless fan sub-command on every shard and folds the
// partials into the fan's final reply, the EXEC step for an aggregating verb (INFO,
// DBSIZE, KEYS, SCAN, RANDOMKEY, FLUSHALL, MEMORY STATS/DOCTOR). It mirrors the
// scatter-gather DoFanAll runs on the live path, but synchronously and off the
// reorder ring: each shard's partial is captured through captureOn and folded, then
// renderFan builds the reply. argv is the shared per-shard argument tail (a match
// pattern for KEYS, the cursor and options for SCAN, nil otherwise). Reader
// goroutine only.
func (c *Conn) RunFanAllCaptured(op byte, kind FanKind, argv [][]byte) []byte {
	fc := &fanCmd{kind: kind}
	for sh := range c.rt.workers {
		var part []byte
		c.CallOwner(sh, func(cx *Ctx) {
			part = captureOn(cx, c, handlerAt(cx, op), argv)
		})
		fc.fold(part, nil)
	}
	return c.renderFan(fc)
}

// RunCrossCaptured runs a tier-two cross-shard command body under the barrier and
// returns its already-RESP reply, the EXEC step for a command whose keys span
// shards (RENAME, SMOVE, the set algebra, the STORE forms). The body is
// shard-count-agnostic by construction, so it is called with the transaction that
// already holds every key, exactly as dispatchCross calls it on the live path.
// Reader goroutine only.
func (c *Conn) RunCrossCaptured(t *Txn, run func(t *Txn) []byte) []byte {
	return run(t)
}

// handlerAt returns the registered handler for op on cx's worker, or nil when op
// is out of range. Owner goroutine only (it reads the worker's fixed table).
func handlerAt(cx *Ctx, op byte) Handler {
	if cx.w == nil || int(op) >= len(cx.w.handlers) {
		return nil
	}
	return cx.w.handlers[op]
}
