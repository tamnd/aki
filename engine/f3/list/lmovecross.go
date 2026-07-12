package list

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The cross-shard LMOVE plan (spec 2064/f3/03 section 6.7, spec 2064/f3/13
// section 5), the F17 intent path for the list family's two-key move. Slice 6
// shipped the co-located form (lmove.go), the free single-shard case dispatch
// keeps on the point path. This slice builds the cross path: when source and
// destination hash to different shards, the transaction holds an intent on both
// keys and the move runs as owner hops under the barrier, so no other command
// interleaves between reading the source and publishing the element. The
// co-located case never reaches here (dispatch keeps it on the single-shard
// fast path), and the same-key rotation cannot reach here either, since one key
// is one shard.
//
// This is the piece SMOVE's cross path (set/smovecross.go) did not need. SMOVE's
// moved value is a command argument, already in hand before the first hop. An
// LMOVE element is discovered by reading the source end, so the source hop must
// capture it: peek the end element and clone it into heap storage (like
// cloneBytes and set/gathercross.go's cloneSet) so it survives off the source
// owner's goroutine and the destination hop, running on another shard, can push
// it. That cross-hop value capture is the operand-gather aspect of the move.

// peekEnd returns the element at the source end without removing it: the head
// when left is true and the tail otherwise. The list must be non-empty. The
// bytes alias internal storage, so the caller clones before carrying them off
// the owner.
func peekEnd(l *list, left bool) []byte {
	if left {
		return l.get(0)
	}
	return l.get(l.length() - 1)
}

// LmoveCross runs LMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT> under an
// acquired transaction holding both keys, and returns the finished RESP reply.
// The semantics mirror the co-located lmove exactly, differentially tested
// against it: the source type is checked first, a missing or empty source
// replies the null bulk without ever consulting the destination type (Redis's
// lmoveGenericCommand order), then the destination type is checked, the element
// is pushed onto the destination end, and finally removed from the source end,
// dropping the source key if it emptied. srcLeft picks the source end the pop
// takes (head when true, tail when false); dstLeft picks the destination end.
// src and dst are distinct keys on distinct shards by the dispatch check; the
// same-key rotation cannot reach here, since one key is one shard.
func LmoveCross(t *shard.Txn, src, dst []byte, srcLeft, dstLeft bool) []byte {
	var srcWrong, dstWrong, have, moved bool
	var elem []byte
	t.Do(src, func(cx *shard.Ctx) {
		s, w := registry(cx).lookup(cx, src)
		if w {
			srcWrong = true
			return
		}
		// A missing or empty source moves nothing and replies the null bulk. Like
		// the co-located lmove and Redis, we stop here before the destination type
		// is ever consulted, so a wrong-typed destination behind a missing source is
		// never reported. The empty case cannot arise (an emptied list is dropped
		// from the registry), but the length guard mirrors the co-located branch.
		if s == nil || s.length() == 0 {
			return
		}
		// Peek the source end and clone the element into heap storage so it survives
		// off this owner goroutine: the destination push runs on another shard, and
		// the source blob it aliases can move under a later write. The co-located
		// lmove got this copy for free from cloneBytes over a pop; here the element
		// is discovered by the read, so the source hop captures it for the push hop.
		elem = cloneBytes(peekEnd(s, srcLeft))
		have = true
	})
	if have && !srcWrong {
		t.Do(dst, func(cx *shard.Ctx) {
			g := registry(cx)
			d, w := g.lookup(cx, dst)
			if w {
				dstWrong = true
				return
			}
			// Push onto the destination first, then remove from the source in a final
			// hop. The transient state is element-in-both, invisible under the
			// barrier, never element-in-neither. smovecross.go makes the identical
			// choice for the same reason; for the list an element in neither list is
			// the phantom-hole analog the P9/L15 lesson warns against (lmove.go). The
			// destination is created on first insert exactly as the push path does; a
			// list is always born listpack and only the byte budget moves it native.
			if d == nil {
				d = newList()
				g.m[string(dst)] = d
			}
			pushEnd(d, elem, dstLeft)
			// Wake any client blocked on the destination, exactly as the co-located
			// lmove distinct-key branch (lmove.go) and the blocking cross move
			// (blockmovecross.go) do. This runs on the dst owner under the barrier, so
			// a BLPOP or BLMPOP parked on dst is served the element right here; a dst
			// with no waiters makes it a no-op, and a kindMove waiter whose own
			// destination is remote takes the serveMoveRemote spawn path. Without this
			// hook a cross LMOVE that pushes onto a key with a parked blocker leaves it
			// hanging, a Redis-parity bug, since the co-located and blocking-cross forms
			// both wake it. The final source pop hop is unchanged, so the transient
			// stays element-in-both.
			serveWaiters(cx, g, dst, d)
			// A serve that consumed the moved element leaves the destination empty, so
			// drop it: a cross LMOVE deletes an emptied destination exactly as the
			// co-located lmove does (lmove.go), keeping EXISTS/TYPE/OBJECT ENCODING in
			// step. g.drop touches only the list index; waiters still parked on dst live
			// in the separate waiter set and stay for a future push.
			if d.length() == 0 {
				g.drop(dst)
			}
			moved = true
		})
	}
	if moved {
		t.Do(src, func(cx *shard.Ctx) {
			g := registry(cx)
			s, _ := g.lookup(cx, src)
			// The peeked end has not moved: the barrier held the source key across
			// every hop, so this pops the same element the source hop cloned. The
			// drop runs after the destination push, so the emptied source is deleted
			// exactly as the co-located lmove deletes it (Redis deletes an emptied
			// list).
			popEnd(s, srcLeft)
			if s.length() == 0 {
				g.drop(src)
			}
		})
	}
	switch {
	case srcWrong || dstWrong:
		return resp.AppendError(nil, wrongType)
	case moved:
		return resp.AppendBulk(nil, elem)
	default:
		return resp.AppendNull(nil)
	}
}

// RpoplpushCross runs RPOPLPUSH source destination on the intent path, the same
// move as LMOVE source destination RIGHT LEFT: pop the source tail and push the
// destination head.
func RpoplpushCross(t *shard.Txn, src, dst []byte) []byte {
	return LmoveCross(t, src, dst, false, true)
}

// ParseDir is the exported form of parseDir for dispatch's cross-shard LMOVE
// closure: left is true for LEFT and false for RIGHT, and ok is false for
// anything else, which the closure answers with SyntaxError.
func ParseDir(tok []byte) (left, ok bool) { return parseDir(tok) }

// SyntaxError is the finished RESP syntax-error reply the cross-shard LMOVE
// closure returns for an invalid direction token, byte-identical to the
// errSyntax reply the co-located Lmove writes through shard.Reply.Err.
func SyntaxError() []byte { return resp.AppendError(nil, errSyntax) }
