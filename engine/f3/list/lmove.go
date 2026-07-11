package list

import (
	"github.com/tamnd/aki/engine/f3/shard"
)

// LMOVE and RPOPLPUSH, the list family's same-shard two-key move (spec
// 2064/f3/13 section 5 and the M3 slice 6 line, "same-shard RPOPLPUSH/LMOVE as
// two O(1) end ops in one owner step"). LMOVE source destination <LEFT|RIGHT>
// <LEFT|RIGHT> pops one element off the source end (its head when the source
// side is LEFT, its tail when RIGHT) and pushes it onto the destination end (its
// head when the destination side is LEFT, its tail when RIGHT), replying the
// moved element as a bulk string. RPOPLPUSH source destination is the older
// spelling of LMOVE source destination RIGHT LEFT.
//
// The doc's cross-shard plan is the F17 intent path: acquire intents on both
// keys in ascending shard order, pop at the source's owner, push at the
// destination's owner, and reply under the barrier so no other command
// interleaves between the pop and the push. That substrate is not built for the
// list yet, so this slice ships the co-located form the way SMOVE first shipped
// (set/smove.go): the command routes to one shard and reads both keys from that
// owner's registry, which is correct while source and destination are co-located
// (the common case, and the only case a hash tag can force). A source and
// destination that hash to different shards need the intent path, and until that
// lands (slice 7, cross-shard LMOVE on the F17 intent pair) LMOVE assumes
// co-located keys, recorded honestly here rather than papered over with
// machinery this slice does not own. When the intent path is built, Lmove
// becomes its owner's pop-at-source / push-at-destination step and this local
// form stays the single-shard fast path doc 03 section 6.1 calls free.
//
// Atomicity in the co-located form is the owner goroutine itself: the whole move
// runs on one shard's single worker with no yield, so no other command observes
// a state where the element is in neither list. That is the same guarantee the
// intent barrier buys across shards, provided here for free by single ownership
// (F1).
//
// This slice also carries the P9 ordered-commit lesson (spec 2064/f3/19 the P9
// row, 01 section on L15, 03 section on the push window). Under single ownership
// the v1 multi-writer reservation machinery deflates to a plain deque append, so
// the push here is just pushFront/pushBack on the owner's list. What survives is
// the discipline the lesson names: reserve-then-fill without an ordered commit
// exposes phantom holes (L15), so the move publishes the element as one ordered
// step, never a half-filled slot. The durability-log append that discipline
// becomes the append rule for is M8's home (engine/f3/akifile is a doc.go stub
// today), so this slice guards the move's ordered publication with a regression
// test (lmove_test.go TestMovePhantomHoleOrderedCommit) and leaves the log
// append to M8.

// lmove runs the LMOVE core on the local registry. moved is the element that
// changed lists, ok is true when a move happened, and wrong reports a WRONGTYPE.
// srcLeft picks the source end the pop takes (head when true, tail when false);
// dstLeft picks the destination end the push adds to.
//
// The order matches Redis's lmoveGenericCommand exactly: the source type is
// checked first, then a missing or empty source replies the null bulk without
// consulting the destination at all, then the destination type is checked. So a
// wrong-typed destination behind a missing source is never reported (Redis
// replies nil there), and whenever a move actually happens both types were
// validated before any mutation, so a wrong-typed pair never half-moves.
func lmove(g *reg, cx *shard.Ctx, srcKey, dstKey []byte, srcLeft, dstLeft bool) (moved []byte, ok, wrong bool) {
	src, w := g.lookup(cx, srcKey)
	if w {
		return nil, false, true
	}
	// A missing or empty source moves nothing and replies the null bulk. Redis
	// stops here before it looks at the destination, so the destination type is
	// not checked in this branch. The empty case cannot arise through this engine
	// (an emptied list is dropped from the registry), but the length guard mirrors
	// Redis's defensive branch.
	if src == nil || src.length() == 0 {
		return nil, false, false
	}
	dst, w := g.lookup(cx, dstKey)
	if w {
		return nil, false, true
	}
	// The popped bytes alias the source's internal chunk storage, which the push
	// can overwrite (the same-key case pushes back onto this very blob) or a
	// promotion can reallocate. Copy the element out before any push touches the
	// storage it points into.
	v := cloneBytes(popEnd(src, srcLeft))
	// Same key (this covers RPOPLPUSH key key and every LMOVE key key rotation):
	// the pop and the push land on one list object, so push the copied value back
	// onto it and never drop the key, since the list still holds the element.
	if string(srcKey) == string(dstKey) {
		pushEnd(src, v, dstLeft)
		return v, true, false
	}
	// Create the destination on first insert exactly as the push path does; a list
	// is always born listpack and only the byte budget moves it to the native band.
	if dst == nil {
		dst = newList()
		g.m[string(dstKey)] = dst
	}
	pushEnd(dst, v, dstLeft)
	// The pop emptied the source: Redis deletes an emptied list. The drop runs
	// after the destination push so distinct keys never race, and the same-key
	// case above already returned, so this never drops a key just pushed onto.
	if src.length() == 0 {
		g.drop(srcKey)
	}
	return v, true, false
}

// popEnd pops the head when left is true and the tail otherwise. The list must
// be non-empty.
func popEnd(l *list, left bool) []byte {
	if left {
		return l.popFront()
	}
	return l.popBack()
}

// pushEnd pushes onto the head when left is true and the tail otherwise.
func pushEnd(l *list, v []byte, left bool) {
	if left {
		l.pushFront(v)
		return
	}
	l.pushBack(v)
}

// parseDir reads an LMOVE direction token. left is true for LEFT and false for
// RIGHT; ok is false for anything else, which the caller answers as a syntax
// error. The token is case-insensitive, matching Redis's getListPositionFromObject.
func parseDir(tok []byte) (left, ok bool) {
	switch {
	case eqFold(tok, "LEFT"):
		return true, true
	case eqFold(tok, "RIGHT"):
		return false, true
	default:
		return false, false
	}
}

// moveReply runs the core move and writes its reply: WRONGTYPE on a string key,
// the null bulk when nothing moved, or the moved element as a bulk string.
func moveReply(cx *shard.Ctx, srcKey, dstKey []byte, srcLeft, dstLeft bool, r shard.Reply) {
	g := registry(cx)
	moved, ok, wrong := lmove(g, cx, srcKey, dstKey, srcLeft, dstLeft)
	if wrong {
		r.Err(wrongType)
		return
	}
	if !ok {
		r.Null()
		return
	}
	r.Bulk(moved)
}

// Lmove answers LMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT>: pop from the
// source end and push onto the destination end, replying the moved element or a
// null bulk when the source is missing or empty. The direction tokens are parsed
// first, so an invalid one is a syntax error before any key is touched, matching
// Redis's argument order.
func Lmove(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	from, ok1 := parseDir(args[2])
	to, ok2 := parseDir(args[3])
	if !ok1 || !ok2 {
		r.Err(errSyntax)
		return
	}
	moveReply(cx, args[0], args[1], from, to, r)
}

// Rpoplpush answers RPOPLPUSH source destination: pop the source tail and push
// onto the destination head, the same move as LMOVE source destination RIGHT
// LEFT.
func Rpoplpush(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	moveReply(cx, args[0], args[1], false, true, r)
}
