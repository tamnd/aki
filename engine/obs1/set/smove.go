package set

import (
	"github.com/tamnd/aki/engine/obs1/shard"
)

// SMOVE, the set family's smallest F17 tier-two plan (spec 2064/f3/11 section 9.2,
// spec 2064/f3/03 sections 6.1 and 6.7). SMOVE source destination member removes
// member from source and adds it to destination, replying 1 when it moved and 0
// when member was not in source (a missing source included). The doc's plan is:
// acquire intents on both keys in ascending shard order, remove at source's owner,
// add at destination's owner, release, reply, with the intent barrier the only
// synchronization so no other command interleaves between the remove and the add.
//
// The tier-two intent substrate (the txnTicket, the per-key VLL intent queues, the
// at-head barrier) is not built yet: M0 landed only the tier-one fan-out (fan.go),
// and no cross-key-atomic command (RENAME, COPY, the STORE forms, LMOVE) has an
// intent path in the tree. So this slice implements SMOVE the way the read-side
// algebra (algebra_commands.go) and the STORE forms (setstore.go) already do: the
// command routes to one shard and reads both keys from that owner's registry, which
// is correct while source and destination are co-located (the common case, and the
// only case hash tags can force). A source and destination that hash to different
// shards need the F17 intent path, and until that substrate lands SMOVE assumes
// co-located keys, recorded honestly here rather than papered over with machinery
// this slice does not own. When the intent path is built, Smove becomes its owner's
// remove-at-source / add-at-destination step and this local form stays the
// single-shard fast path the doc calls free (03 section 6.1).
//
// Atomicity in the co-located form is the owner goroutine itself: the whole move
// runs on one shard's single worker with no yield, so no other command observes a
// state where member is in neither set. That is the same guarantee the intent
// barrier buys across shards, provided here for free by single ownership (F1).

// moveFx records what a move actually mutated, which is what the log frames:
// the same-key case replies 1 without touching anything (srcChanged false, no
// frames), and moving onto a destination that already held the member changes
// only the source (dstChanged false, an srem-only emission).
type moveFx struct {
	srcChanged bool // member left the source
	srcDropped bool // the source emptied and was deleted
	dstCreated bool // the destination was created for this insert
	dstChanged bool // member joined the destination
}

// smove runs the SMOVE core on the local registry. moved is true when member left
// source for destination (reply 1) and false when member was not in source (reply
// 0). wrong reports a WRONGTYPE on either key. Both types are checked before any
// mutation, so a wrong-typed pair never leaves a half-done move, matching Redis's
// up-front type check on both keys.
func smove(g *reg, cx *shard.Ctx, srcKey, dstKey, member []byte) (moved, wrong bool, fx moveFx) {
	src, w := g.lookup(cx, srcKey)
	if w {
		return false, true, fx
	}
	dst, w := g.lookup(cx, dstKey)
	if w {
		return false, true, fx
	}
	// Source and destination are the same key: nothing moves, and the reply is
	// membership alone (doc 11 section 9.2, "moving onto an existing dst member is
	// a remove-only", degenerate to a no-op when the two keys are one). This also
	// covers the both-missing case, which replies 0.
	if string(srcKey) == string(dstKey) {
		return src != nil && src.has(member), false, fx
	}
	// A missing source, or a member not in source, is a no-op that replies 0. The
	// remove happens here so the reply and the mutation share one probe.
	if src == nil || !src.rem(member) {
		return false, false, fx
	}
	fx.srcChanged = true
	// The last member left source: Redis deletes an emptied set (doc 11 section
	// 9.2). Dropping it before the destination insert keeps the invariant that the
	// registry never holds an empty set.
	if src.card() == 0 {
		g.drop(srcKey)
		fx.srcDropped = true
	} else {
		g.note(src)
	}
	// Create the destination on first insert, its band chosen from the member's
	// shape exactly as SADD's create path does (an integer member opens an intset);
	// the insert then follows the normal one-way ladder through add, so a member
	// that breaches the destination's band cap converts it upward in place.
	if dst == nil {
		dst = newSet(member)
		g.m[string(dstKey)] = dst
		fx.dstCreated = true
	}
	fx.dstChanged = dst.add(member)
	g.note(dst)
	return true, false, fx
}

// Smove answers SMOVE source destination member: move member from source to
// destination, reply 1 when it moved and 0 when member was not in source. A key
// holding a string on either side answers WRONGTYPE before anything is written.
func Smove(cx *shard.Ctx, args [][]byte, r shard.Reply) {
	g := registry(cx)
	moved, wrong, fx := smove(g, cx, args[0], args[1], args[2])
	if wrong {
		r.Err(wrongType)
		return
	}
	// The co-located move frames as one atomic run when both sides changed;
	// a destination that already held the member frames srem-only. The
	// same-key reply and the not-in-source reply mutated nothing and frame
	// nothing.
	if fx.srcChanged {
		var err error
		if fx.dstChanged {
			err = cx.LogSetMove(args[0], args[1], args[2], fx.srcDropped, fx.dstCreated)
		} else {
			err = cx.LogSetRem(args[0], [][]byte{args[2]}, fx.srcDropped)
		}
		if err != nil {
			r.Err(err.Error())
			return
		}
	}
	if moved {
		r.Int(1)
		return
	}
	r.Int(0)
}
