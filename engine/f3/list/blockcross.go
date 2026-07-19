package list

import (
	"sync/atomic"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// Cross-shard blocking pops (spec 2064/f3/13 M3 slice 8, PR 6): BLPOP, BRPOP,
// and BLMPOP over keys that span shards. The co-located forms (blocking.go,
// blockmove.go) read every listed key from the one shard the command routed to;
// this file handles the case where the keys land on different owners, which the
// dispatcher sends through DoBlockCross so the command holds an intent on every
// key across the whole serve-or-park decision.
//
// The decision itself is easy under the barrier: DoBlockCross holds all the
// keys, so the ordered immediate-serve scan below reads each key in command
// order and pops from the first non-empty one, exactly the first-non-empty
// priority Redis gives, with no other command able to slip an element in or out
// between the checks. The hard part is parking. A co-located multi-key waiter is
// one owner's problem: the single owner serializes a serving push against the
// timeout, so a plain live flag is enough (waiter.go D2). A cross-shard waiter is
// parked on several owners at once, and a push landing on one owner's key races a
// push landing on another's, or the timeout firing on a third. Exactly one of
// those may serve the client; the rest must find the waiter already taken and
// unlink only their own dead local node.
//
// blockClaim is that arbiter. One heap cell per parked cross wait, shared by
// every owner's node through waitNode.claim, holds a single atomic word. The
// first serve or timeout to CAS it from zero wins and completes the reply; every
// later one loses the CAS and just tears down its local sibling ring. The winner
// then fires an idempotent cancel hop (PostOwner) to each other owner so their
// now-dead nodes leave promptly rather than lingering until their own push or
// timeout. The plain push/pop path and the co-located park path pay nothing new:
// a co-located waiter's claim is nil, so serveKey's prologue is one nil-pointer
// load and the cancel fan-out is skipped.

// blockSite records where one owner parked a cross wait: its shard and the
// sibling-ring head node on that owner, the index a cancel hop or the timeout
// addresses. The owners slice is built on the coordinator goroutine while every
// intent is held and is immutable once the park returns, so a serving push (which
// cannot run on a held key until the coordinator releases) always reads it whole.
type blockSite struct {
	shard int
	head  uint32
}

// blockClaim is the one-winner arbiter a cross-shard multi-key blocking pop
// shares across the owners it parks on. claimed is the only cross-goroutine
// field: a serve or a timeout CASes it from zero to one, and only the winner
// completes the reply. conn and seq are the completion target, and owners lists
// every parked site so the winner can cancel the losers. It is one heap
// allocation per parked cross wait, off the plain push/pop path entirely.
type blockClaim struct {
	claimed atomic.Uint32
	conn    *shard.Conn
	seq     uint32
	owners  []blockSite
}

// tryClaim reports whether the caller is the one serve or timeout allowed to
// complete this wait, taking the claim if so. A false means another owner (a
// racing push, or the timeout) already won and is tearing the waiter down.
func (bc *blockClaim) tryClaim() bool { return bc.claimed.CompareAndSwap(0, 1) }

// fireCancels tells every other owner this wait parked on to drop its now-dead
// local node, the cleanup the winner runs after it completes the reply. It skips
// exceptShard, the winner's own shard, whose node the winner already unlinked.
// Each hop rides PostOwner, so it runs single-owner on the target and is ordered
// after the park's enqueues; cancelBlockNode is idempotent, so a hop that finds
// the node already gone (its own push or timeout raced the cancel and lost the
// claim, then unlinked locally) is a no-op.
func (bc *blockClaim) fireCancels(cx *shard.Ctx, exceptShard int) {
	rt := cx.Runtime()
	if rt == nil {
		return
	}
	for _, site := range bc.owners {
		if site.shard == exceptShard {
			continue
		}
		s := site
		rt.PostOwner(s.shard, func(cx2 *shard.Ctx) { cancelBlockNode(cx2, s.head, bc) })
	}
}

// cancelBlockNode unlinks one owner's dead sibling ring for a cross wait another
// owner served, the target side of fireCancels. It is idempotent: the claim
// guards double completion, and this only reclaims nodes, so a head that is no
// longer live or no longer this claim's (already unlinked, or the slot recycled
// to a different waiter) is left untouched. Owner goroutine only.
func cancelBlockNode(cx *shard.Ctx, head uint32, bc *blockClaim) {
	g := registry(cx)
	if int(head) >= len(g.wpool.nodes) {
		return
	}
	nd := &g.wpool.nodes[head]
	if !nd.live || nd.claim != bc {
		return
	}
	g.unlinkAll(cx, head)
}

// makeCrossFire is the timeout callback for a cross-shard pop wait, the cross
// analog of makeFire (blocking.go). It runs on the owner that carries the timer,
// the first group's ring head. The claim makes it exclusive against a serving
// push on any owner: it takes the claim, and only on success unlinks its own
// node, cancels the other owners, and delivers the null array (BLPOP/BRPOP/BLMPOP
// all time out to *-1). A lost claim means a push already won and its cancel hop
// is on the way, so the timeout just steps aside. The live and claim-identity
// guards keep a fired-then-recycled slot from acting on a stranger's node.
func makeCrossFire(g *reg, head uint32, bc *blockClaim) func(*shard.Ctx) {
	return func(cx *shard.Ctx) {
		nd := &g.wpool.nodes[head]
		if !nd.live || nd.claim != bc {
			return
		}
		nd.timer = nil // the firing timer is off the heap already
		if !bc.tryClaim() {
			return
		}
		conn := nd.conn
		seq := nd.seq
		g.unlinkAll(cx, head)
		bc.fireCancels(cx, cx.ShardID())
		conn.CompleteBlocked(seq, resp.AppendNullArray(nil))
	}
}

// shardGroup is one owner's slice of a cross wait's keys, in the order the keys
// were listed, so a single t.Do hop to grp.keys[0] parks the whole group's
// sibling nodes on that owner in one step.
type shardGroup struct {
	shard int
	keys  [][]byte
}

// groupByShardInOrder buckets keys by owning shard, preserving the first-seen
// order of both the shards and the keys within a shard. Each group parks as one
// hop on its owner, and the group order fixes which owner carries the timeout
// (the first). Duplicate keys stay in their group and park twice on the same
// list, which the sibling unlink cleans up, so the caller need not dedupe.
func groupByShardInOrder(t *shard.Txn, keys [][]byte) []shardGroup {
	var groups []shardGroup
	for _, k := range keys {
		sh := t.Shard(k)
		idx := -1
		for i := range groups {
			if groups[i].shard == sh {
				idx = i
				break
			}
		}
		if idx < 0 {
			groups = append(groups, shardGroup{shard: sh, keys: [][]byte{k}})
		} else {
			groups[idx].keys = append(groups[idx].keys, k)
		}
	}
	return groups
}

// parkPopCross parks one cross-shard pop waiter: a fresh blockClaim shared by
// every owner, one sibling ring per owner, and one timer on the first owner when
// the timeout is finite. It runs on the coordinator goroutine with every intent
// held, so a serving push (which defers on a held key until release) cannot read
// bc.owners half-built. The timeout is the exception: it fires from the owner's
// fireTimers step, which the held intents do not gate, so an armed timer can run
// while this function is still building the slice. So the timer is not armed
// until a second pass, after the first pass has finished bc.owners: the callback
// reads a complete, then-immutable slice, never one an append is still growing.
func parkPopCross(t *shard.Txn, conn *shard.Conn, seq uint32, keys [][]byte, spec waitSpec, timeout float64) {
	bc := &blockClaim{conn: conn, seq: seq}
	spec.claim = bc
	groups := groupByShardInOrder(t, keys)
	// Pass 1: park every owner's sibling ring, no timer yet, so bc.owners is
	// finished before anything that reads it can exist.
	for _, grp := range groups {
		grp := grp
		var head uint32
		t.Do(grp.keys[0], func(cx *shard.Ctx) {
			g := registry(cx)
			head = parkWaiter(g, grp.keys, spec, conn, seq)
		})
		bc.owners = append(bc.owners, blockSite{shard: grp.shard, head: head})
	}
	// Pass 2: with bc.owners final, arm the timeout on the first owner. The arm
	// runs on that owner and the fire runs on the same owner later, so the
	// callback sees the completed slice through the arm's ordering. The live and
	// claim guards skip a node a push served between the passes.
	if timeout > 0 && len(groups) > 0 {
		head0 := bc.owners[0].head
		t.Do(groups[0].keys[0], func(cx *shard.Ctx) {
			g := registry(cx)
			nd := &g.wpool.nodes[head0]
			if !nd.live || nd.claim != bc {
				return
			}
			deadline := cx.NowMs + int64(timeout*1000)
			nd.timer = cx.ArmTimer(deadline, makeCrossFire(g, head0, bc))
		})
	}
}

// blockPopCross runs the cross-shard BLPOP/BRPOP core: parse the timeout, scan
// the keys in order under the barrier and serve the first non-empty one, and on
// an all-empty set park a kindPop waiter on every owner and return nil so the
// reply stays open for a serving push or the timeout. front picks the pop end,
// the head for BLPOP and the tail for BRPOP.
func blockPopCross(t *shard.Txn, conn *shard.Conn, seq uint32, keys [][]byte, timeoutArg []byte, front bool) []byte {
	timeout, ok := parseTimeout(timeoutArg)
	if !ok {
		return resp.AppendError(nil, errTimeoutFloat)
	}
	if timeout < 0 {
		return resp.AppendError(nil, errTimeoutNeg)
	}
	for _, key := range keys {
		var out []byte
		var wrong, have bool
		t.Do(key, func(cx *shard.Ctx) {
			g := registry(cx)
			l, w := g.lookup(cx, key)
			if w {
				wrong = true
				return
			}
			if l == nil || l.length() == 0 {
				return
			}
			out = appendReply(nil, key, popOne(l, front))
			cx.NotifyKeyspaceEvent(shard.NotifyList, popEvent(front), key)
			if l.length() == 0 {
				g.drop(key)
				cx.NotifyKeyspaceEvent(shard.NotifyGeneric, "del", key)
			} else {
				g.note(l)
			}
			have = true
		})
		if wrong {
			return resp.AppendError(nil, wrongType)
		}
		if have {
			return out
		}
	}
	parkPopCross(t, conn, seq, keys, waitSpec{kind: kindPop, front: front}, timeout)
	return nil
}

// BlpopCross answers a cross-shard BLPOP through DoBlockCross: a is the argument
// tail, the listed keys then the trailing timeout.
func BlpopCross(t *shard.Txn, conn *shard.Conn, seq uint32, a [][]byte) []byte {
	return blockPopCross(t, conn, seq, a[:len(a)-1], a[len(a)-1], true)
}

// BrpopCross answers a cross-shard BRPOP: BlpopCross popping the tail.
func BrpopCross(t *shard.Txn, conn *shard.Conn, seq uint32, a [][]byte) []byte {
	return blockPopCross(t, conn, seq, a[:len(a)-1], a[len(a)-1], false)
}

// BlmpopCross answers a cross-shard BLMPOP: a is the argument tail, the leading
// timeout then the numkeys/keys/direction/COUNT run BLMPOP shares with LMPOP. It
// serves the first non-empty key up to its count under the barrier, or parks a
// kindMpop waiter on every owner carrying the pop end and count. A malformed tail
// cannot reach here: BlmpopKeys returns nil for it, so the dispatcher keeps the
// command on the point path, which answers the parse error in place.
func BlmpopCross(t *shard.Txn, conn *shard.Conn, seq uint32, a [][]byte) []byte {
	timeout, ok := parseTimeout(a[0])
	if !ok {
		return resp.AppendError(nil, errTimeoutFloat)
	}
	if timeout < 0 {
		return resp.AppendError(nil, errTimeoutNeg)
	}
	keys, front, count, emsg := parseLmpopTail(a[1:])
	if emsg != "" {
		return resp.AppendError(nil, emsg)
	}
	for _, key := range keys {
		var out []byte
		var wrong, have bool
		t.Do(key, func(cx *shard.Ctx) {
			g := registry(cx)
			l, w := g.lookup(cx, key)
			if w {
				wrong = true
				return
			}
			if l == nil || l.length() == 0 {
				return
			}
			npop := count
			if npop > l.length() {
				npop = l.length()
			}
			out = resp.AppendArrayHeader(nil, 2)
			out = resp.AppendBulk(out, key)
			out = resp.AppendArrayHeader(out, npop)
			for j := 0; j < npop; j++ {
				out = resp.AppendBulk(out, popOne(l, front))
			}
			cx.NotifyKeyspaceEvent(shard.NotifyList, popEvent(front), key)
			if l.length() == 0 {
				g.drop(key)
				cx.NotifyKeyspaceEvent(shard.NotifyGeneric, "del", key)
			} else {
				g.note(l)
			}
			have = true
		})
		if wrong {
			return resp.AppendError(nil, wrongType)
		}
		if have {
			return out
		}
	}
	parkPopCross(t, conn, seq, keys, waitSpec{kind: kindMpop, front: front, count: count}, timeout)
	return nil
}

// BlpopKeys extracts the listed keys from a BLPOP/BRPOP argument tail for the
// co-location check and the intent list: every argument but the trailing
// timeout. It returns nil for a tail too short to hold a key and a timeout, which
// keeps the command on the point path.
func BlpopKeys(a [][]byte) [][]byte {
	if len(a) < 2 {
		return nil
	}
	return a[:len(a)-1]
}

// BlmpopKeys extracts the listed keys from a BLMPOP argument tail (the leading
// timeout then the LMPOP tail) for the co-location check and the intent list. A
// malformed tail returns nil, so the dispatcher keeps the command on the point
// path and its handler answers the parse error.
func BlmpopKeys(a [][]byte) [][]byte {
	if len(a) < 1 {
		return nil
	}
	keys, _, _, emsg := parseLmpopTail(a[1:])
	if emsg != "" {
		return nil
	}
	return keys
}
