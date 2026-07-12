package list

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The blocking-waiter set (spec 2064/f3/13 M3 slice 8, lab 03's frozen verdict).
// A connection that BLPOP/BRPOP-blocks on one or more keys parks a waitNode on
// each key's FIFO list. The nodes live in one per-shard slab (waitPool) addressed
// by index, so park, wake, and timeout-unlink are each O(1) with no per-waiter
// allocation once the slab has grown, and a multi-key waiter's nodes are stitched
// into a circular sibling ring so serving on one key unlinks the waiter from
// every other key in one walk. Everything here is single-owner state: only the
// shard goroutine that owns the key touches it, so there are no locks and no
// atomics, and the single-shard slice needs no atomic claim because the one owner
// serializes serve against timeout (D2). The cross-shard atomic claim is a later
// slice.

// nilIdx is the sentinel index for an absent link, the arena's version of a nil
// pointer. A real node index is a slab offset and never equals it.
const nilIdx = ^uint32(0)

// The waiter kind discriminates the three blocking shapes a parked node can
// carry, so one serve loop can complete any of them off the same key's FIFO.
// kindPop is BLPOP/BRPOP: pop one element and reply [key, element]. kindMove is
// BLMOVE/BRPOPLPUSH: move one element to a destination end and reply the moved
// bulk. kindMpop is BLMPOP: pop up to count elements off one end and reply
// [key, [elem, ...]]. The push-side serve reads the kind off whichever sibling
// node is the served key's list head and branches on it.
const (
	kindPop  uint8 = 0
	kindMove uint8 = 1
	kindMpop uint8 = 2
)

// waitNode is one connection's parked interest in one key. prev and next are the
// intrusive links within that key's FIFO list; sib is the circular ring across
// the keys of a single multi-key waiter (a one-key waiter's sib points at
// itself). wl is the list this node lives on, so a sibling walk can unlink each
// node in O(1) without re-finding its key. conn and seq are the deferred-reply
// target the handler captured through CurConn and CurSeq. front is true for
// BLPOP (pop the served key's head) and false for BRPOP (pop its tail). timer is
// the armed timeout, nil when the waiter blocks forever; it is set on the
// sibling-ring head only. live is the idempotency guard that keeps a serve and a
// timeout from both firing the same waiter.
//
// kind, count, dstKey, and dstLeft carry the per-shape parameters the serve reads
// off the list head. count is the BLMPOP element budget for one wake; dstKey and
// dstLeft are the BLMOVE destination key and push end. park writes every one of
// them on each call, since a recycled node holds a prior waiter's stale values (a
// node that last served a kindMove still holds its dstKey until overwritten).
// claim and serving carry the cross-shard coordination a co-located waiter never
// needs (blockcross.go). claim is the shared one-winner arbiter for a wait parked
// across several owners, nil for a co-located wait, so the serve prologue's guard
// is one nil-pointer load on the common path. serving marks a cross BLMOVE whose
// remote destination hop is in flight on a spawned coordinator, so a second push
// on the source does not launch a duplicate; it is owner-local, cleared or
// unlinked back on the source owner.
type waitNode struct {
	prev, next uint32
	sib        uint32
	wl         *waitList
	conn       *shard.Conn
	seq        uint32
	timer      shard.TimerHandle
	claim      *blockClaim
	kind       uint8
	front      bool
	dstLeft    bool
	live       bool
	serving    bool
	count      int
	dstKey     string
}

// waitSpec is the by-value bundle of one waiter's per-shape parameters, threaded
// through park so every sibling node of a multi-key waiter carries the same kind,
// end, count, and destination. It is stack-copied into each node, so a kindPop
// spec (empty dstKey, zero count) parks with no allocation while a kindMove spec
// pays only the one dstKey string copy the handler already made.
type waitSpec struct {
	kind    uint8
	front   bool
	count   int
	dstKey  string
	dstLeft bool
	claim   *blockClaim
}

// waitPool is the per-shard node slab. nodes grows once to its working size and
// then holds steady; free is the recycle stack a released node returns to, so a
// warm park reuses a slot and allocates nothing. It hangs off the registry as a
// value, so &g.wpool is a stable pointer every waitList keeps.
type waitPool struct {
	nodes []waitNode
	free  []uint32
}

// alloc returns a free node index, growing the slab only when the recycle stack
// is empty. The returned node's fields are stale and the caller sets every one.
func (p *waitPool) alloc() uint32 {
	if n := len(p.free); n > 0 {
		i := p.free[n-1]
		p.free = p.free[:n-1]
		return i
	}
	i := uint32(len(p.nodes))
	p.nodes = append(p.nodes, waitNode{})
	return i
}

// release returns a detached node to the recycle stack.
func (p *waitPool) release(i uint32) { p.free = append(p.free, i) }

// waitList is one key's FIFO of parked waiters, an intrusive doubly linked list
// over the shared pool. head is the oldest waiter (served first), tail the
// newest. key is kept so an emptied list can drop itself from the registry map
// without the caller re-deriving the key from a node.
type waitList struct {
	pool       *waitPool
	key        string
	head, tail uint32
	n          int
}

// park appends a new waiter to the tail and returns its node index, the FIFO
// order Redis serves blocked clients in. The caller fills the sibling ring; park
// sets the list links and every reply-shape field from spec. It writes kind,
// front, count, dstKey, and dstLeft on every call, never skipping one, because
// the node may be recycled from a prior waiter of a different kind whose stale
// fields would otherwise leak (dstKey="" is a header write, not an alloc, so a
// kindPop park stays zero-alloc).
func (l *waitList) park(spec waitSpec, c *shard.Conn, seq uint32) uint32 {
	i := l.pool.alloc()
	nd := &l.pool.nodes[i]
	nd.wl = l
	nd.conn = c
	nd.seq = seq
	nd.kind = spec.kind
	nd.front = spec.front
	nd.count = spec.count
	nd.dstKey = spec.dstKey
	nd.dstLeft = spec.dstLeft
	nd.claim = spec.claim
	nd.timer = nil
	nd.live = true
	nd.serving = false
	nd.prev = l.tail
	nd.next = nilIdx
	nd.sib = i
	if l.tail == nilIdx {
		l.head = i
	} else {
		l.pool.nodes[l.tail].next = i
	}
	l.tail = i
	l.n++
	return i
}

// unlink removes node i from its list in O(1), splicing its neighbours. It
// touches only the list links and the count; marking the node dead and returning
// it to the pool is the caller's job, so unlink can serve both a served head and
// a timed-out middle node.
func (l *waitList) unlink(i uint32) {
	nd := &l.pool.nodes[i]
	if nd.prev == nilIdx {
		l.head = nd.next
	} else {
		l.pool.nodes[nd.prev].next = nd.next
	}
	if nd.next == nilIdx {
		l.tail = nd.prev
	} else {
		l.pool.nodes[nd.next].prev = nd.prev
	}
	l.n--
}

// peekHead returns the oldest waiter's index, or false when the list is empty.
func (l *waitList) peekHead() (uint32, bool) {
	if l.head == nilIdx {
		return 0, false
	}
	return l.head, true
}

// parkWaiter parks one connection on every key it blocks on and returns the
// sibling-ring head index, the node the caller arms the timeout on. The nodes of
// one waiter are stitched into a circular ring through sib so a later serve on
// any one key can walk to and unlink all of them. Duplicate keys park twice on
// the same list, which the sibling unlink cleans up, so the caller need not
// dedupe.
func parkWaiter(g *reg, keys [][]byte, spec waitSpec, c *shard.Conn, seq uint32) uint32 {
	first := nilIdx
	prev := nilIdx
	for _, key := range keys {
		wl := g.waitListFor(key)
		i := wl.park(spec, c, seq)
		if first == nilIdx {
			first = i
		} else {
			g.wpool.nodes[prev].sib = i
		}
		prev = i
	}
	g.wpool.nodes[prev].sib = first
	return first
}

// unlinkAll removes a whole multi-key waiter from every key it parked on: it
// walks the sibling ring from idx, unlinks each node, cancels the armed timeout
// if any node carries it, drops any list it empties, and recycles the nodes. It
// is the shared teardown for both a serve and a timeout, so a waiter leaves all
// of its lists in one pass. cx may be nil in a unit test that arms no timer,
// which is safe because CancelTimer only runs for a non-nil handle.
func (g *reg) unlinkAll(cx *shard.Ctx, idx uint32) {
	j := idx
	for {
		nd := &g.wpool.nodes[j]
		next := nd.sib
		wl := nd.wl
		nd.live = false
		if nd.timer != nil {
			cx.CancelTimer(nd.timer)
			nd.timer = nil
		}
		wl.unlink(j)
		g.dropWaitersIfEmpty(wl)
		g.wpool.release(j)
		if next == idx {
			return
		}
		j = next
	}
}

// serveWaiters hands freshly pushed elements to the clients blocked on key, in
// waiter FIFO order, until either the list empties or no waiter remains. It runs
// on the owner from the push handler (and the LMOVE destination hook), after the
// elements are in the list. Each waiter's reply is built into its own fresh
// buffer because CompleteBlocked copies it and the next waiter reuses none of it.
//
// serveKey drains the pushed key. A served BLMOVE whose destination is a distinct
// key pushes onto that key and queues it on g.ready, since its own blocked clients
// may now be servable. The worklist drain then serves each queued key in turn,
// which may queue further keys, so a chain A->B->C or a ping-pong A<->B is served
// to completion in this one call. Termination is bounded: each kindMove serve
// unlinks exactly one waiter, so the strictly decreasing parked-waiter count caps
// the total moves, and a key is queued at most once per served move, so the total
// pushes never exceed the number of parked waiters. The drain is an explicit LIFO
// stack (O(1) depth, no recursion), and g.ready is truncated back to empty at the
// end so a plain push that served no move keeps the slice nil and allocates none.
func serveWaiters(cx *shard.Ctx, g *reg, key []byte, l *list) {
	serveKey(cx, g, key, l)
	for len(g.ready) > 0 {
		k := g.ready[len(g.ready)-1]
		g.ready = g.ready[:len(g.ready)-1]
		l2 := g.m[k]
		if l2 == nil {
			continue
		}
		serveKey(cx, g, []byte(k), l2)
		if l2.length() == 0 {
			delete(g.m, k)
		}
	}
	if cap(g.ready) > 0 {
		g.ready = g.ready[:0]
	}
}

// serveKey serves the waiters parked on one key against its list, in FIFO order,
// until the list empties or no waiter remains. It branches on each head waiter's
// kind: a BLPOP/BRPOP pops one element and replies [key, element]; a BLMPOP pops
// up to its recorded count off its end and replies [key, [elem, ...]], consuming
// only its own budget so the next waiter is served from what is left; a BLMOVE is
// handed to serveMove, which pushes to the destination and may queue a chained
// key. Every reply lands at the waiter's parked sequence through CompleteBlocked.
func serveKey(cx *shard.Ctx, g *reg, key []byte, l *list) {
	wl := g.waiters[string(key)]
	if wl == nil {
		return
	}
	for wl.n > 0 && l.length() > 0 {
		i, ok := wl.peekHead()
		if !ok {
			return
		}
		nd := &g.wpool.nodes[i]
		switch nd.kind {
		case kindPop:
			// A cross-shard waiter (claim set) must win its shared claim before it
			// serves: a lost claim means a racing push on another owner, or the
			// timeout, already took this client, so drop only this dead local ring
			// and move to the next waiter. A co-located waiter (nil claim) skips the
			// CAS entirely and pays one nil-pointer load.
			if nd.claim != nil && !nd.claim.tryClaim() {
				g.unlinkAll(cx, i)
				continue
			}
			conn := nd.conn
			seq := nd.seq
			bc := nd.claim
			rep := appendReply(nil, key, popOne(l, nd.front))
			g.unlinkAll(cx, i)
			if bc != nil {
				bc.fireCancels(cx, cx.ShardID())
			}
			conn.CompleteBlocked(seq, rep)
		case kindMpop:
			if nd.claim != nil && !nd.claim.tryClaim() {
				g.unlinkAll(cx, i)
				continue
			}
			conn := nd.conn
			seq := nd.seq
			bc := nd.claim
			front := nd.front
			npop := nd.count
			if npop > l.length() {
				npop = l.length()
			}
			rep := resp.AppendArrayHeader(nil, 2)
			rep = resp.AppendBulk(rep, key)
			rep = resp.AppendArrayHeader(rep, npop)
			for j := 0; j < npop; j++ {
				rep = resp.AppendBulk(rep, popOne(l, front))
			}
			g.unlinkAll(cx, i)
			if bc != nil {
				bc.fireCancels(cx, cx.ShardID())
			}
			conn.CompleteBlocked(seq, rep)
		default: // kindMove
			serveMove(cx, g, key, l, i, nd)
		}
	}
}

// serveMove completes one blocked BLMOVE/BRPOPLPUSH waiter parked on key: pop the
// source element off the waiter's end and push it onto its recorded destination
// end, then reply the moved element as a bulk string. It is the deferred twin of
// lmove()'s core, run when a push finally makes the source non-empty. Every
// per-waiter field is read off the node before unlinkAll recycles it.
//
// A self-move (destination equals the served key) pushes the popped element back
// onto the same list and is never queued: the enclosing serveKey loop re-observes
// the still non-empty list on its own. A distinct destination's type is checked
// first, at serve, not at park, the way Redis defers it: a wrong-typed destination
// fails the client with WRONGTYPE and leaves the source element in place, because
// the pop runs only after the check passes. Otherwise the element is cloned out
// before the push (it aliases the source's chunk storage), the destination is
// created on first insert, and its key is queued on g.ready so its own waiters are
// served by the drain.
func serveMove(cx *shard.Ctx, g *reg, key []byte, l *list, i uint32, nd *waitNode) {
	conn := nd.conn
	seq := nd.seq
	front := nd.front
	dstKey := nd.dstKey
	dstLeft := nd.dstLeft
	self := dstKey == string(key)
	var dst *list
	if !self {
		d, wrong := g.lookup(cx, []byte(dstKey))
		if wrong {
			g.unlinkAll(cx, i)
			conn.CompleteBlocked(seq, resp.AppendError(nil, wrongType))
			return
		}
		dst = d
	}
	elem := cloneBytes(popOne(l, front))
	if self {
		pushEnd(l, elem, dstLeft)
	} else {
		if dst == nil {
			dst = newList()
			g.m[dstKey] = dst
		}
		pushEnd(dst, elem, dstLeft)
		g.ready = append(g.ready, dstKey)
	}
	g.unlinkAll(cx, i)
	conn.CompleteBlocked(seq, resp.AppendBulk(nil, elem))
}
