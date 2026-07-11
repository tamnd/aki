package list

import "github.com/tamnd/aki/engine/f3/shard"

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
type waitNode struct {
	prev, next uint32
	sib        uint32
	wl         *waitList
	conn       *shard.Conn
	seq        uint32
	timer      shard.TimerHandle
	front      bool
	live       bool
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
// order Redis serves blocked clients in. The caller fills conn, seq, front, and
// the sibling ring; park sets the list links and the reply-shape fields it was
// given.
func (l *waitList) park(front bool, c *shard.Conn, seq uint32) uint32 {
	i := l.pool.alloc()
	nd := &l.pool.nodes[i]
	nd.wl = l
	nd.conn = c
	nd.seq = seq
	nd.front = front
	nd.timer = nil
	nd.live = true
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
func parkWaiter(g *reg, keys [][]byte, front bool, c *shard.Conn, seq uint32) uint32 {
	first := nilIdx
	prev := nilIdx
	for _, key := range keys {
		wl := g.waitListFor(key)
		i := wl.park(front, c, seq)
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
// waiter FIFO order, until either the list empties or no waiter remains. Each
// served waiter pops from its own chosen end (front for BLPOP, back for BRPOP)
// and receives a two-element [key, element] array delivered at its parked
// sequence through CompleteBlocked, the deferred-reply seam. It runs on the
// owner from the push handler, after the elements are in the list, and each
// waiter's reply is built into its own fresh buffer because CompleteBlocked
// copies it and the next waiter reuses none of it.
func serveWaiters(cx *shard.Ctx, g *reg, key []byte, l *list) {
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
		conn := nd.conn
		seq := nd.seq
		var elem []byte
		if nd.front {
			elem = l.popFront()
		} else {
			elem = l.popBack()
		}
		rep := appendReply(nil, key, elem)
		g.unlinkAll(cx, i)
		conn.CompleteBlocked(seq, rep)
	}
}
