package stream

import (
	"github.com/tamnd/aki/engine/obs1/shard"
)

// The blocking-XREAD waiter set (spec 2064/f3/14 section 6.4). A connection that
// XREAD ... BLOCK-blocks on one or more streams parks a waitNode on each key's
// FIFO list. The nodes live in one per-shard slab (waitPool) addressed by index,
// so park, wake, and timeout-unlink are each O(1) with no per-waiter allocation
// once the slab has grown, and a multi-key waiter's nodes are stitched into a
// circular sibling ring so serving on one key unlinks the waiter from every key
// it parked on in one walk. Everything here is single-owner state: only the shard
// goroutine that owns the keys touches it, so there are no locks and no atomics.
// Cross-shard XREAD is refused at dispatch, so a blocked waiter's keys all live on
// one owner and one goroutine serializes serve against timeout.
//
// A plain XREAD serve is a fan-out, not a hand-off: an XADD is a read event every
// blocked reader on that key observes, so it completes every plain waiter parked on
// the key rather than stopping once the value drains, and such a waiter never
// re-parks (the appended entry always has an ID above the after-ID it resolved at
// park, so a wake always produces entries).
//
// A blocking XREADGROUP `>` waiter (grp set on the request) is a hand-off instead:
// the appended entry goes to exactly one consumer, and delivering advances the
// group's shared cursor, so a second consumer parked on the same group finds
// nothing on wake and stays parked. serveWaiters therefore snapshots the FIFO and,
// for a group waiter, re-runs the delivery from the current cursor and completes it
// only when it produced entries; when an earlier consumer already took them, the
// waiter is left in place for the next XADD.

// nilIdx is the sentinel index for an absent link, the arena's nil pointer.
const nilIdx = ^uint32(0)

// xreadWait is the shared request behind one blocked read, pointed at by every
// sibling node of the waiter. keys and afters are the resolved read: keys[j]'s
// entries above afters[j] (an exclusive lower bound, the stream's last ID at park
// for "$"/"+", the explicit ID otherwise). count is the per-stream COUNT cap, -1
// for unbounded. Both slices are cloned at park so they outlive the request args.
// The struct is read once on wake to re-scan every key, so one copy serves the
// whole ring. grp is nil for a plain XREAD and set for a blocking XREADGROUP `>`,
// where the wake delivers into the group PEL from the live cursor rather than
// scanning afters, so afters is unused (all `>`) on a group waiter.
type xreadWait struct {
	keys   [][]byte
	afters []streamID
	count  int
	grp    *groupWait
}

// groupWait is the consumer-group context a blocking XREADGROUP `>` carries into
// the waiter: the group and consumer names (cloned at park) and NOACK, everything
// the wake needs to re-run deliverNew for this consumer on each named stream.
type groupWait struct {
	group []byte
	con   []byte
	noack bool
}

// waitNode is one connection's parked interest in one stream key. prev and next
// are the intrusive links within that key's FIFO list; sib is the circular ring
// across the keys of a single multi-key waiter (a one-key waiter's sib points at
// itself). wl is the list this node lives on, so a sibling walk can unlink each
// node in O(1) without re-finding its key. conn and seq are the deferred-reply
// target the handler captured through CurConn and CurSeq. req is the shared read
// every sibling shares. timer is the armed timeout, nil when the waiter blocks
// forever; it is set on the sibling-ring head only. live is the idempotency guard
// that keeps a serve and a timeout from both firing the same waiter.
type waitNode struct {
	prev, next uint32
	sib        uint32
	wl         *waitList
	conn       *shard.Conn
	seq        uint32
	req        *xreadWait
	timer      shard.TimerHandle
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
// over the shared pool. head is the oldest waiter (served first), tail the newest.
// key is kept so an emptied list can drop itself from the registry map without the
// caller re-deriving the key from a node.
type waitList struct {
	pool       *waitPool
	key        string
	head, tail uint32
	n          int
}

// park appends a new waiter to the tail and returns its node index, the FIFO order
// Redis serves blocked clients in. The caller fills the sibling ring and the timer;
// park sets the list links, the reply target, and the shared request.
func (l *waitList) park(req *xreadWait, c *shard.Conn, seq uint32) uint32 {
	i := l.pool.alloc()
	nd := &l.pool.nodes[i]
	nd.wl = l
	nd.conn = c
	nd.seq = seq
	nd.req = req
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

// unlink removes node i from its list in O(1), splicing its neighbours. It touches
// only the list links and the count; marking the node dead and returning it to the
// pool is the caller's job.
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

// parkWaiter parks one connection on every key it blocks on and returns the
// sibling-ring head index, the node the caller arms the timeout on. The nodes of
// one waiter are stitched into a circular ring through sib so a later serve on any
// key can walk to and unlink all of them. Duplicate keys park twice on the same
// list, which the sibling unlink cleans up, so the caller need not dedupe.
func parkWaiter(g *reg, req *xreadWait, c *shard.Conn, seq uint32) uint32 {
	first := nilIdx
	prev := nilIdx
	for _, key := range req.keys {
		wl := g.waitListFor(key)
		i := wl.park(req, c, seq)
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

// unlinkAll removes a whole multi-key waiter from every key it parked on: it walks
// the sibling ring from idx, unlinks each node, cancels the armed timeout if any
// node carries it, drops any list it empties, and recycles the nodes. It is the
// shared teardown for both a serve and a timeout, so a waiter leaves all of its
// lists in one pass. cx may be nil in a unit test that arms no timer, which is safe
// because CancelTimer only runs for a non-nil handle.
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

// serveWaiters completes the clients blocked on key after an XADD appended to it. It
// runs on the owner from the XADD handler, after the entry is in the stream. It
// walks the FIFO in order over a snapshot of the parked node indices, because a
// served waiter unlinks itself and its siblings, and a group waiter another consumer
// already drained stays parked, so the head is not always the next to remove. A dead
// node in the snapshot (unlinked as a sibling of an earlier served waiter) is
// skipped. A plain XREAD waiter is a fan-out, framed from readAfter and always
// completed; a group `>` waiter is a hand-off, delivered from the live cursor and
// completed only when it produced entries.
func serveWaiters(cx *shard.Ctx, g *reg, key []byte) {
	wl := g.waiters[string(key)]
	if wl == nil {
		return
	}
	order := g.snapshotWaiters(wl)
	for _, i := range order {
		nd := &g.wpool.nodes[i]
		if !nd.live {
			continue
		}
		if nd.req.grp == nil {
			conn, seq := nd.conn, nd.seq
			rep := framePark(g, nd.req)
			g.unlinkAll(cx, i)
			conn.CompleteBlocked(seq, rep)
			continue
		}
		rep, marks, served := frameGroupPark(cx, g, nd.req)
		if !served {
			continue // the entries went to an earlier consumer; stay parked
		}
		conn, seq := nd.conn, nd.seq
		g.unlinkAll(cx, i)
		// The delivery ran on the waking writer's Ctx, so the reply completes under
		// the waiter's own ack mode against the marks the delivery emitted.
		cx.CompleteServed(conn, seq, rep, marks)
	}
}

// snapshotWaiters records the FIFO order of a key's parked nodes into a reusable
// per-registry scratch, so serveWaiters can walk the order while unlinking nodes
// mid-walk without losing its place. The slice is valid only until the next call.
func (g *reg) snapshotWaiters(wl *waitList) []uint32 {
	g.serveOrder = g.serveOrder[:0]
	for i := wl.head; i != nilIdx; i = g.wpool.nodes[i].next {
		g.serveOrder = append(g.serveOrder, i)
	}
	return g.serveOrder
}

// framePark re-reads a blocked XREAD's streams and builds its reply, the array of
// [key, entries] pairs a non-blocking XREAD would return now. It is called only
// after an XADD served the waiter, so at least the appended stream yields entries
// and the reply is never the null array. The buffer is freshly allocated because
// CompleteBlocked copies it and each served waiter needs its own.
func framePark(g *reg, req *xreadWait) []byte {
	results := make([]readResult, 0, len(req.keys))
	for j := range req.keys {
		s := g.m[string(req.keys[j])]
		entries := readAfterMaybe(s, req.afters[j], req.count)
		if len(entries) > 0 {
			results = append(results, readResult{key: req.keys[j], entries: entries})
		}
	}
	return frameReadResults(nil, results)
}

// readAfterMaybe returns a stream's entries above afterID, or nothing when the
// stream is still absent, so a multi-key waiter woken on one key reads its other,
// possibly still-missing, keys without a nil dereference.
func readAfterMaybe(s *stream, afterID streamID, count int) []rangeEntry {
	if s == nil {
		return nil
	}
	return s.readAfter(afterID, count)
}
