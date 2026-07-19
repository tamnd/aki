package zset

import (
	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/resp"
)

// The zset blocking-waiter set (spec 2064/f3/12 section 6.7), the sorted-set
// twin of the list waiter substrate (engine/f3/list/waiter.go). A connection
// that BZPOPMIN/BZPOPMAX/BZMPOP-blocks on one or more keys parks a waitNode on
// each key's FIFO list. The nodes live in one per-shard slab (waitPool) addressed
// by index, so park, wake, and timeout-unlink are each O(1) with no per-waiter
// allocation once the slab has grown, and a multi-key waiter's nodes are stitched
// into a circular sibling ring so serving on one key unlinks the waiter from every
// other key in one walk. Everything here is single-owner state: only the shard
// goroutine that owns the key touches it, so there are no locks and no atomics, and
// the one owner serializes serve against timeout. The cross-shard route (a wait
// whose keys span owners) is a later slice; this slice serves the co-located key
// set the same way the zset pops already read their keys from one owner.

// nilIdx is the sentinel index for an absent link, the arena's version of a nil
// pointer. A real node index is a slab offset and never equals it.
const nilIdx = ^uint32(0)

// The waiter kind discriminates the two blocking shapes a parked node can carry,
// so one serve loop can complete either off the same key's FIFO. kindPop is
// BZPOPMIN/BZPOPMAX: pop one member and reply [key, member, score]. kindMpop is
// BZMPOP: pop up to count members off one end and reply [key, [[member, score],
// ...]]. The serve reads the kind off whichever sibling node is the served key's
// list head and branches on it. There is no move shape: the sorted set has no
// blocking move verb, so the list's kindMove is absent here.
const (
	kindPop  uint8 = 0
	kindMpop uint8 = 1
)

// waitNode is one connection's parked interest in one key. prev and next are the
// intrusive links within that key's FIFO list; sib is the circular ring across the
// keys of a single multi-key waiter (a one-key waiter's sib points at itself). wl
// is the list this node lives on, so a sibling walk can unlink each node in O(1)
// without re-finding its key. conn and seq are the deferred-reply target the
// handler captured through CurConn and CurSeq. min is true to pop the lowest-scored
// end (BZPOPMIN) and false for the highest (BZPOPMAX). timer is the armed timeout,
// nil when the waiter blocks forever; it is set on the sibling-ring head only. live
// is the idempotency guard that keeps a serve and a timeout from both firing the
// same waiter. kind and count carry the per-shape parameters the serve reads off
// the list head: count is the BZMPOP member budget for one wake. park writes every
// one of them on each call, since a recycled node holds a prior waiter's stale
// values.
type waitNode struct {
	prev, next uint32
	sib        uint32
	wl         *waitList
	conn       *shard.Conn
	seq        uint32
	timer      shard.TimerHandle
	kind       uint8
	min        bool
	live       bool
	count      int
}

// waitSpec is the by-value bundle of one waiter's per-shape parameters, threaded
// through park so every sibling node of a multi-key waiter carries the same kind,
// end, and count. It is stack-copied into each node, so a kindPop spec (zero count)
// parks with no allocation.
type waitSpec struct {
	kind  uint8
	min   bool
	count int
}

// waitPool is the per-shard node slab. nodes grows once to its working size and
// then holds steady; free is the recycle stack a released node returns to, so a
// warm park reuses a slot and allocates nothing. It hangs off the registry as a
// value, so &g.wpool is a stable pointer every waitList keeps.
type waitPool struct {
	nodes []waitNode
	free  []uint32
}

// alloc returns a free node index, growing the slab only when the recycle stack is
// empty. The returned node's fields are stale and the caller sets every one.
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
// Redis serves blocked clients in. The caller fills the sibling ring; park sets the
// list links and every reply-shape field from spec. It writes kind, min, and count
// on every call, never skipping one, because the node may be recycled from a prior
// waiter of a different kind whose stale fields would otherwise leak.
func (l *waitList) park(spec waitSpec, c *shard.Conn, seq uint32) uint32 {
	i := l.pool.alloc()
	nd := &l.pool.nodes[i]
	nd.wl = l
	nd.conn = c
	nd.seq = seq
	nd.kind = spec.kind
	nd.min = spec.min
	nd.count = spec.count
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
// pool is the caller's job, so unlink can serve both a served head and a timed-out
// middle node.
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

// waitListFor returns the waiter FIFO for key, creating an empty one on first
// block. It lazily initializes the map so a registry built directly in a unit test
// (with a nil waiters map) can still park; the real registry() path pre-builds it.
func (g *reg) waitListFor(key []byte) *waitList {
	if g.waiters == nil {
		g.waiters = make(map[string]*waitList)
	}
	wl := g.waiters[string(key)]
	if wl == nil {
		wl = &waitList{pool: &g.wpool, key: string(key), head: nilIdx, tail: nilIdx}
		g.waiters[string(key)] = wl
	}
	return wl
}

// dropWaitersIfEmpty removes a waiter list from the registry once its last waiter
// leaves, mirroring drop for the value map so a key that was blocked on and then
// drained leaves nothing behind.
func (g *reg) dropWaitersIfEmpty(wl *waitList) {
	if wl.n == 0 {
		delete(g.waiters, wl.key)
	}
}

// parkWaiter parks one connection on every key it blocks on and returns the
// sibling-ring head index, the node the caller arms the timeout on. The nodes of
// one waiter are stitched into a circular ring through sib so a later serve on any
// one key can walk to and unlink all of them. Duplicate keys park twice on the same
// list, which the sibling unlink cleans up, so the caller need not dedupe.
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

// serveWaiters hands freshly added members to the clients blocked on key, in waiter
// FIFO order, until either the zset empties or no waiter remains. It runs on the
// owner from every zset-growing command (ZADD, ZINCRBY, and the *STORE writers),
// after the members are in the zset. Unlike the list's serve there is no chained
// worklist: the sorted set has no blocking move, so serving one key never grows
// another, and this stays a single-key drain. Each waiter's reply is built into its
// own fresh buffer because CompleteBlocked copies it and the next waiter reuses none
// of it. It reaps the zset to nothing (drop) or reconciles its footprint (note) once
// the drain stops, so a fully drained key leaves no empty zset behind.
func serveWaiters(cx *shard.Ctx, g *reg, key []byte, z *zset) {
	wl := g.waiters[string(key)]
	if wl == nil {
		return
	}
	var sc [40]byte
	for wl.n > 0 && z.card() > 0 {
		i, ok := wl.peekHead()
		if !ok {
			return
		}
		nd := &g.wpool.nodes[i]
		conn := nd.conn
		seq := nd.seq
		min := nd.min
		// The reply carries the parked client's negotiated protocol, not the
		// serving writer's: a BZPOPMIN issued over RESP3 gets its score as a double
		// even when a RESP2 client's ZADD is what serves it.
		resp3 := conn.Resp3()
		switch nd.kind {
		case kindPop:
			var rep []byte
			z.pop(min, 1, func(m []byte, s float64) {
				logRemove(cx, key, m)
				rep = resp.AppendArrayHeader(nil, 3)
				rep = resp.AppendBulk(rep, key)
				rep = resp.AppendBulk(rep, m)
				rep = appendScore(rep, s, resp3, sc[:])
			})
			g.unlinkAll(cx, i)
			// The deferred serve pops from the zset just like an immediate one, so it
			// fires the same zpopmin/zpopmax from the served end. The generic del, if
			// the drain empties the key, is grewNote's to fire once the loop ends.
			cx.NotifyKeyspaceEvent(shard.NotifyZset, popEvent(min), key)
			conn.CompleteBlocked(seq, rep)
		default: // kindMpop
			npop := nd.count
			if npop > z.card() {
				npop = z.card()
			}
			rep := resp.AppendArrayHeader(nil, 2)
			rep = resp.AppendBulk(rep, key)
			rep = resp.AppendArrayHeader(rep, npop)
			z.pop(min, npop, func(m []byte, s float64) {
				logRemove(cx, key, m)
				rep = resp.AppendArrayHeader(rep, 2)
				rep = resp.AppendBulk(rep, m)
				rep = appendScore(rep, s, resp3, sc[:])
			})
			g.unlinkAll(cx, i)
			cx.NotifyKeyspaceEvent(shard.NotifyZset, popEvent(min), key)
			conn.CompleteBlocked(seq, rep)
		}
	}
}

// grewNote settles a zset that a command just grew and that still holds members:
// it serves any blocked BZPOPMIN/BZPOPMAX/BZMPOP waiters from the new members, then
// reaps the zset if the serve drained it or reconciles its footprint if members
// remain. It is the drop-in replacement for g.note on every zset-growing path
// (ZADD, ZINCRBY, the *STORE writers, GEOADD): a freshly created zset must already
// be installed so a fully drained serve can drop it and a served waiter reads it
// live. On the common path with nobody blocked it is one map-length load plus the
// note the caller would have made anyway, holding the zero-delta contract for a
// grow with no blocked client.
func (g *reg) grewNote(cx *shard.Ctx, key []byte, z *zset) {
	if len(g.waiters) != 0 {
		serveWaiters(cx, g, key, z)
		if z.card() == 0 {
			g.drop(key)
			return
		}
	}
	g.note(z)
}
