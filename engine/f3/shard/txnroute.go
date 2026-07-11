package shard

import "time"

// The cross-shard tier-two command route (spec 2064/f3/03 sections 6.1 and
// 6.7, spec 2064/f3/11 section 9.2): how a live command rides the F17 intent
// substrate (intent.go) with its ordering against the point traffic around it
// intact. Slice 1 built the ticket, the queues, and the barrier; this file
// wires them to a connection. Three problems own the shape.
//
// Ordering in. Intents ride their own control queue, off the inbound
// watermark, so a bare Begin could overtake point commands the same
// connection enqueued first. The route therefore arms the transaction through
// the inbound path: one arm sub-command per key rides a normal hop to the
// key's owner, all sharing one reply sequence through a fan coordinator
// record, and the owner enqueues the intent when the arm executes, which is
// after every earlier command the connection sent to that shard. Tickets are
// fetched in dispatch order on the reader goroutine, so a connection's
// transactions also serialize against each other in the order it issued them.
//
// Ordering out. The reply is not known until the transaction has run, long
// after the arm partials have gathered, so the arms' fan record never emits.
// The coordinator goroutine finishes by pushing a loopback node carrying the
// finished reply at the command's sequence onto the connection's outbound
// queue, and the reorder ring slots it exactly like a reply from an owner.
//
// Exclusion. The barrier holds off other transactions, but a point command
// would sail through its owner's batch loop mid-critical-section and observe
// a half-done move. So the owner defers a point command that touches a key
// with queued intents: the command parks against the intents present at that
// moment and runs the instant the last of them has released, which serializes
// it after every transaction it arrived behind and before any that arms
// later (a later arm from the same connection always carries a larger
// ticket, and its critical section cannot start until these earlier intents
// have left the queue). A shard with no queued intents pays one map length
// check per keyed command, measured against the drain-execute gate.

// OpTxnArm is the reserved arm op: the owner-side half of DoTxn's enqueue. It
// is a shard builtin like OpError, dispatched in execute before the handler
// table, so no registered handler may claim 0xfe. The loopback reply node
// reuses the byte as a marker; it travels only on the outbound queue, where
// the op is never read.
const OpTxnArm byte = 0xfe

// DoTxn enqueues one cross-shard tier-two command: keys are its distinct keys
// (duplicates collapse), and run is the transaction body, called with the
// barrier held; it issues its per-owner steps through Txn.Do and returns the
// command's finished RESP reply. The command takes one reply sequence, like a
// fan-out. run executes on a spawned coordinator goroutine, so every byte it
// captures (keys and operands alike) must be a stable copy, never a parser or
// hop-node view.
func (c *Conn) DoTxn(keys [][]byte, run func(t *Txn) []byte) error {
	if c.seq-c.emitted.Load() >= uint32(len(c.ring)) {
		if err := c.throttle(); err != nil {
			return err
		}
	}
	t := c.rt.newTxn(keys)
	fc := &fanCmd{kind: FanTxn, pending: int32(len(t.intents)), txn: t}
	for _, in := range t.intents {
		if err := c.enqueueFan(in.shard, OpTxnArm, [][]byte{[]byte(in.key)}, fc); err != nil {
			return err
		}
	}
	seq := c.seq
	c.seq++
	go func() {
		t.Acquire()
		rep := run(t)
		t.Release()
		c.finishTxn(seq, rep)
	}()
	return nil
}

// finishTxn delivers a transaction's reply: a loopback node with the finished
// bytes at the command's sequence goes onto the outbound queue, where the
// writer reorders it like any owner-produced reply. The free-list channel and
// the outbound MPSC are both safe from this goroutine, and the wake-skip rule
// is the same one every producer follows (see flushShard).
func (c *Conn) finishTxn(seq uint32, rep []byte) {
	b := c.take()
	b.add(OpTxnArm, seq, false, nil)
	r := Reply{b: b, i: 0}
	r.Raw(rep)
	if c.out.push(b) {
		c.wk.wake()
	}
}

// armIntent is the OpTxnArm builtin: the owner enqueues the arm's intent into
// its key queue, in inbound order relative to the point traffic around it,
// and answers the empty partial its fan record swallows. Arm commands bypass
// the defer check by construction (execute branches on the op first): an arm
// that parked behind an earlier transaction's intents could deadlock two
// transactions arming across the same shards in opposite orders, and the
// ticket order already serializes them safely.
func (w *worker) armIntent(b *hopBatch, i int) {
	fc := b.fan(i)
	w.enqueueIntent(fc.txn.intentFor(b.arg(i, 0)))
	Reply{b: b, i: i}.FanOK()
}

// defCmd is one point command parked behind a key's queued intents: its slot
// in its hop node, and the intents it still awaits. It runs when the last
// awaited intent releases. done guards against a second run through another
// key's waiter list once a multi-key command has fired.
type defCmd struct {
	b     *hopBatch
	i     int
	await []*intent
	done  bool
}

// deferForIntent parks command i when a key it touches has queued intents,
// reporting whether it deferred. The await set is the intents present right
// now: the command arrived (in inbound order) after each of them armed, so it
// must follow their critical sections, and nothing that arms later may cut in
// front of it. A fan sub-command checks every argument, because its whole
// argument run is same-shard keys by construction (values or position blobs
// that collide with a queued key can only defer it needlessly, never corrupt
// it). Owner goroutine only.
func (w *worker) deferForIntent(b *hopBatch, i int) bool {
	c := &b.cmds[i]
	var dc *defCmd
	var qs []*keyList
	attach := func(q *keyList) {
		if q == nil || q.front == nil {
			return
		}
		for _, held := range qs {
			if held == q {
				return
			}
		}
		if dc == nil {
			dc = &defCmd{b: b, i: i}
		}
		qs = append(qs, q)
		for cur := q.front; cur != nil; cur = cur.next {
			dc.await = append(dc.await, cur)
		}
		q.waiters = append(q.waiters, dc)
	}
	if b.fan(i) != nil {
		for k := 0; k < int(c.argn); k++ {
			attach(w.keyQ[string(b.arg(i, k))])
		}
	} else {
		attach(w.keyQ[string(b.arg(i, 0))])
	}
	if dc == nil {
		return false
	}
	b.deferN++
	return true
}

// wakeDeferred advances q's parked commands after released left the queue:
// each waiter drops released from its await set, and a waiter whose set
// empties runs now, in the order it parked. Entries that already ran through
// another key's list are swept out. Owner goroutine only.
func (w *worker) wakeDeferred(q *keyList, released *intent) {
	if len(q.waiters) == 0 {
		return
	}
	kept := q.waiters[:0]
	var due []*defCmd
	for _, dc := range q.waiters {
		for k, in := range dc.await {
			if in == released {
				dc.await[k] = dc.await[len(dc.await)-1]
				dc.await = dc.await[:len(dc.await)-1]
				break
			}
		}
		switch {
		case dc.done:
			// Ran through another key's list; sweep it.
		case len(dc.await) == 0:
			due = append(due, dc)
		default:
			kept = append(kept, dc)
		}
	}
	for i := len(kept); i < len(q.waiters); i++ {
		q.waiters[i] = nil
	}
	q.waiters = kept
	if len(due) > 0 {
		w.runDeferred(due)
	}
}

// runDeferred executes parked commands whose awaited intents have all
// released, under the same epoch bracket and batch clock a drained batch
// gets. A node whose last deferred command completes here is pushed to its
// connection with a direct wake; this is the rare tier-two path, so the
// pass-level wake coalescing is not worth its bookkeeping.
func (w *worker) runDeferred(due []*defCmd) {
	w.ep.enter()
	w.cx.NowMs = time.Now().UnixMilli()
	for _, dc := range due {
		dc.done = true
		w.executeCmd(dc.b, dc.i)
		if st := dc.b.stream(dc.i); st != nil {
			w.streams = append(w.streams, st)
		}
		dc.b.deferN--
		if dc.b.deferN == 0 {
			conn := dc.b.conn
			if conn.out.push(dc.b) {
				conn.wk.wake()
			}
		}
	}
	w.ep.exit()
}

// intentFor returns the transaction's intent for key. The arm builtin calls
// it on the owner; keys are deduplicated in newTxn, so the match is unique.
func (t *Txn) intentFor(key []byte) *intent {
	for _, in := range t.intents {
		if in.key == string(key) {
			return in
		}
	}
	return nil
}

// SameShard reports whether two keys route to the same shard, the dispatch
// check that keeps a co-located tier-two command on its free single-shard
// fast path (doc 03 section 6.1) and sends only the genuinely cross-shard
// case through DoTxn.
func (c *Conn) SameShard(a, b []byte) bool {
	return c.rt.ShardOf(a) == c.rt.ShardOf(b)
}
