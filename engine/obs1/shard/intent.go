package shard

import "sync/atomic"

// The F17 tier-two intent substrate (spec 2064/f3/03 section 6.1, spec
// 2064/f3/11 section 9.2). Tier-one multi-key commands fan out with no barrier
// (fan.go); tier-two cross-key-atomic commands (SMOVE, RENAME, COPY, the STORE
// forms, LMOVE, MSETNX, MULTI/EXEC) ride VLL intent locks in the Dragonfly
// lineage. This file builds the substrate itself: the process-global ticket,
// the per-owner intent queues, and the at-head barrier. No command rides it
// yet; the set and list consumers wire onto Txn in their own slices, and this
// slice proves the mechanism in isolation with the property and race tests.
//
// The doc's model, restated as it lands here. A tier-two command fetches one
// ticket from the process-global counter, computes its distinct shards, and
// enqueues a per-key intent at each owner in ascending shard order. Intents are
// owner-only memory, so enqueue and advance are plain structure operations on
// the owning worker; a key's intent queue is ordered by ticket; the command
// runs when every one of its intents is at its queue head. The ticket total
// order plus ascending acquisition makes the schedule deadlock-free.
//
// The acquisition shape here is optimistic two-phase, which is the ticket-order
// argument made operational for f1's async cross-shard enqueue. A transaction
// first waits for every intent to reach the head of its key's queue (the head
// flag each owner publishes), then locks all its heads in one all-or-nothing
// step; a lock fails only when a smaller ticket displaced the head between the
// wait and the lock, and the transaction backs off (releasing whatever it
// locked) and waits again. The globally smallest incomplete ticket can never be
// displaced, so it always locks and makes progress, and by induction every
// transaction completes: deadlock-free and livelock-free, with the lowest
// ticket the guaranteed winner. A locked head is never displaced by a later
// enqueue, so once a transaction holds all its locks no other command touches
// any of its keys until it releases, which is the cross-key atomicity the
// tier-two commands promise. Intents are owner memory and off the inbound
// watermark entirely (doc 03 section 10.2), on their own per-worker control
// queue, so tier-two traffic on some keys never latches a shard or slows the
// single-key traffic on other keys of the same shard.

// opKind tags an intent control message the coordinator posts to an owner.
type opKind uint8

const (
	opEnqueue opKind = iota // place an intent in its key's queue
	opLock                  // lock the intent if it is still its key's head
	opUnlock                // release a lock taken during a failed acquire
	opRun                   // run the critical-section work on the owner
	opRelease               // remove the intent and advance its key's queue
	opAsync                 // run a fire-and-forget owner-side closure (PostOwner)
)

// intent is one key's participation in a transaction. It lives in the owning
// worker's per-key queue and is mutated only by that worker, except head, which
// the owner publishes and the coordinator reads to know when the barrier is
// met.
type intent struct {
	ticket uint64
	txn    *Txn
	shard  int
	key    string

	// head is the owner's published "this intent is the front of its key's
	// queue" flag: the owner sets it when the intent reaches the head and clears
	// it when the intent loses the head, and the coordinator reads it in
	// Acquire. It is the only cross-goroutine field on the intent.
	head atomic.Bool

	// locked and next are owner-only. locked marks the head a transaction has
	// taken for its critical section, which pins it against displacement; next
	// links the ticket-ordered queue.
	locked bool
	next   *intent
}

// intentOp is one control message on a worker's intent queue: the op kind, the
// intent it acts on, and the reply channels the synchronous ops answer through.
// Nodes are allocated per op because tier-two traffic is rare and off every hot
// path, so there is no free list to manage.
type intentOp struct {
	next atomic.Pointer[intentOp]
	kind opKind
	in   *intent
	ack  chan bool     // opLock: whether the lock was taken
	done chan struct{} // opRun, opUnlock, opRelease: completion signal
	fn   func(*Ctx)    // opRun: the critical-section work
}

// opQueue is the intrusive MPSC the coordinator posts intent ops onto and the
// owner drains, the same Vyukov shape the hop queue uses (queue.go): producers
// swap the tail and link, the sole consumer walks with plain cursor moves, and
// the one mid-publish state is detected and retried. It is a separate queue
// from the hop inbound so intents never count against the inbound watermark and
// tier-two control traffic never sits behind a batch of point ops.
type opQueue struct {
	tail atomic.Pointer[intentOp]
	head *intentOp
	stub intentOp
}

func (q *opQueue) init() {
	q.head = &q.stub
	q.tail.Store(&q.stub)
}

func (q *opQueue) push(o *intentOp) {
	o.next.Store(nil)
	prev := q.tail.Swap(o)
	prev.next.Store(o)
}

func (q *opQueue) ready() bool {
	return q.head.next.Load() != nil || q.tail.Load() != q.head
}

func (q *opQueue) pop() *intentOp {
	h := q.head
	next := h.next.Load()
	if h == &q.stub {
		if next == nil {
			return nil
		}
		q.head = next
		h = next
		next = h.next.Load()
	}
	if next != nil {
		q.head = next
		return h
	}
	if q.tail.Load() != h {
		return nil
	}
	q.push(&q.stub)
	next = h.next.Load()
	if next == nil {
		return nil
	}
	q.head = next
	return h
}

// keyList is one key's ticket-ordered intent queue on its owner: a singly
// linked list, front first. front is the current head; a locked front is pinned
// against displacement. published is the intent whose head flag currently reads
// true, so a head change clears the old flag and sets the new one exactly once.
type keyList struct {
	front     *intent
	published *intent

	// waiters are the point commands deferred behind this key's intents
	// (txnroute.go): each parked against the intents present when it arrived
	// and run by wakeDeferred when the last of them releases.
	waiters []*defCmd
}

// intentDrainCap bounds how many intent ops a worker applies per loop turn, the
// same bounded-return discipline the hop drain follows (doc 03 section 3.1): an
// intent at a queue head waits at most one batch plus one bounded intent slice.
const intentDrainCap = 64

// advanceIntents drains the worker's intent control queue and applies the ops
// to its owner-only queues (doc 03 section 3.1's advanceIntents step). The fast
// path is one relaxed load: a worker with no pending intent ops returns at once,
// so a shard serving only single-key traffic never touches the intent
// structures. The load is paid once per drain pass, not per command, so it is
// invisible against a pass of hundreds of point ops.
func (w *worker) advanceIntents() int {
	if w.intentPending.Load() == 0 {
		return 0
	}
	n := 0
	for i := 0; i < intentDrainCap; i++ {
		o := w.intentInbox.pop()
		if o == nil {
			break
		}
		w.intentPending.Add(-1)
		w.applyIntentOp(o)
		n++
	}
	return n
}

// intentReady reports whether the intent queue has an op waiting, the plain
// load the idle re-check folds in so a worker never parks with an intent op
// unserved.
func (w *worker) intentReady() bool {
	return w.intentPending.Load() != 0 && w.intentInbox.ready()
}

// applyIntentOp runs one control message on the owner goroutine.
func (w *worker) applyIntentOp(o *intentOp) {
	switch o.kind {
	case opEnqueue:
		w.enqueueIntent(o.in)
	case opLock:
		o.ack <- w.lockIntent(o.in)
	case opUnlock:
		w.unlockIntent(o.in)
		close(o.done)
	case opRun:
		// The closure runs between commands, but curConn still names the
		// previous command's connection: an emission inside it would mark
		// against that stranger, and the connection's next command would
		// merge marks it never emitted and hold on them. Clear the pair so
		// tier-two emissions register nothing (relaxed-only, the FanTxn
		// interim noted in doc 04 section 3.2).
		w.cx.curConn = nil
		w.cx.curSeq = 0
		o.fn(&w.cx)
		close(o.done)
	case opRelease:
		w.releaseIntent(o.in)
		if o.done != nil {
			close(o.done)
		}
	case opAsync:
		w.cx.curConn = nil // same stale-mark hazard as opRun
		w.cx.curSeq = 0
		o.fn(&w.cx)
	}
}

// listFor returns the owner's queue for key, creating it on first use.
func (w *worker) listFor(key string) *keyList {
	q := w.keyQ[key]
	if q == nil {
		q = &keyList{}
		w.keyQ[key] = q
	}
	return q
}

// enqueueIntent inserts in into its key's queue in ticket order, never ahead of
// a locked front (a locked head is pinned for its transaction's critical
// section, so a later, smaller ticket queues behind it and takes the head when
// the lock releases). Tickets are unique, so the order is total.
func (w *worker) enqueueIntent(in *intent) {
	q := w.listFor(in.key)
	if q.front == nil {
		q.front = in
		in.next = nil
		w.publishHead(q)
		return
	}
	var prev *intent
	cur := q.front
	if cur.locked {
		prev, cur = cur, cur.next
	}
	for cur != nil && cur.ticket < in.ticket {
		prev, cur = cur, cur.next
	}
	in.next = cur
	if prev == nil {
		q.front = in
	} else {
		prev.next = in
	}
	w.publishHead(q)
}

// lockIntent takes the lock when in is still its key's head, pinning it against
// displacement for the transaction's critical section. It reports whether the
// lock was taken; a false is the signal for the coordinator to back off and
// wait for the head again, which happens only when a smaller ticket displaced
// in between the barrier wait and the lock.
func (w *worker) lockIntent(in *intent) bool {
	q := w.keyQ[in.key]
	if q == nil || q.front != in {
		return false
	}
	in.locked = true
	return true
}

// unlockIntent clears a lock taken during an acquire that later failed on
// another key. It then re-sorts the key's queue into pure ticket order and
// republishes the head. The re-sort is the liveness fix for the speculative
// lock: while in held its head locked, a smaller ticket that enqueued was
// forced behind it (enqueueIntent never displaces a locked head), and on
// another key of the same smaller ticket no lock was held so it sat ahead;
// leaving those two keys in opposite order is the classic cross-key deadlock.
// Restoring ticket order the moment the speculative lock releases returns the
// queue to the deadlock-free VLL invariant, where the globally smallest ticket
// heads every key it wants.
func (w *worker) unlockIntent(in *intent) {
	q := w.keyQ[in.key]
	if q == nil {
		return
	}
	in.locked = false
	w.sortList(q)
}

// sortList rebuilds q.front in ascending ticket order and republishes the head.
// It runs an insertion sort over the linked list, which is right for the short
// queues a contended key carries; it is only ever called with no locked node in
// the queue, so it never reorders a pinned head.
func (w *worker) sortList(q *keyList) {
	var sorted *intent
	for cur := q.front; cur != nil; {
		next := cur.next
		if sorted == nil || cur.ticket < sorted.ticket {
			cur.next = sorted
			sorted = cur
		} else {
			p := sorted
			for p.next != nil && p.next.ticket < cur.ticket {
				p = p.next
			}
			cur.next = p.next
			p.next = cur
		}
		cur = next
	}
	q.front = sorted
	w.publishHead(q)
}

// releaseIntent removes in from its key's queue and advances the queue, so the
// next waiter takes the head. An emptied queue is dropped so the map holds only
// contended keys.
func (w *worker) releaseIntent(in *intent) {
	q := w.keyQ[in.key]
	if q == nil {
		return
	}
	in.locked = false
	if q.front == in {
		q.front = in.next
	} else {
		for cur := q.front; cur != nil; cur = cur.next {
			if cur.next == in {
				cur.next = in.next
				break
			}
		}
	}
	in.next = nil
	if q.published == in {
		q.published = nil
	}
	in.head.Store(false)
	// Run the point commands this release unblocks before the next head
	// publishes: a waiter whose await set empties here executes ahead of any
	// lock the new head's transaction will post, so deferred traffic lands
	// between critical sections, never inside one.
	w.wakeDeferred(q, in)
	if q.front == nil {
		// Any waiter still parked awaits other keys' intents only (its await
		// entries for this queue died with their releases) and is reachable
		// through those keys' lists, so the entry can go.
		delete(w.keyQ, in.key)
		return
	}
	w.publishHead(q)
}

// publishHead makes the head flags match the queue front: the intent that lost
// the head has its flag cleared and its transaction poked, and the new front
// has its flag set and its transaction poked, each exactly once. A poke wakes
// the coordinator so it re-checks the barrier.
func (w *worker) publishHead(q *keyList) {
	if q.published == q.front {
		return
	}
	if q.published != nil {
		q.published.head.Store(false)
		pokeTxn(q.published.txn)
	}
	q.published = q.front
	if q.front != nil {
		q.front.head.Store(true)
		pokeTxn(q.front.txn)
	}
}

// pokeTxn nudges a transaction's coordinator to re-check its barrier. The
// channel has capacity one and the send is non-blocking, so a poke that finds
// one already pending is dropped: the coordinator re-reads every head flag on
// wake, so one pending poke covers any number of changes.
func pokeTxn(t *Txn) {
	select {
	case t.sig <- struct{}{}:
	default:
	}
}

// postIntent publishes one op to a worker's intent queue and wakes it. The wake
// is the same producer-side claim the hop path uses (waker.go), so a worker
// parked with no traffic still services the op promptly.
func (w *worker) postIntent(o *intentOp) {
	w.intentPending.Add(1)
	w.intentInbox.push(o)
	w.wk.wake()
}

// PostOwner runs fn on the target shard's owner goroutine, off the intent
// control queue, with no acknowledgement and no barrier. It is the cross-owner
// side-hop a cross-shard blocking command uses at serve time: the owner that
// serves the block posts a fire-and-forget closure to each other participating
// owner to cancel its now-dead local waiter node. Because it rides the same MPSC
// control queue Begin's enqueues ride, a closure posted after those enqueues
// runs on the owner in that order, and any state the poster published before the
// post is visible to fn. fn runs single-owner on the target shard, so it may
// touch that shard's owner-only structures (its waiter set, its timers) exactly
// like a handler. It never blocks and takes no locks.
func (r *Runtime) PostOwner(shard int, fn func(*Ctx)) {
	r.workers[shard].postIntent(&intentOp{kind: opAsync, fn: fn})
}

// Txn is a tier-two intent transaction: a ticket, one intent per distinct key
// in ascending shard order, and the barrier signal the owners poke. A caller
// runs Begin, Acquire, its per-key work through Do, then Release.
type Txn struct {
	rt      *Runtime
	ticket  uint64
	intents []*intent
	sig     chan struct{}
	locked  bool
}

// nextTicket hands out the next transaction ticket from the process-global
// counter, the one deliberate F1 deviation off the single-key path (doc 03
// section 6.1): it is touched only by tier-two commands, which are rare.
func (r *Runtime) nextTicket() uint64 {
	return r.txnTicket.Add(1)
}

// Begin declares a tier-two transaction over keys. Duplicate keys collapse to
// one intent, and the intents are ordered by shard ascending, the doc's
// acquisition order. Nothing is acquired yet; the intents are enqueued at their
// owners and Acquire waits for the barrier. When every key lands on one shard
// the transaction still rides the substrate correctly, the single-shard
// degenerate case the parser will later shortcut to a plain batch.
func (r *Runtime) Begin(keys [][]byte) *Txn {
	t := r.newTxn(keys)
	for _, in := range t.intents {
		r.workers[in.shard].postIntent(&intentOp{kind: opEnqueue, in: in})
	}
	return t
}

// newTxn builds the transaction without enqueueing anything: the ticket and
// the shard-sorted intents. Begin posts the enqueues through the intent
// control queues; DoTxn (txnroute.go) instead arms each intent through the
// inbound hop path so the enqueue is ordered against the connection's point
// traffic.
func (r *Runtime) newTxn(keys [][]byte) *Txn {
	t := &Txn{rt: r, ticket: r.nextTicket(), sig: make(chan struct{}, 1)}
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		ks := string(k)
		if _, dup := seen[ks]; dup {
			continue
		}
		seen[ks] = struct{}{}
		t.intents = append(t.intents, &intent{
			ticket: t.ticket,
			txn:    t,
			shard:  r.ShardOf(k),
			key:    ks,
		})
	}
	// Ascending shard order: the doc's acquisition order, which reduces conflict
	// and matches the deadlock-free schedule. A stable sort keeps distinct keys
	// on one shard in their first-seen order.
	sortByShard(t.intents)
	return t
}

// Acquire blocks until the transaction holds every key at its queue head and
// locked (the barrier). It is deadlock-free by ticket order: a failed lock
// means a smaller ticket took a head, so the transaction backs off and waits
// again, and the globally smallest ticket never fails, so it always progresses.
// After Acquire returns the caller owns every key until Release.
func (t *Txn) Acquire() {
	for {
		t.waitAllHead()
		if t.lockAll() {
			t.locked = true
			return
		}
		t.unlockAll()
	}
}

// waitAllHead blocks until every intent's head flag reads true, re-checking on
// each owner poke. The flags can flip back to false after the check, which the
// lock phase catches; this is the optimistic half of the acquire.
func (t *Txn) waitAllHead() {
	for {
		if t.allHead() {
			return
		}
		<-t.sig
	}
}

func (t *Txn) allHead() bool {
	for _, in := range t.intents {
		if !in.head.Load() {
			return false
		}
	}
	return true
}

// lockAll asks every owner to lock its head in ascending shard order and
// reports whether all locks were taken. A single failure leaves the taken locks
// in place for unlockAll to clear.
func (t *Txn) lockAll() bool {
	ok := true
	for _, in := range t.intents {
		ack := make(chan bool, 1)
		t.rt.workers[in.shard].postIntent(&intentOp{kind: opLock, in: in, ack: ack})
		if !<-ack {
			ok = false
		}
	}
	return ok
}

// unlockAll clears every lock the transaction currently holds and waits for the
// owners to apply it, so a retry sees a clean slate.
func (t *Txn) unlockAll() {
	dones := make([]chan struct{}, 0, len(t.intents))
	for _, in := range t.intents {
		done := make(chan struct{})
		t.rt.workers[in.shard].postIntent(&intentOp{kind: opUnlock, in: in, done: done})
		dones = append(dones, done)
	}
	for _, done := range dones {
		<-done
	}
}

// Do runs fn on the owner of key inside the transaction's critical section: the
// key must be one the transaction holds, and fn runs on that owner's goroutine
// with the shard's Ctx, so it is a plain single-owner mutation exactly like a
// point command's handler. This is the hop the doc's worked plans issue between
// the barrier and the release (doc 03 sections 6.3 to 6.7); the set and list
// consumers build their per-owner steps on it.
func (t *Txn) Do(key []byte, fn func(*Ctx)) {
	ks := string(key)
	for _, in := range t.intents {
		if in.key == ks {
			done := make(chan struct{})
			t.rt.workers[in.shard].postIntent(&intentOp{kind: opRun, in: in, fn: fn, done: done})
			<-done
			return
		}
	}
}

// Release drops every intent and advances the queues so the next waiters take
// the heads. It is safe to call whether or not Acquire succeeded; after it the
// transaction must not be used again.
func (t *Txn) Release() {
	for _, in := range t.intents {
		t.rt.workers[in.shard].postIntent(&intentOp{kind: opRelease, in: in})
	}
	t.locked = false
}

// sortByShard orders intents by shard ascending with an insertion sort, stable
// and allocation-free, which is right for the short key lists a tier-two
// command carries.
func sortByShard(a []*intent) {
	for i := 1; i < len(a); i++ {
		x := a[i]
		j := i - 1
		for j >= 0 && a[j].shard > x.shard {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = x
	}
}
