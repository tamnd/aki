package shard

import "sync/atomic"

// mpsc is the intrusive multi-producer single-consumer batch queue both hop
// directions ride (doc 03 sections 3.2 and 4.2): a producer publishes a node
// with one atomic swap of the tail plus the link store, and the sole consumer
// walks the list with plain cursor moves, so the per-batch cost is one atomic
// operation on each side. The stub node keeps the list non-empty so neither
// side ever special-cases an empty chain, and the one in-between state (a
// producer past its tail swap but before its link store) is detected by the
// consumer and resolved by returning empty for one turn.
type mpsc struct {
	tail atomic.Pointer[hopBatch] // producers swap the new node in here
	head *hopBatch                // consumer-only cursor
	stub hopBatch
}

func (q *mpsc) init() {
	q.head = &q.stub
	q.tail.Store(&q.stub)
}

// push publishes b. Any goroutine may call it; the swap serializes producers
// and a single producer's pushes stay in its program order.
func (q *mpsc) push(b *hopBatch) {
	b.next.Store(nil)
	prev := q.tail.Swap(b)
	prev.next.Store(b)
}

// ready reports whether a pop can make progress, the plain-load check the
// consumer's spin loop and the producer wake rule both key on.
func (q *mpsc) ready() bool {
	return q.head.next.Load() != nil || q.tail.Load() != q.head
}

// pop returns the oldest published node, or nil when the queue is empty or a
// producer is mid-publish; a nil under load is transient because the two
// producer instructions are back to back, and the wake rules guarantee the
// consumer comes back for the node.
func (q *mpsc) pop() *hopBatch {
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
		// A producer swapped the tail but has not linked yet.
		return nil
	}
	// h is the last node; cycle the stub behind it so h can detach.
	q.push(&q.stub)
	next = h.next.Load()
	if next == nil {
		// A concurrent producer swapped in between; its link lands momentarily.
		return nil
	}
	q.head = next
	return h
}
