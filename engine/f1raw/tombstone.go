package f1raw

// Deferred ordered-index removal takes the one expensive step off an element delete's
// critical path. A point delete (SREM, HDEL) already removes the element from the
// authoritative hash index with a lock-free CAS, the same O(1) work Redis does. What made
// it lose to Redis was the second structure it also had to repair synchronously: the global
// order-statistic skip list (oindex.go) that serves enumeration and random selection. Every
// element of every collection is a node in that one list, and unlinking a node is an O(log n)
// splice under a single global write lock. On the single-hot-key delete gate that lock and
// that splice, held on the reply path, are the whole gap: the read on the same key is
// lock-free and scales across every core, and the delete could not, so a 5x read sat next to
// a 1.4x delete on the identical key.
//
// The fix is to stop splicing on the hot path. When deferred removal is on, a delete copies
// the composite element key onto a lock-free Treiber stack (the tombstone queue) and returns.
// A background folder goroutine pops the stack in batches and does the O(log n) splices under
// oi.mu, off the reply path, so the global lock is taken by one folder on its own schedule
// instead of by every foreground connection on every delete. The skip list is briefly stale
// (it still holds nodes for elements the hash index has already dropped), and two things keep
// that invisible: the cardinality is the seqlock count word, not the node count, so SCARD is
// always exact; and every enumeration treats a node as a hint and skips one whose record is no
// longer live (the liveness filter in scanBatch), the same "the column is authoritative, the
// sibling index is a derived hint" contract the ordered index already documented, extended
// from "the value may have moved" to "the element may be gone".
//
// The one hazard the deferral introduces is a re-add racing the folder: an element is deleted
// (node still present, pointing at the now-dead record), then added again before the folder
// processes the tombstone. removeManyLive closes it by re-checking liveness against the hash
// index under oi.mu, atomic with the splice, and skipping a key whose record is live again.
// The re-add republishes the hash record before it touches the ordered index, so the folder's
// under-lock ExistsKind either sees the live record and keeps the node, or sees no record and
// splices a genuinely dead one; no interleaving ends with a live element missing from the
// index. Spec 2064/f1_rewrite_ltm/16 section 6 carries the full case analysis.

// tombNode is one batch of deferred ordered-index removals, the unit pushed onto the tombstone
// stack. A batch carries the composite element keys packed end to end in buf, with ends[k] the
// cumulative byte length through the k-th key, exactly the packed form the coalesced delete
// drain already builds, so a whole pipeline run of same-key deletes folds into one node and one
// push. kind is the record kind every key in the batch shares (a delete run is single-type), so
// the folder's liveness re-check knows which namespace to probe. The bytes are copied out of the
// caller's per-command scratch at push time, since the folder reads them long after the delete
// returned and the scratch is reused by the next command.
type tombNode struct {
	buf  []byte
	ends []int
	kind byte
	next *tombNode
}

// EnableDeferredRemoval turns on deferred ordered-index removal and starts the folder goroutine.
// It is idempotent and must be called once at startup before serving, the same lifecycle point
// SetTopKindFunc uses: the server calls it, engine tests that want to exercise the folder call it
// explicitly, and a store that never calls it keeps the synchronous inline splice, so every
// existing test and every non-server user is unaffected. The folder is stopped and its remaining
// tombstones drained by Close.
func (s *Store) EnableDeferredRemoval() {
	if s.folderOn.Swap(true) {
		return
	}
	s.folderStop = make(chan struct{})
	s.folderDone = make(chan struct{})
	// A single-slot wake channel: a producer signals it after a push without ever blocking
	// (the default branch drops the signal when one is already pending), so a burst of deletes
	// costs one non-blocking send each and the folder coalesces them into one wake.
	s.folderWake = make(chan struct{}, 1)
	go s.folderLoop()
}

// folderLoop is the background folder: it drains the tombstone stack, and when the stack is
// empty it parks until a producer wakes it or Close stops it. Under sustained delete load drain
// keeps returning work so the loop never parks; when the stream stops it blocks, so an idle store
// spends nothing. On stop it does one final drain so no tombstone outlives the folder.
func (s *Store) folderLoop() {
	defer close(s.folderDone)
	for {
		if s.drainTombstones() > 0 {
			// Drained a batch; there may be more already queued, so loop and drain again
			// rather than park, but check for a stop first so shutdown is prompt.
			select {
			case <-s.folderStop:
				s.drainTombstones()
				return
			default:
			}
			continue
		}
		select {
		case <-s.folderStop:
			s.drainTombstones()
			return
		case <-s.folderWake:
		}
	}
}

// enqueueTomb copies the packed keys and pushes them as one batch onto the tombstone stack, then
// wakes the folder. The push is a single CAS on the stack head, wait-free in the common case; the
// copy is the cost of deferral (the folder reads the keys after the delete returned and the
// caller's scratch is reused), and it is far cheaper than the O(log n) globally-locked splice it
// replaces on the hot path.
func (s *Store) enqueueTomb(buf []byte, ends []int, kind byte) {
	n, _ := s.tombPool.Get().(*tombNode)
	if n == nil {
		n = &tombNode{}
	}
	// Reuse the node's arenas: append onto a zero-length reslice grows them only when a batch is
	// larger than any this node held before, so a steady stream of same-size drains never allocates.
	n.buf = append(n.buf[:0], buf...)
	n.ends = append(n.ends[:0], ends...)
	n.kind = kind
	n.next = nil
	for {
		head := s.tombHead.Load()
		n.next = head
		if s.tombHead.CompareAndSwap(head, n) {
			break
		}
	}
	s.tombPend.Add(int64(len(ends)))
	select {
	case s.folderWake <- struct{}{}:
	default:
	}
}

// drainTombstones swaps the whole stack out in one atomic step and splices every batch it took,
// returning the number of element keys it processed. It holds folderMu across the whole snapshot
// so a foreground SyncPendingRemovals that wants the index reconciled right now blocks on an
// in-flight folder drain instead of returning while the folder still has this key's dead node
// un-spliced (the folder releases oi.mu between batches, so oi.mu alone is not that barrier). The
// swap makes the drain a private snapshot, so the folder and a foreground drain take disjoint
// chains and never process the same node twice. Each batch goes through removeManyLive, which
// re-checks liveness under oi.mu so a key re-added since the tombstone was queued keeps its node.
func (s *Store) drainTombstones() int {
	s.folderMu.Lock()
	defer s.folderMu.Unlock()
	head := s.tombHead.Swap(nil)
	if head == nil {
		return 0
	}
	total := 0
	for n := head; n != nil; {
		s.oidx.Load().removeManyLive(n.buf, n.ends, n.kind)
		total += len(n.ends)
		// The node is off the stack in this drain's private snapshot and now spliced, so nothing
		// else references it; hand it and its arenas back to the pool for the next enqueue. Read
		// next before recycling, since a producer may repopulate n the moment it is Put.
		next := n.next
		n.next = nil
		s.tombPool.Put(n)
		n = next
	}
	s.tombPend.Add(int64(-total))
	return total
}

// CollRemovePacked drops a run of just-deleted element keys from the ordered index, deferring the
// splice to the folder when deferred removal is on and doing it inline when it is not. buf holds
// the composite keys packed end to end and ends[k] is the cumulative byte length through the k-th
// key, the same packed form the coalesced delete drain builds; kind is the shared record kind.
// Call it under the collection's stripe lock, right after the hash-index rows are deleted, the
// same serialization CollRemoveMany relies on. When deferred, the keys are copied and the caller's
// scratch is free to reuse immediately; when inline, the reslices are consumed before return.
func (s *Store) CollRemovePacked(buf []byte, ends []int, kind byte) {
	if len(ends) == 0 {
		return
	}
	if s.folderOn.Load() {
		s.enqueueTomb(buf, ends, kind)
		return
	}
	keys := make([][]byte, 0, len(ends))
	prev := 0
	for _, e := range ends {
		keys = append(keys, buf[prev:e])
		prev = e
	}
	s.oidx.Load().removeMany(keys)
}

// SyncPendingRemovals drains any outstanding tombstones synchronously, so a caller that needs the
// ordered index reconciled with the hash index right now (a rank-based random select, whose width
// arithmetic counts a not-yet-spliced dead node as if it were live) can get an exact structure. It
// is a no-op when nothing is pending, which is the steady state for a select workload that is not
// interleaved with deletes, so the random-select hot path pays only one atomic load. When deletes
// are pending it co-drains with the folder (both go through drainTombstones' atomic swap), so it is
// safe to call from a foreground command.
func (s *Store) SyncPendingRemovals() {
	if s.tombPend.Load() > 0 {
		s.drainTombstones()
	}
}

// liveAt reports whether the record at off is still present in the hash index, the liveness probe
// the enumeration filter and the folder's re-check share. The composite key and kind are read from
// the record header (immutable, and the arena is grow-only so a deleted record's bytes are never
// reclaimed underneath this), so a node pointing at a since-deleted record resolves to not-found
// and is filtered out, while a node whose key was re-added resolves to the fresh live record.
func (s *Store) liveAt(off uint64) bool {
	return s.ExistsKind(s.keyAt(off), s.arena[off+offKind])
}
