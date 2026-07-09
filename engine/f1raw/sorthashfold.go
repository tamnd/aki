package f1raw

import (
	"sync"
	"sync/atomic"
)

// The sorted-hash fold facility maintains the per-partition sorted member-hash arrays (sorthash.go)
// off the set write path, the same way the tombstone folder (tombstone.go) maintains the ordered
// index off the delete path. The design lesson it implements is spec 2064/24 and the labs/seteager
// numbers: the set-algebra 2x lever is a two-pointer merge over hash-sorted arrays, which only wins
// if the array is ALREADY in hash order, so the sort has to happen continuously and off the reply
// path or a SADD pays the ~205 ns sorted insert the lab measured. This file is the "off the reply
// path" machine: a set write appends a foldDelta to the target partition's journal (one slice
// append under a tiny per-partition lock, not the stripe lock the command holds), lists the
// partition on a lock-free dirty stack once, and returns at vector-append speed; a background
// folder pops the dirty stack and applies each partition's accumulated deltas through
// sortedHashes.foldBatch on its own schedule, so the sorted array is always materialized rather
// than rebuilt on the first algebra call.
//
// This slice is the facility in isolation: the registry, the journal, the folder, and the enable
// and drain surface, exercised by its own tests. The set command path does not call shAppend yet,
// because the engine's insert and remove primitives are not handed the member bytes today, so
// threading the member hash onto the real SADD/SREM path is a focused follow-on. Keeping the
// facility a standalone, flag-gated unit lets it be reviewed and tested against a synthetic delta
// stream before it carries live traffic, and a store that never calls EnableSortedHashFold pays
// nothing, exactly like the tombstone folder.
//
// Why a per-partition journal and not one global stack like the tombstone folder. The tombstone
// folder's target is a single global structure (the ordered index under one lock), so one stack of
// batches is the whole story. The sorted arrays are per partition (a member routes to
// hash(m)&(P-1), doc 19), and foldBatch mutates one partition's working arrays with no lock,
// correct only because a single folder goroutine touches a given partition at a time. So each
// partition owns its own journal and its own sorted array, and the dirty stack lists which
// partitions have pending work; the folder drains partition by partition. The producer and the
// folder coordinate through the partition's queued flag: the producer lists a partition on the
// stack only on the false-to-true transition of that flag, so a burst of writes to one partition
// costs one stack push, and the folder clears the flag before it reads the journal, so a write
// that lands between the clear and the read re-lists the partition rather than being lost.

// shRegShards is the fan-out of the partition registry's striped map. It matches the member-vector
// shard count (randvec.go) so the two structures spread a hot keyspace across the same number of
// locks; the registry lock is taken only to find-or-create a partition's state, never on the fold
// or the merge, so it is off every hot path.
const shRegShards = 256

// shReg is the registry of per-partition sorted-hash state, keyed by the partition prefix (the same
// prefix the member vector uses: the set key for an unpartitioned set, the key plus a partition
// byte for a partitioned one). It is a striped map so find-or-create on one partition does not block
// another, mirroring randVec.
type shReg struct {
	shards [shRegShards]shRegShard
}

type shRegShard struct {
	mu sync.Mutex
	m  map[string]*shPart
}

// shPart is one partition's fold state: its sorted array plus the journal of deltas not yet folded
// into it. jrnl is appended under jmu by the producer and swapped out under jmu by the folder, a
// short critical section on a lock distinct from the command's stripe lock so the fold never
// contends with the reply path. enq counts every delta ever appended and folded records the count
// the folder has applied; the partition is current (the merge can trust its sorted array) exactly
// when folded equals enq, equivalently when jrnl is empty. queued is the dirty-stack membership
// flag and dnext is the stack link, both touched only through the producer's CAS list and the
// folder's atomic swap.
type shPart struct {
	prefix string
	sorted *sortedHashes

	jmu    sync.Mutex
	jrnl   []foldDelta
	enq    uint64
	folded uint64

	queued atomic.Bool
	dnext  *shPart
}

// part finds or creates the fold state for a partition prefix. The prefix bytes are copied into the
// map key string on create, so the caller's buffer is free to reuse. The lock is held only for the
// map lookup and the rare insert.
func (r *shReg) part(prefix []byte) *shPart {
	sh := &r.shards[hash(prefix)&(shRegShards-1)]
	sh.mu.Lock()
	if sh.m == nil {
		sh.m = make(map[string]*shPart)
	}
	p := sh.m[string(prefix)]
	if p == nil {
		key := string(prefix)
		p = &shPart{prefix: key, sorted: newSortedHashes(0)}
		sh.m[key] = p
	}
	sh.mu.Unlock()
	return p
}

// EnableSortedHashFold builds the partition registry and starts the folder goroutine. Like
// EnableDeferredRemoval it is a startup call, made once before the store serves traffic: the write
// path reads shOn without synchronization to decide whether to journal a delta, so the registry and
// the channels must exist before the flag is published, which is why shOn is stored last. A store
// that never calls it keeps shReg nil and never journals, so the set path is exactly as it was. The
// folder is stopped and its pending journals applied by Close.
func (s *Store) EnableSortedHashFold() {
	if s.shOn.Load() {
		return
	}
	s.shReg = &shReg{}
	s.shStop = make(chan struct{})
	s.shDone = make(chan struct{})
	// A single-slot wake channel: a producer signals it after listing a dirty partition without
	// ever blocking (the default branch drops the signal when one is already pending), so a burst
	// of writes costs one non-blocking send each and the folder coalesces them into one wake.
	s.shWake = make(chan struct{}, 1)
	s.shOn.Store(true)
	go s.shFoldLoop()
}

// shAppend journals one member change for the partition at prefix and lists the partition for the
// folder. memberHash is hash64(member) and off is the member record's arena offset; add is true for
// a SADD and false for a SREM. It is called under the command's stripe or partition lock, right
// beside the member vector's add or remove, and it does the minimum there: one slice append under
// the partition's own small lock, then a lock-free stack push only if the partition was not already
// listed. The sort is entirely the folder's job. Calling it without EnableSortedHashFold panics on
// a nil registry, which never happens because the write path journals only when shOn is set.
func (s *Store) shAppend(prefix []byte, memberHash, off uint64, add bool) {
	p := s.shReg.part(prefix)
	p.jmu.Lock()
	p.jrnl = append(p.jrnl, foldDelta{hash: memberHash, off: off, add: add})
	p.enq++
	p.jmu.Unlock()
	s.shPend.Add(1)
	if p.queued.CompareAndSwap(false, true) {
		for {
			head := s.shDirty.Load()
			p.dnext = head
			if s.shDirty.CompareAndSwap(head, p) {
				break
			}
		}
	}
	select {
	case s.shWake <- struct{}{}:
	default:
	}
}

// shFoldLoop is the background folder: it drains the dirty stack, and when the stack is empty it
// parks until a producer wakes it or Close stops it. Under sustained write load the drain keeps
// returning work so the loop never parks; when writes stop it blocks, so an idle store spends
// nothing. On stop it does one final drain so no journal outlives the folder.
func (s *Store) shFoldLoop() {
	defer close(s.shDone)
	for {
		if s.shDrain() > 0 {
			select {
			case <-s.shStop:
				s.shDrain()
				return
			default:
			}
			continue
		}
		select {
		case <-s.shStop:
			s.shDrain()
			return
		case <-s.shWake:
		}
	}
}

// shDrain swaps the whole dirty stack out in one atomic step and folds every listed partition,
// returning the number of deltas it applied. It holds shMu across the drain so a foreground
// SyncSortedHashes that wants the arrays current blocks on an in-flight fold instead of racing it.
// For each partition it clears the queued flag before it reads the journal, so a write that appends
// after the swap re-lists the partition (the folder will pop it again and fold the newcomer) rather
// than being lost. An empty journal, the residue of such a re-list, folds to nothing: the sorted
// array already reflects every delta up to enq, so there is no work and the partition stays current.
func (s *Store) shDrain() int {
	s.shMu.Lock()
	defer s.shMu.Unlock()
	head := s.shDirty.Swap(nil)
	if head == nil {
		return 0
	}
	total := 0
	for p := head; p != nil; {
		next := p.dnext
		p.dnext = nil
		// Allow a re-list before reading the journal: a producer appending between here and the
		// journal swap sees queued false, pushes the partition again, and the folder folds the new
		// delta on the next pop rather than dropping it.
		p.queued.Store(false)
		p.jmu.Lock()
		batch := p.jrnl
		p.jrnl = nil
		gen := p.enq
		p.jmu.Unlock()
		if len(batch) > 0 {
			p.sorted.foldBatch(batch, gen)
			total += len(batch)
		}
		// Record the applied generation under jmu, the same lock SortedHashCurrent reads it and enq
		// under, so a foreground currency check never races the fold. foldBatch has already published
		// the new snapshot by here, and jmu carries that publish to a reader that observes folded, so
		// folded == enq implies the snapshot reflects every appended delta. A producer that appended
		// between the swap and here bumped enq and re-listed the partition, so folded stays below enq
		// and the partition reads not-current until the next drain, which is correct.
		p.jmu.Lock()
		p.folded = gen
		p.jmu.Unlock()
		p = next
	}
	s.shPend.Add(int64(-total))
	return total
}

// SyncSortedHashes applies any outstanding journals synchronously, so a caller that needs every
// partition's sorted array reconciled with its member vector right now (a merge that will
// two-pointer over the arrays and must not miss a just-added member) can force the fold. It is a
// no-op when nothing is pending, the steady state for an algebra workload not interleaved with
// writes, so a read pays only one atomic load. When work is pending it co-drains with the folder
// (both go through shDrain's atomic swap under shMu), so it is safe to call from a foreground
// command.
func (s *Store) SyncSortedHashes() {
	if s.shOn.Load() && s.shPend.Load() > 0 {
		s.shDrain()
	}
}

// SortedHashSnapshot returns the current published sorted array for a partition prefix, or nil if
// the fold facility is off or the partition has never been written. The snapshot may lag the member
// vector by the partition's pending journal; a caller that needs it current calls SyncSortedHashes
// first. It is the read surface the merge path consumes.
func (s *Store) SortedHashSnapshot(prefix []byte) *sortedSnap {
	if s.shReg == nil {
		return nil
	}
	p := s.shReg.part(prefix)
	return p.sorted.load()
}

// SortedHashCurrent reports whether a partition's sorted array reflects every delta appended to it,
// the condition the merge checks before trusting the array over the probe fallback. It reads the
// counters under the partition's journal lock so the answer is consistent with a concurrent append.
func (s *Store) SortedHashCurrent(prefix []byte) bool {
	if s.shReg == nil {
		return false
	}
	p := s.shReg.part(prefix)
	p.jmu.Lock()
	current := p.folded == p.enq
	p.jmu.Unlock()
	return current
}
