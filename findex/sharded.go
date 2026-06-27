package findex

import (
	"sync"
	"sync/atomic"
)

// Sharded is the multi-core form of the index: N independent Index shards, each
// behind its own mutex, the key's shard chosen by the high bits of its hash so
// the shard pick is independent of the per-shard probe bits. This is slice 1 of
// doc 01's concurrency plan: a per-shard lock spreads contention across cores
// the way the v2 store's 256-shard map did, and it is the baseline the
// latch-free CAS insert later replaces. Reads take the shard lock here; the
// lock-free atomic-load read path is the next slice (it needs the per-shard log
// to never reallocate under a reader, which the spike's growable []byte does
// not yet guarantee).
type Sharded struct {
	shards []shard
	mask   uint64
}

type shard struct {
	mu sync.Mutex
	ix *Index
	_  [40]byte // pad to keep each shard's mutex on its own cache line
}

// NewSharded returns an index of n shards (rounded up to a power of two) sized
// for hint total keys.
func NewSharded(n, hint int) *Sharded {
	cnt := uint64(1)
	for cnt < uint64(n) {
		cnt <<= 1
	}
	perShard := hint/int(cnt) + 1
	s := &Sharded{shards: make([]shard, cnt), mask: cnt - 1}
	for i := range s.shards {
		s.shards[i].ix = New(perShard)
	}
	return s
}

// shardFor picks the shard from the top bits of the hash, leaving the low bits
// for the per-shard slot index so the two are independent.
func (s *Sharded) shardFor(key []byte) (*shard, uint64) {
	h := hash64(key)
	return &s.shards[(h>>40)&s.mask], h
}

func (s *Sharded) Get(key []byte) ([]byte, bool) {
	sh, _ := s.shardFor(key)
	sh.mu.Lock()
	v, ok := sh.ix.Get(key)
	if ok {
		// copy out under the lock: the per-shard log may reallocate on a
		// concurrent Put, so a returned slice into it is not safe to hold.
		cp := make([]byte, len(v))
		copy(cp, v)
		sh.mu.Unlock()
		return cp, true
	}
	sh.mu.Unlock()
	return nil, false
}

// GetLockFree is the target read path from doc 01 slice 1 and doc 05 RANK 3:
// no lock, an atomic load of each probed slot, and a zero-copy return slice
// into the log. It is safe for concurrent readers only when no writer is
// growing or appending to that shard concurrently, because the spike's log is
// a growable []byte that may reallocate under a reader. The shipping engine
// gets this guarantee from fixed, non-reallocating mmap pages (doc 02/05), at
// which point this becomes the always-on read path. It is benchmarked here to
// show the structure's true read ceiling, separate from the lock-and-copy that
// the growable-slice spike otherwise needs.
func (s *Sharded) GetLockFree(key []byte) ([]byte, bool) {
	sh, h := s.shardFor(key)
	ix := sh.ix
	tag := tagOf(h)
	i := h & ix.mask
	for {
		e := atomic.LoadUint64(&ix.slots[i])
		if e == 0 {
			return nil, false
		}
		if entryTag(e) == tag {
			addr := entryAddr(e)
			if bytesEqual(ix.recKey(addr), key) {
				return ix.recVal(addr), true
			}
		}
		i = (i + 1) & ix.mask
	}
}

func (s *Sharded) Put(key, val []byte) {
	sh, _ := s.shardFor(key)
	sh.mu.Lock()
	sh.ix.Put(key, val)
	sh.mu.Unlock()
}

func (s *Sharded) Delete(key []byte) bool {
	sh, _ := s.shardFor(key)
	sh.mu.Lock()
	ok := sh.ix.Delete(key)
	sh.mu.Unlock()
	return ok
}

// Len sums the live key counts across shards.
func (s *Sharded) Len() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		n += s.shards[i].ix.Len()
		s.shards[i].mu.Unlock()
	}
	return n
}

// IndexBytes sums the resident table bytes across shards.
func (s *Sharded) IndexBytes() int {
	n := 0
	for i := range s.shards {
		s.shards[i].mu.Lock()
		n += s.shards[i].ix.IndexBytes()
		s.shards[i].mu.Unlock()
	}
	return n
}
