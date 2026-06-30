package lab

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Technique question: which primitive guards a record's value cell?
//
// Same-size value updates are the steady-state write. Three ways to make the update
// safe against concurrent readers:
//
//   - seqlock: a per-record version word, low bit set while a writer holds it. The
//     writer flips it, memcpys in place, flips it back; the reader copies between two
//     even reads of the version and retries if it moved. No allocation, no reader lock.
//     This is what f1raw uses, and the benchmark is here to justify it.
//   - rwmutex: a sync.RWMutex over the cell. Readers take the read lock, the writer the
//     write lock. Correct and simple, but the read lock is a contended atomic on a
//     shared word, the lock tax note 355 is about.
//   - rcu: the value is an atomic pointer to an immutable slice. Readers load the
//     pointer with no copy; the writer allocates a new slice and swaps it. Reads are the
//     cheapest possible, but every write allocates and the old slice becomes garbage,
//     the 128 B/op the integrated engine pays.
//
// Measured single-thread and at 10 cores, separately for the read-mostly and the
// write-mostly case, so the tradeoff (seqlock's zero-alloc writes vs rcu's
// copy-free reads vs rwmutex's lock tax) is visible rather than asserted.

const cellCap = 64

// --- seqlock cell ---

type seqCell struct {
	ver atomic.Uint32
	n   atomic.Uint32
	buf [cellCap]byte
}

func (c *seqCell) set(v []byte) {
	for {
		x := c.ver.Load()
		if x&1 != 0 {
			continue
		}
		if c.ver.CompareAndSwap(x, x+1) {
			copy(c.buf[:], v)
			c.n.Store(uint32(len(v)))
			c.ver.Store(x + 2)
			return
		}
	}
}

func (c *seqCell) get(dst []byte) []byte {
	for {
		x := c.ver.Load()
		if x&1 != 0 {
			continue
		}
		n := c.n.Load()
		dst = append(dst[:0], c.buf[:n]...)
		if c.ver.Load() == x {
			return dst
		}
	}
}

// --- rwmutex cell ---

type muCell struct {
	mu  sync.RWMutex
	buf []byte
}

func (c *muCell) set(v []byte) {
	c.mu.Lock()
	c.buf = append(c.buf[:0], v...)
	c.mu.Unlock()
}

func (c *muCell) get(dst []byte) []byte {
	c.mu.RLock()
	dst = append(dst[:0], c.buf...)
	c.mu.RUnlock()
	return dst
}

// --- rcu cell ---

type rcuCell struct {
	p atomic.Pointer[[]byte]
}

func (c *rcuCell) set(v []byte) {
	b := append([]byte(nil), v...)
	c.p.Store(&b)
}

func (c *rcuCell) get() []byte { return *c.p.Load() }

func newCells() (*seqCell, *muCell, *rcuCell) {
	v := make([]byte, cellCap)
	s := &seqCell{}
	s.set(v)
	m := &muCell{}
	m.set(v)
	r := &rcuCell{}
	r.set(v)
	return s, m, r
}

func BenchmarkUpdateSeqlockSet(b *testing.B) {
	s, _, _ := newCells()
	v := make([]byte, cellCap)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.set(v)
		}
	})
}

func BenchmarkUpdateRWMutexSet(b *testing.B) {
	_, m, _ := newCells()
	v := make([]byte, cellCap)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.set(v)
		}
	})
}

func BenchmarkUpdateRCUSet(b *testing.B) {
	_, _, r := newCells()
	v := make([]byte, cellCap)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.set(v)
		}
	})
}

func BenchmarkUpdateSeqlockGet(b *testing.B) {
	s, _, _ := newCells()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		for pb.Next() {
			dst = s.get(dst)
		}
	})
}

func BenchmarkUpdateRWMutexGet(b *testing.B) {
	_, m, _ := newCells()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		for pb.Next() {
			dst = m.get(dst)
		}
	})
}

func BenchmarkUpdateRCUGet(b *testing.B) {
	_, _, r := newCells()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x int
		for pb.Next() {
			x += len(r.get())
		}
		_ = x
	})
}
