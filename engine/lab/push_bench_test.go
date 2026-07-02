package lab

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Technique question: what coordinates concurrent appends to one hot list?
//
// A single-hot-list append (RPUSH/LPUSH hammering one key from many connections) is the
// one write the saturation sweep cannot push past parity. On the GamingPC 32-core box,
// core_scale.csv shows GET and HSET riding every core (HSET 2.6M at one core to 15.5M at
// fourteen) while rpushtail sits dead flat near 1.3M from one core to fourteen, below
// Redis's single-threaded 2.0M. HSET spreads across keys and shards, so it scales; a hot
// list is one key, and today every appender to it serializes on that key's stripe mutex.
// The lock, not the CPU, is the ceiling, so more cores buy nothing.
//
// An append needs three things from the shared list header: reserve the next position,
// record the element at that position, and add the element's bytes to the running size.
// The element write already lands at a unique position, so two appenders never touch the
// same element cell; the only real contention is reserving the position and bumping the
// size. Three ways to coordinate that:
//
//   - mutex: a sync.Mutex held across reserve, record, and size bump. This is today's
//     stripe lock. Correct and simple, but every appender to the key parks behind the one
//     holder, and the critical section spans the element write, so the lock is held for
//     the whole publish, not just the counter bump.
//   - atomic: the tail is an atomic counter. An appender reserves its slot with one
//     FetchAdd, writes its element outside any lock, then adds its bytes to an atomic
//     size. No parking and the element write is off the critical path, but two contended
//     atomics (tail and size) still ping-pong the same cache lines across cores.
//   - striped: reserve the slot with the same atomic FetchAdd on the tail, but accumulate
//     bytes in per-CPU striped counters summed only when a reader needs the total. The
//     tail stays a single shared atomic (order requires it), but the size counter stops
//     being a second contended line, so a steady append touches exactly one shared word.
//
// Measured at 1, 4, and 16 cores so the serialization is visible rather than asserted:
// the mutex should flatten as cores rise the way rpushtail does on the box, and the
// lock-free variants should keep climbing.

// pushElems is how many element cells each list pre-sizes for the benchmark. It is large
// enough that b.N appends across the run never wrap, so no appender ever waits on space
// and the benchmark times coordination alone. The cell is a plain counter stand-in for
// the element row f1raw publishes at each position, which lands lock-free at a unique
// index in the real engine, so writing our own unique index models it without dragging
// the whole store into the measurement.
const pushElems = 1 << 24

// mutexList guards the header with one lock, the way the stripe mutex does today.
type mutexList struct {
	mu    sync.Mutex
	tail  int64
	bytes int64
	cell  []int64
}

func newMutexList() *mutexList { return &mutexList{cell: make([]int64, pushElems)} }

func (l *mutexList) push(elemLen int64) {
	l.mu.Lock()
	pos := l.tail
	l.tail++
	l.cell[pos&(pushElems-1)] = elemLen
	l.bytes += elemLen
	l.mu.Unlock()
}

// atomicList reserves the position with one FetchAdd and keeps the size in a second
// shared atomic. The element write happens outside any lock at the reserved slot.
type atomicList struct {
	tail  atomic.Int64
	bytes atomic.Int64
	cell  []int64
}

func newAtomicList() *atomicList { return &atomicList{cell: make([]int64, pushElems)} }

func (l *atomicList) push(elemLen int64) {
	pos := l.tail.Add(1) - 1
	l.cell[pos&(pushElems-1)] = elemLen
	l.bytes.Add(elemLen)
}

// stripedList reserves the position with the same FetchAdd but scatters the size bump
// across per-shard counters, so a steady append touches only the tail line. The size is
// the sum of the shards, computed on demand by a reader. numSizeShards is a power of two
// so the index mask is a single and.
const numSizeShards = 64

type sizeShard struct {
	n atomic.Int64
	_ [56]byte // pad to a cache line so shards do not share one
}

type stripedList struct {
	tail  atomic.Int64
	sizes [numSizeShards]sizeShard
	cell  []int64
}

func newStripedList() *stripedList { return &stripedList{cell: make([]int64, pushElems)} }

func (l *stripedList) push(elemLen int64, shard int) {
	pos := l.tail.Add(1) - 1
	l.cell[pos&(pushElems-1)] = elemLen
	l.sizes[shard&(numSizeShards-1)].n.Add(elemLen)
}

func (l *stripedList) size() int64 {
	var n int64
	for i := range l.sizes {
		n += l.sizes[i].n.Load()
	}
	return n
}

// elemLen is a representative small element size, the 64-byte value the sweep pushes.
const elemLen = 64

func BenchmarkPushMutex(b *testing.B) {
	l := newMutexList()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.push(elemLen)
		}
	})
}

func BenchmarkPushAtomic(b *testing.B) {
	l := newAtomicList()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.push(elemLen)
		}
	})
}

func BenchmarkPushStriped(b *testing.B) {
	l := newStripedList()
	var shardSeq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each parallel goroutine claims a stable shard so its size bumps stay on one
		// line, the way a per-P counter would bind to the running core.
		shard := int(shardSeq.Add(1))
		for pb.Next() {
			l.push(elemLen, shard)
		}
	})
	if l.size() != int64(b.N)*elemLen {
		b.Fatalf("size drift: got %d want %d", l.size(), int64(b.N)*elemLen)
	}
}
