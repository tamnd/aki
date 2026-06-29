package store

import (
	"encoding/binary"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// These benchmarks are the permanent, in-repo version of the /tmp/locktax probe:
// they measure the read-guard cost on the REAL shard structure (the published
// index view over real log pages), not an isolated uint64 table, so the number
// reflects what a GET actually pays. Run the guard sweep to see where the
// lock-free read overtakes the RWMutex read as cores climb:
//
//	go test ./store/ -run x -bench BenchmarkReadGuard -benchtime=1s -cpu 1,2,4,8
//
// The point of keeping them committed is that every perf claim about the read
// path can be re-grounded on the target box in one command, rather than trusting
// the spec. The spec's prediction (rewrite/01: lock-free reads scale, RWMutex
// reads plateau a few cores in) is a hypothesis these numbers confirm or refute
// per box.

// readGuardShard builds one fully-resident shard preloaded with n small keys and
// returns it plus the key set, so a guard benchmark indexes precomputed keys
// rather than formatting per op.
func readGuardShard(b *testing.B, n int) (*shard, [][]byte) {
	b.Helper()
	sh, err := newShard(0, Tunables{Shards: 1, PageSize: 1 << 20})
	if err != nil {
		b.Fatalf("newShard: %v", err)
	}
	keys := make([][]byte, n)
	val := make([]byte, benchValLen)
	for i := range keys {
		keys[i] = []byte(strconv.Itoa(i))
		sh.mu.Lock()
		if _, err := sh.setLocked(keys[i], val); err != nil {
			b.Fatalf("set: %v", err)
		}
		sh.mu.Unlock()
	}
	return sh, keys
}

// setLocked is the set body assuming the caller holds mu; the guard benchmarks
// preload through it so they do not double-lock.
func (sh *shard) setLocked(key, value []byte) (int, error) {
	v := sh.view.Load()
	rl := recordLen(key, value)
	if sh.tailPos+rl > sh.pageSize {
		sh.tailPage++
		sh.tailPos = 0
		newPages := make([][]byte, len(v.pages)+1)
		copy(newPages, v.pages)
		newPages[sh.tailPage] = make([]byte, sh.pageSize)
		nv := &view{slots: v.slots, mask: v.mask, pages: newPages}
		sh.view.Store(nv)
		v = nv
		sh.diskOff = append(sh.diskOff, 0)
		sh.residentOrder = append(sh.residentOrder, sh.tailPage)
	}
	page := v.pages[sh.tailPage]
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	off := sh.tailPos
	binary.LittleEndian.PutUint32(page[off:], uint32(len(key)))
	binary.LittleEndian.PutUint32(page[off+4:], uint32(len(value)))
	copy(page[off+recHdr:], key)
	copy(page[off+recHdr+len(key):], value)
	sh.tailPos += rl
	return sh.indexPut(key, uint64(recStart)), nil
}

// readGuardSweep runs probe under the given guard across b.N parallel iterations.
func readGuardSweep(b *testing.B, n int, guard func(sh *shard, key []byte)) {
	sh, keys := readGuardShard(b, n)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 2463534242
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			guard(sh, keys[int(x)%n])
		}
	})
}

const readGuardKeys = 1_000_000

// BenchmarkReadGuardLockFree is the shipped path: atomic view load, atomic entry
// load, no mutex. This is what getResident does.
func BenchmarkReadGuardLockFree(b *testing.B) {
	readGuardSweep(b, readGuardKeys, func(sh *shard, key []byte) {
		_, _, _ = sh.getResident(key)
	})
}

// BenchmarkReadGuardRWMutex is the old path: a per-shard RWMutex.RLock around the
// same probe. The gap to lock-free is the tax the rewrite removes.
func BenchmarkReadGuardRWMutex(b *testing.B) {
	readGuardSweep(b, readGuardKeys, func(sh *shard, key []byte) {
		sh.mu.RLock()
		v := sh.view.Load()
		sh.indexGet(v, key)
		sh.mu.RUnlock()
	})
}

// BenchmarkReadGuardMutex is a plain Mutex around the probe, the worst sharded
// guard, for reference (it serializes readers within a shard).
func BenchmarkReadGuardMutex(b *testing.B) {
	var mu sync.Mutex
	readGuardSweep(b, readGuardKeys, func(sh *shard, key []byte) {
		mu.Lock()
		v := sh.view.Load()
		sh.indexGet(v, key)
		mu.Unlock()
	})
}

// BenchmarkWriteGuardAtomicStore measures the slot-install cost the lock-free read
// path imposes on writers: an atomic store (XCHG on amd64) instead of a plain
// store. This is the other side of the trade and the SET-regression suspect, kept
// committed so the write-side cost is never invisible.
func BenchmarkWriteGuardAtomicStore(b *testing.B) {
	slots := make([]uint64, 1<<20)
	mask := uint64(len(slots) - 1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 88172645
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			atomic.StoreUint64(&slots[uint64(x)&mask], uint64(x)|1)
		}
	})
}

// BenchmarkWriteGuardPlainStore is the same store without the atomic, the lower
// bound a non-lock-free writer would pay. The gap to AtomicStore is the per-write
// price of lock-free reads.
func BenchmarkWriteGuardPlainStore(b *testing.B) {
	slots := make([]uint64, 1<<20)
	mask := uint64(len(slots) - 1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var x uint32 = 88172645
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			slots[uint64(x)&mask] = uint64(x) | 1
		}
	})
}
