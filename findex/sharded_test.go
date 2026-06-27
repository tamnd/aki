package findex

import (
	"strconv"
	"sync"
	"testing"
)

func TestShardedPutGetDelete(t *testing.T) {
	s := NewSharded(256, 50000)
	const n = 50000
	for i := 0; i < n; i++ {
		s.Put(key(i), []byte(strconv.Itoa(i)))
	}
	if s.Len() != n {
		t.Fatalf("Len = %d, want %d", s.Len(), n)
	}
	for i := 0; i < n; i++ {
		got, ok := s.Get(key(i))
		if !ok || string(got) != strconv.Itoa(i) {
			t.Fatalf("key %d = %q,%v", i, got, ok)
		}
	}
	for i := 0; i < n; i += 3 {
		if !s.Delete(key(i)) {
			t.Fatalf("delete %d false", i)
		}
	}
	for i := 0; i < n; i++ {
		_, ok := s.Get(key(i))
		if (i%3 == 0) == ok {
			t.Fatalf("key %d present=%v after delete-every-3", i, ok)
		}
	}
}

// TestShardedConcurrent hammers the index from many goroutines under -race to
// prove the per-shard locking is sound.
func TestShardedConcurrent(t *testing.T) {
	s := NewSharded(256, 100000)
	const keys = 20000
	for i := 0; i < keys; i++ {
		s.Put(key(i), []byte(strconv.Itoa(i)))
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				k := key((i + g*97) % keys)
				if i%4 == 0 {
					s.Put(k, []byte("x"))
				} else {
					s.Get(k)
				}
			}
		}(g)
	}
	wg.Wait()
}

// The parallel read benchmarks below settle doc 01/05's concurrency claim: a
// per-shard-locked index scales across cores where a single global lock and an
// RWMutex-guarded map plateau. Run with:
//
//	go test ./v2/findex -run XXX -bench 'BenchmarkParallelGet' -cpu 1,2,4,8 -benchtime 1s
//
// and read ns/op (lower is more aggregate throughput) across the -cpu steps.
const pbKeys = 1_000_000

func fillSharded() *Sharded {
	s := NewSharded(256, pbKeys)
	for i := 0; i < pbKeys; i++ {
		s.Put(key(i), []byte("0123456789abcdef"))
	}
	return s
}

func BenchmarkParallelGetSharded(b *testing.B) {
	s := fillSharded()
	ks := benchKeys(pbKeys)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(ks[i%pbKeys])
			i++
		}
	})
}

func BenchmarkParallelGetShardedLockFree(b *testing.B) {
	s := fillSharded()
	ks := benchKeys(pbKeys)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.GetLockFree(ks[i%pbKeys]) // read-only: no concurrent writer
			i++
		}
	})
}

func BenchmarkParallelGetGlobalMutex(b *testing.B) {
	ix := New(pbKeys)
	for i := 0; i < pbKeys; i++ {
		ix.Put(key(i), []byte("0123456789abcdef"))
	}
	var mu sync.Mutex
	ks := benchKeys(pbKeys)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.Lock()
			ix.Get(ks[i%pbKeys])
			mu.Unlock()
			i++
		}
	})
}

func BenchmarkParallelGetMapRWMutex(b *testing.B) {
	mp := make(map[string]valLoc, pbKeys)
	for i := 0; i < pbKeys; i++ {
		mp[string(key(i))] = valLoc{addr: uint64(i), vlen: 16}
	}
	var mu sync.RWMutex
	ks := benchKeys(pbKeys)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.RLock()
			_ = mp[string(ks[i%pbKeys])]
			mu.RUnlock()
			i++
		}
	})
}

func BenchmarkParallelGetSyncMap(b *testing.B) {
	var sm sync.Map
	for i := 0; i < pbKeys; i++ {
		sm.Store(string(key(i)), valLoc{addr: uint64(i), vlen: 16})
	}
	ks := benchKeys(pbKeys)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(string(ks[i%pbKeys]))
			i++
		}
	})
}
