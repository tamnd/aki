package bench_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// g5Tags are hash-tagged key prefixes that each route to a distinct keyspace
// shard, verified by TestG5TagsDistinct. Using {tag} syntax forces all keys
// with the same tag to the same shard regardless of the suffix.
var g5Tags = [keyspace.NumShards]string{
	"{g6}", // shard 0
	"{g7}", // shard 1
	"{g4}", // shard 2
	"{g5}", // shard 3
	"{g2}", // shard 4
	"{g3}", // shard 5
	"{g0}", // shard 6
	"{g1}", // shard 7
}

// TestG5TagsDistinct verifies every g5Tags entry maps to a unique shard.
func TestG5TagsDistinct(t *testing.T) {
	seen := make(map[int]string, keyspace.NumShards)
	for i, tag := range g5Tags {
		k := []byte(tag + ":key:000000")
		s := keyspace.ShardOf(k)
		if prev, dup := seen[s]; dup {
			t.Fatalf("g5Tags[%d]=%q and g5Tags (prev %q) both map to shard %d", i, tag, prev, s)
		}
		seen[s] = tag
	}
}

// newBenchDB opens an in-memory keyspace and returns database 0, the storage
// path behind GET/SET. aki keeps keys in a paged B-tree rather than an in-RAM
// hash, so these benchmarks cover the real lookup path, not a separate dict.
func newBenchDB(b *testing.B) *keyspace.DB {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.aki", pager.Options{PageSize: 4096, DBCount: 16})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	ks, err := keyspace.Open(p)
	if err != nil {
		b.Fatalf("open keyspace: %v", err)
	}
	db, err := ks.DB(0)
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	return db
}

// seedKeys writes n string keys named key:0..n-1 for read benchmarks.
func seedKeys(b *testing.B, db *keyspace.DB, n int) [][]byte {
	b.Helper()
	keys := make([][]byte, n)
	for i := range n {
		k := []byte("key:" + strconv.Itoa(i))
		keys[i] = k
		if err := db.Set(k, []byte("value"), 0, 0, -1); err != nil {
			b.Fatalf("seed set: %v", err)
		}
	}
	return keys
}

// BenchmarkDictGet measures a point lookup over a populated keyspace.
func BenchmarkDictGet(b *testing.B) {
	db := newBenchDB(b)
	keys := seedKeys(b, db, 10000)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k := keys[i%len(keys)]
		if _, _, found, err := db.Get(k); err != nil || !found {
			b.Fatalf("get %s: found=%v err=%v", k, found, err)
		}
		i++
	}
}

// BenchmarkDictSet measures a key insert/overwrite.
func BenchmarkDictSet(b *testing.B) {
	db := newBenchDB(b)
	val := []byte("value")
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k := []byte("key:" + strconv.Itoa(i))
		if err := db.Set(k, val, 0, 0, -1); err != nil {
			b.Fatalf("set: %v", err)
		}
		i++
	}
}

// BenchmarkDictSetExpiry measures an insert that also carries a TTL.
func BenchmarkDictSetExpiry(b *testing.B) {
	db := newBenchDB(b)
	val := []byte("value")
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k := []byte("key:" + strconv.Itoa(i))
		if err := db.Set(k, val, 0, 0, 60000); err != nil {
			b.Fatalf("set ttl: %v", err)
		}
		i++
	}
}

// BenchmarkDictDelete measures key removal from a populated keyspace.
func BenchmarkDictDelete(b *testing.B) {
	db := newBenchDB(b)
	keys := seedKeys(b, db, 100000)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i >= len(keys) {
			b.StopTimer()
			keys = seedKeys(b, db, 100000)
			i = 0
			b.StartTimer()
		}
		if _, err := db.Delete(keys[i]); err != nil {
			b.Fatalf("delete: %v", err)
		}
		i++
	}
}

// BenchmarkDictGetParallel measures concurrent hot-cache reads. This is the
// primary target of the lock-free hot-GET bypass: under high read concurrency
// the old e.mu.RLock() became a bottleneck; HotGet bypasses it entirely.
func BenchmarkDictGetParallel(b *testing.B) {
	db := newBenchDB(b)
	keys := seedKeys(b, db, 10000)
	// Warm the hot cache by reading each key once.
	for _, k := range keys {
		if _, _, found, err := db.Get(k); err != nil || !found {
			b.Fatalf("warm get %s: found=%v err=%v", k, found, err)
		}
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := keys[i%len(keys)]
			if _, _, ok := db.HotGet(k); !ok {
				// Miss is fine on a race with eviction; fall back.
				if _, _, found, err := db.Get(k); err != nil || !found {
					b.Fatalf("get %s: found=%v err=%v", k, found, err)
				}
			}
			i++
		}
	})
}

// BenchmarkDictSetParallelDisjoint is the G5 multi-core scaling benchmark
// (spec doc 00 §G5). It runs NumShards goroutines concurrently, each writing
// to keys that all route to a different shard, so the shard mutexes are fully
// independent. The aggregate throughput divided by a single-goroutine run
// should reach at least 4x on an 8-core machine.
func BenchmarkDictSetParallelDisjoint(b *testing.B) {
	db := newBenchDB(b)
	val := []byte("value")
	b.ReportAllocs()
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	b.RunParallel(func(pb *testing.PB) {
		// Each parallel goroutine picks one of the NumShards tag slots.
		// Go's testing framework may use more goroutines than NumShards;
		// mod ensures no out-of-bounds access and reuse is fine for the
		// scaling measurement.
		var myID int
		mu.Lock()
		myID = total % keyspace.NumShards
		total++
		mu.Unlock()
		tag := g5Tags[myID]
		wg.Add(1)
		defer wg.Done()
		i := 0
		for pb.Next() {
			k := []byte(fmt.Sprintf("%s:key:%08d", tag, i))
			if err := db.Set(k, val, 0, 0, -1); err != nil {
				b.Fatalf("set: %v", err)
			}
			i++
		}
	})
	wg.Wait()
}
