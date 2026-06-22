package bench_test

import (
	"strconv"
	"testing"

	"github.com/tamnd/aki/keyspace"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

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
