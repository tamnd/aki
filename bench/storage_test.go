package bench_test

import (
	"strconv"
	"testing"

	"github.com/tamnd/aki/btree"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// newBenchTree builds an in-memory B-tree over a pager, the storage path the
// buffer pool fronts. These benchmarks stand in for the spec's separate
// skiplist and buffer-pool suites: aki stores every type in one paged B-tree,
// so the tree read/write path is the hot path those suites describe.
func newBenchTree(b *testing.B) *btree.Tree {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.aki", pager.Options{PageSize: 4096})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	tr, err := btree.Create(p)
	if err != nil {
		b.Fatalf("create tree: %v", err)
	}
	return tr
}

// BenchmarkTreePut measures an insert into the paged B-tree.
func BenchmarkTreePut(b *testing.B) {
	tr := newBenchTree(b)
	val := []byte("value")
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k := []byte("key:" + strconv.Itoa(i))
		if err := tr.Put(k, val); err != nil {
			b.Fatalf("put: %v", err)
		}
		i++
	}
}

// BenchmarkTreeGet measures a lookup that hits the buffer pool warm.
func BenchmarkTreeGet(b *testing.B) {
	tr := newBenchTree(b)
	const n = 10000
	keys := make([][]byte, n)
	for i := range n {
		k := []byte("key:" + strconv.Itoa(i))
		keys[i] = k
		if err := tr.Put(k, []byte("value")); err != nil {
			b.Fatalf("seed put: %v", err)
		}
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		k := keys[i%n]
		if _, ok, err := tr.Get(k); err != nil || !ok {
			b.Fatalf("get %s: ok=%v err=%v", k, ok, err)
		}
		i++
	}
}
