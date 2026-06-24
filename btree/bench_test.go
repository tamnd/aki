package btree

import (
	"encoding/binary"
	"testing"

	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// benchTree builds a tree of n keys at a small page size so the tree is several
// levels deep, which is where the interior-descent cost the search-on-page path
// removes actually shows up. Keys are fixed-width big-endian so order is the
// numeric order and the spread of descent paths is even.
func benchTree(b *testing.B, n int, pageSize uint32) *Tree {
	b.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "bench.aki", pager.Options{PageSize: pageSize})
	if err != nil {
		b.Fatalf("create pager: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })
	tr, err := Create(p)
	if err != nil {
		b.Fatalf("create tree: %v", err)
	}
	var k [8]byte
	val := make([]byte, 16)
	for i := range n {
		binary.BigEndian.PutUint64(k[:], uint64(i))
		if err := tr.Put(k[:], val); err != nil {
			b.Fatalf("put %d: %v", i, err)
		}
	}
	return tr
}

func benchKey(i, n int) [8]byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(i%n))
	return k
}

// BenchmarkGetDeep measures the read descent on a multi-level tree. The
// search-on-page path reads separators and the leaf value straight off the
// pinned pages, so it should allocate nothing per Get.
func BenchmarkGetDeep(b *testing.B) {
	const n = 200000
	tr := benchTree(b, n, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		k := benchKey(i, n)
		if _, ok, err := tr.Get(k[:]); err != nil || !ok {
			b.Fatalf("get miss at %d: ok=%v err=%v", i, ok, err)
		}
	}
}

// BenchmarkUpsertDeep measures the write descent (no split: the keys already
// exist, so each Upsert replaces a value in place). Interior levels are
// descended by search-on-page and never decoded, since no separator changes.
func BenchmarkUpsertDeep(b *testing.B) {
	const n = 200000
	tr := benchTree(b, n, 4096)
	val := make([]byte, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		k := benchKey(i, n)
		if _, err := tr.Upsert(k[:], val); err != nil {
			b.Fatalf("upsert at %d: %v", i, err)
		}
	}
}
