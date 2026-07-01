package f1raw

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

// This lab benchmark settles the ordered-structure decision the hash model left open
// (spec 2064/f1_rewrite_ltm/05 and 03): enumerate one collection's members in key order
// either through the per-collection sorted run (the skip-list ordered index, the
// implemented path) or by scanning the whole hash index and sorting the survivors (the
// rejected alternative). It runs both over one target hash embedded in a much larger
// keyspace, which is the case that separates them: the ordered index seeks straight to
// the target's prefix run and walks it, while the full scan pays for every unrelated
// record on every enumeration.
//
// Run: go test ./engine/f1raw/ -run x -bench OIndexEnumerate -benchmem

// benchStore builds a store holding `others` unrelated single-member collections plus
// one target collection of `target` members, with the ordered index maintained exactly
// as the server maintains it.
func benchStore(others, target int) (*Store, []byte) {
	s := New(1<<20, 1<<26)
	for i := 0; i < others; i++ {
		coll := fmt.Sprintf("other%08d", i)
		k := collKey(coll, "m")
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			panic(err)
		}
		s.CollInsert(k, kindTestField)
	}
	for i := 0; i < target; i++ {
		k := collKey("target", fmt.Sprintf("f%08d", i))
		if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
			panic(err)
		}
		s.CollInsert(k, kindTestField)
	}
	return s, collPrefix("target")
}

// BenchmarkOIndexEnumerateOrdered measures the implemented path: a bounded cursor over
// the ordered index. Cost tracks the target's size, not the keyspace size.
func BenchmarkOIndexEnumerateOrdered(b *testing.B) {
	for _, sz := range []struct{ others, target int }{
		{1000, 100},
		{100000, 100},
		{100000, 1000},
	} {
		b.Run(fmt.Sprintf("others=%d/target=%d", sz.others, sz.target), func(b *testing.B) {
			s, prefix := benchStore(sz.others, sz.target)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var after []byte
				n := 0
				for {
					keys, last := s.CollScan(prefix, after, 256, make([][]byte, 0, 256))
					n += len(keys)
					if last == nil {
						break
					}
					after = last
				}
				if n != sz.target {
					b.Fatalf("got %d, want %d", n, sz.target)
				}
			}
		})
	}
}

// BenchmarkOIndexSelectAt measures the random-access primitive SPOP/SRANDMEMBER seek
// through: an order-statistic rank-then-select descent into one target collection
// embedded in a large keyspace. Cost is O(log n) in the whole-index size and allocation
// free, which is what keeps a random member O(log n) instead of the O(n) count a plain
// ordered structure would force. A cheap splitmix walk stands in for the server's
// uniform draw so the benchmark touches spread positions, not one hot node.
func BenchmarkOIndexSelectAt(b *testing.B) {
	for _, sz := range []struct{ others, target int }{
		{1000, 100},
		{100000, 100},
		{100000, 1000},
	} {
		b.Run(fmt.Sprintf("others=%d/target=%d", sz.others, sz.target), func(b *testing.B) {
			s, prefix := benchStore(sz.others, sz.target)
			b.ReportAllocs()
			b.ResetTimer()
			var r uint64 = 0x1234567
			for i := 0; i < b.N; i++ {
				r += 0x9e3779b97f4a7c15
				z := r
				z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
				z ^= z >> 31
				idx := int(z % uint64(sz.target))
				if _, ok := s.CollSelectAt(prefix, idx); !ok {
					b.Fatalf("CollSelectAt(%d) absent", idx)
				}
			}
		})
	}
}

// BenchmarkOIndexEnumerateFullScan measures the rejected alternative: scan the whole
// index, keep the prefix matches, sort them. Cost tracks the keyspace size, so it
// degrades as unrelated collections grow even when the target stays small.
func BenchmarkOIndexEnumerateFullScan(b *testing.B) {
	for _, sz := range []struct{ others, target int }{
		{1000, 100},
		{100000, 100},
		{100000, 1000},
	} {
		b.Run(fmt.Sprintf("others=%d/target=%d", sz.others, sz.target), func(b *testing.B) {
			s, prefix := benchStore(sz.others, sz.target)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var hit [][]byte
				s.Each(func(key, _ []byte) bool {
					if bytes.HasPrefix(key, prefix) {
						hit = append(hit, key)
					}
					return true
				})
				sort.Slice(hit, func(a, b int) bool { return bytes.Compare(hit[a], hit[b]) < 0 })
				if len(hit) != sz.target {
					b.Fatalf("got %d, want %d", len(hit), sz.target)
				}
			}
		})
	}
}
