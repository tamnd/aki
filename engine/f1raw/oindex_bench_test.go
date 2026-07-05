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

// benchStoreVals builds one target collection of `target` members, each with a
// valBytes-long value, embedded in `others` unrelated single-member collections. It is
// benchStore with a realistic value width so the value-carrying enumeration benchmark
// copies a real payload per field rather than a one-byte token.
func benchStoreVals(others, target, valBytes int) (*Store, []byte) {
	s := New(1<<20, 1<<27)
	val := bytes.Repeat([]byte("x"), valBytes)
	for i := 0; i < others; i++ {
		coll := fmt.Sprintf("other%08d", i)
		k := collKey(coll, "m")
		if _, err := s.PutKind(k, val, kindTestField); err != nil {
			panic(err)
		}
		s.CollInsert(k, kindTestField)
	}
	for i := 0; i < target; i++ {
		k := collKey("target", fmt.Sprintf("f%08d", i))
		if _, err := s.PutKind(k, val, kindTestField); err != nil {
			panic(err)
		}
		s.CollInsert(k, kindTestField)
	}
	return s, collPrefix("target")
}

// BenchmarkOIndexEnumerateValues settles the value-carrying enumeration decision (task
// #190): HGETALL/HVALS walk the ordered index for the field keys, then must produce each
// field's value. The "split" path re-resolves every value with a GetKind (a fresh hash
// plus a bucket probe per field), the way the first cut did; the "fused" path reads the
// value straight from the record offset the ordered walk already yielded (CollScanKV plus
// ReadValueAt), so a field is one lookup instead of two. Both drain the same target
// collection in the same 256-key batches a real HGETALL uses, so the delta is purely the
// per-field second lookup the fused path drops.
func BenchmarkOIndexEnumerateValues(b *testing.B) {
	for _, sz := range []struct{ others, target int }{
		{100000, 100},
		{100000, 1000},
	} {
		s, prefix := benchStoreVals(sz.others, sz.target, 64)

		b.Run(fmt.Sprintf("split/others=%d/target=%d", sz.others, sz.target), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			vbuf := make([]byte, 0, 64)
			for i := 0; i < b.N; i++ {
				var after []byte
				n := 0
				for {
					keys, last := s.CollScan(prefix, after, 256, make([][]byte, 0, 256))
					for _, k := range keys {
						v, _ := s.GetKind(k, vbuf, kindTestField)
						vbuf = v
						n++
					}
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

		b.Run(fmt.Sprintf("fused/others=%d/target=%d", sz.others, sz.target), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			vbuf := make([]byte, 0, 64)
			for i := 0; i < b.N; i++ {
				var after []byte
				n := 0
				for {
					keys, offs, last := s.CollScanKV(prefix, after, 256, make([][]byte, 0, 256), make([]uint64, 0, 256))
					for j := range keys {
						v := s.ReadValueAt(offs[j], vbuf)
						vbuf = v
						n++
					}
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

// BenchmarkOIndexRemoveReinsert measures the compare-heavy skip-list descent directly: a
// remove-by-key followed by re-insert of the same key, both of which walk the express lanes
// comparing the composite key against nodes at every level. This is the path the saturating
// SPOP profile named (removeLocked plus the deferred folder's splice), and it is where the
// inline comparison-key cache replaces a random arena read per compared node with an inline
// read. Population is a single large collection so the descent has real height; a splitmix
// walk spreads the target key across the whole run so the benchmark measures the average
// descent, not one hot cache-resident node. Reported ns/op is one remove plus one insert.
func BenchmarkOIndexRemoveReinsert(b *testing.B) {
	for _, n := range []int{10000, 200000, 2000000} {
		b.Run(fmt.Sprintf("members=%d", n), func(b *testing.B) {
			s := New(1<<20, 1<<27)
			keys := make([][]byte, n)
			for i := 0; i < n; i++ {
				k := collKey("hot", fmt.Sprintf("m%08d", i))
				if _, err := s.PutKind(k, []byte("v"), kindTestField); err != nil {
					b.Fatal(err)
				}
				s.CollInsert(k, kindTestField)
				keys[i] = k
			}
			b.ReportAllocs()
			b.ResetTimer()
			var r uint64 = 0x9e3779b1
			for i := 0; i < b.N; i++ {
				r += 0x9e3779b97f4a7c15
				z := r
				z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
				z ^= z >> 31
				k := keys[int(z%uint64(n))]
				s.CollRemove(k)                // removeLocked descent: compare per node at each level
				s.CollInsert(k, kindTestField) // insert descent: compare per node at each level
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
