package structs

import (
	"fmt"
	"sort"
	"testing"
)

// benchCards are the cardinality bands lab 01 swept, from L2-resident to well
// past DRAM, so the ns/op read against the lab table at the same shapes.
var benchCards = []int{1_000, 100_000, 1_000_000, 4_000_000}

func benchTree(n int) (*Tree, []uint64) {
	scores := distinctScores(n, uint64(n)*2654435761)
	tr := newTreeSized(BranchSize, LeafSize, CountWidth)
	for i, s := range scores {
		tr.Insert(s, nil, uint32(i), nilMembers{})
	}
	return tr, scores
}

func BenchmarkInsert(b *testing.B) {
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d", n), func(b *testing.B) {
			base, _ := benchTree(n)
			// Fresh keys disjoint from the built set to insert then delete, so the
			// tree size stays near n across iterations.
			fresh := distinctScores(b.N+1, uint64(n)*7919+1)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base.Insert(fresh[i], nil, uint32(n+i), nilMembers{})
				base.Delete(fresh[i], nil, nilMembers{})
			}
		})
	}
}

func BenchmarkRank(b *testing.B) {
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d", n), func(b *testing.B) {
			tr, scores := benchTree(n)
			b.ResetTimer()
			var acc uint64
			for i := 0; i < b.N; i++ {
				r, _ := tr.Rank(scores[i%n], nil, nilMembers{})
				acc += r
			}
			_ = acc
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d", n), func(b *testing.B) {
			tr, _ := benchTree(n)
			b.ResetTimer()
			var acc uint64
			for i := 0; i < b.N; i++ {
				// Scatter the ranks so the descent is not an in-order leaf crawl.
				r := uint64((i * 2654435761) % n)
				s, _, _ := tr.SelectAt(r)
				acc += s
			}
			_ = acc
		})
	}
}

// BenchmarkScan measures the per-entry cost of a leaf-chain walk from a rank, the
// ZRANGE emit shape: one seek then a contiguous 100-entry run.
func BenchmarkScan(b *testing.B) {
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d", n), func(b *testing.B) {
			tr, _ := benchTree(n)
			const win = 100
			b.ResetTimer()
			var acc uint64
			for i := 0; i < b.N; i++ {
				start := uint64((i * 4096) % (n - win))
				emitted := 0
				tr.WalkFromRank(start, func(s uint64, _ uint32) bool {
					acc += s
					emitted++
					return emitted < win
				})
			}
			b.ReportMetric(float64(win), "entries/op")
			_ = acc
		})
	}
}

// BenchmarkDelete measures point delete: remove a present key then re-insert it,
// so the tree size holds steady across iterations.
func BenchmarkDelete(b *testing.B) {
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d", n), func(b *testing.B) {
			tr, scores := benchTree(n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := scores[i%n]
				tr.Delete(k, nil, nilMembers{})
				tr.Insert(k, nil, uint32(i%n), nilMembers{})
			}
		})
	}
}

// BenchmarkPopMin measures the fused single-descent pop against the naive
// two-descent baseline the lab priced at 109ns at 1M (SelectAt the min, then
// Delete it), the win this slice exists to bank (lab 04, labs/f3/m2). Both arms
// drain the tree and rebuild it outside the timer when it runs low, so the timed
// region is the pop technique alone with no refill mixed in.
func BenchmarkPopMin(b *testing.B) {
	rebuild := func(scores []uint64) *Tree {
		tr := newTreeSized(BranchSize, LeafSize, CountWidth)
		for i, s := range scores {
			tr.Insert(s, nil, uint32(i), nilMembers{})
		}
		return tr
	}
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d/fused", n), func(b *testing.B) {
			_, scores := benchTree(n)
			tr := rebuild(scores)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tr.Len() == 0 {
					b.StopTimer()
					tr = rebuild(scores)
					b.StartTimer()
				}
				tr.PopMin()
			}
		})
		b.Run(fmt.Sprintf("card=%d/twoDescent", n), func(b *testing.B) {
			_, scores := benchTree(n)
			tr := rebuild(scores)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tr.Len() == 0 {
					b.StopTimer()
					tr = rebuild(scores)
					b.StartTimer()
				}
				s, _, _ := tr.SelectAt(0)
				tr.Delete(s, nil, nilMembers{})
			}
		})
	}
}

// BenchmarkPopMinN measures the ZMPOP-style drain per element at the lab's
// saturating batch of 31 (one leaf). The tree is rebuilt when it drains, so the
// steady cost is the fused pop looped over a cache-resident spine.
func BenchmarkPopMinN(b *testing.B) {
	const batch = 31
	for _, n := range benchCards {
		b.Run(fmt.Sprintf("card=%d", n), func(b *testing.B) {
			tr, scores := benchTree(n)
			b.ResetTimer()
			done := 0
			for i := 0; i < b.N; i += batch {
				if done+batch > n {
					b.StopTimer()
					tr = newTreeSized(BranchSize, LeafSize, CountWidth)
					for j, s := range scores {
						tr.Insert(s, nil, uint32(j), nilMembers{})
					}
					done = 0
					b.StartTimer()
				}
				tr.PopMinN(batch, func(uint64, uint32) {})
				done += batch
			}
		})
	}
}

// TestBytesPerEntryReport logs the memory column the milestone gates against the
// 2-3B/entry F14 bar: the structural overhead beyond the 16-byte leaf entry, for
// both a right-edge 0.9-fill bulk load and random single-key insertion (which
// settles near the ln2 ~0.7 steady-state fill a churned zset holds). Not an
// assertion, a report; the bar is checked in TestBulkLoadBytesPerEntry.
func TestBytesPerEntryReport(t *testing.T) {
	if testing.Short() {
		t.Skip("memory report over large cardinalities")
	}
	for _, n := range []int{1_000, 100_000, 1_000_000} {
		scores := distinctScores(n, uint64(n))

		rnd := newTreeSized(BranchSize, LeafSize, CountWidth)
		for i, s := range scores {
			rnd.Insert(s, nil, uint32(i), nilMembers{})
		}
		bpeRand := float64(rnd.Bytes())/float64(n) - 16

		sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })
		ents := make([]Entry, n)
		for i, s := range scores {
			ents[i] = Entry{Score: s, Ref: uint32(i)}
		}
		bulk := BulkLoad(ents)
		bpeBulk := float64(bulk.Bytes())/float64(n) - 16

		t.Logf("card=%-8d height=%d arity=%d leafCap=%d  bpeBulk=%.2f  bpeRand=%.2f",
			n, bulk.height, bulk.Arity(), bulk.LeafCap(), bpeBulk, bpeRand)
	}
}
