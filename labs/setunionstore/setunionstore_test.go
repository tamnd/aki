package setunionstore

import (
	"strconv"
	"testing"
)

// overlapSources builds the two half-overlapping sources the aki-bench algebra suite uses: set a holds
// m0..m{n-1}, set b holds the band shifted by n/2, so the two share their upper/lower half and the union
// is 1.5n distinct members. The members are the same "member:<i>" strings the bench and the f1srv
// compute benchmark use, so the string lengths and hash spread match the real workload.
func overlapSources(n int) [][]string {
	a := make([]string, n)
	b := make([]string, n)
	shift := n / 2
	for i := range n {
		a[i] = "member:" + strconv.Itoa(i)
		b[i] = "member:" + strconv.Itoa(i+shift)
	}
	return [][]string{a, b}
}

// mapThenInsert is the pre-fix store path: deduplicate the sources through a seen-set, then insert the
// distinct members into the destination, which deduplicates them a second time. dst stands in for the
// real element index (insert-if-new); the len of dst is the stored cardinality.
func mapThenInsert(sources [][]string) int {
	var total int
	for _, s := range sources {
		total += len(s)
	}
	seen := make(map[string]struct{}, total)
	dst := make(map[string]struct{}, total)
	for _, src := range sources {
		for _, m := range src {
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			// The store insert dedups too; here it never rejects because the seen-set already filtered.
			if _, ok := dst[m]; !ok {
				dst[m] = struct{}{}
			}
		}
	}
	return len(dst)
}

// rawThenInsert is the post-fix store path: walk the sources raw and let the destination be the only
// place a duplicate is caught. Same dst, one dedup instead of two.
func rawThenInsert(sources [][]string) int {
	var total int
	for _, s := range sources {
		total += len(s)
	}
	dst := make(map[string]struct{}, total)
	for _, src := range sources {
		for _, m := range src {
			if _, ok := dst[m]; !ok {
				dst[m] = struct{}{}
			}
		}
	}
	return len(dst)
}

func BenchmarkUnionStore(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		sources := overlapSources(n)
		want := n + n/2 // 1.5n distinct members in the union
		b.Run("n="+strconv.Itoa(n)+"/mapThenInsert", func(b *testing.B) {
			for range b.N {
				if got := mapThenInsert(sources); got != want {
					b.Fatalf("union cardinality = %d, want %d", got, want)
				}
			}
		})
		b.Run("n="+strconv.Itoa(n)+"/rawThenInsert", func(b *testing.B) {
			for range b.N {
				if got := rawThenInsert(sources); got != want {
					b.Fatalf("union cardinality = %d, want %d", got, want)
				}
			}
		})
	}
}

// TestBothPathsAgree pins the correctness the fix rests on: dropping the seen-set does not change the
// stored result, because the destination insert is idempotent per member. If this ever fails the raw
// walk is unsafe and the read form's map is load-bearing on the store path after all.
func TestBothPathsAgree(t *testing.T) {
	for _, n := range []int{2, 10, 1000} {
		sources := overlapSources(n)
		if a, b := mapThenInsert(sources), rawThenInsert(sources); a != b {
			t.Fatalf("n=%d: mapThenInsert=%d rawThenInsert=%d, dedup paths disagree", n, a, b)
		}
	}
}
