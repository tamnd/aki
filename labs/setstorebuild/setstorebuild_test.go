package setstorebuild

import (
	"math/rand"
	"slices"
	"testing"
)

// buildN is the destination cardinality every build benchmark targets: a set well past the
// listpack/intset boundary, in the coll form where the sorted-hash array is maintained, which is the
// regime the STORE cliff showed up in.
const buildN = 1 << 16

// members returns n pseudo-random member hashes, the input a STORE folds into the destination's
// sorted array. Seeded so every benchmark folds the same set and the two builds are compared on
// identical work.
func members(n int) []uint64 {
	r := rand.New(rand.NewSource(1))
	h := make([]uint64, n)
	for i := range h {
		h[i] = r.Uint64()
	}
	return h
}

// foldOne merges a single add into an already-sorted (hash) array, allocating fresh arrays exactly as
// engine/f1raw's sortedHashes.foldBatch does (fresh arrays are what keep a concurrent merge holding
// the old snapshot correct, so the copy is not incidental, it is load-bearing). This is the cost one
// member adds when the destination's sorted order is folded a member at a time, so folding n members
// this way is the sum of 1 + 2 + ... + n copies, O(n^2).
func foldOne(sorted []uint64, add uint64) []uint64 {
	out := make([]uint64, 0, len(sorted)+1)
	i := 0
	for i < len(sorted) && sorted[i] < add {
		out = append(out, sorted[i])
		i++
	}
	out = append(out, add)
	out = append(out, sorted[i:]...)
	return out
}

// incrementalBuild folds the members one at a time, the STORE path's behavior when the async folder
// drains after each inserted member (batch size 1, the worst case the per-member journal produces
// under a folder that keeps pace). It is the O(n^2) build the cliff was.
func incrementalBuild(hashes []uint64) []uint64 {
	var sorted []uint64
	for _, h := range hashes {
		sorted = foldOne(sorted, h)
	}
	return sorted
}

// batchedBuild folds the members in fixed-size batches, the STORE path's behavior when the folder
// coalesces batch members per wake: each fold sorts the batch then merges it into the growing array in
// one fresh-array pass, so the build is sum over the b batches of O(existing + batch), still O(n^2/b)
// dominated by the tail merges. It shows the coalescing softens the constant but not the order.
func batchedBuild(hashes []uint64, batch int) []uint64 {
	var sorted []uint64
	for lo := 0; lo < len(hashes); lo += batch {
		hi := min(lo+batch, len(hashes))
		adds := slices.Clone(hashes[lo:hi])
		slices.Sort(adds)
		out := make([]uint64, 0, len(sorted)+len(adds))
		i, j := 0, 0
		for i < len(sorted) && j < len(adds) {
			if sorted[i] <= adds[j] {
				out = append(out, sorted[i])
				i++
			} else {
				out = append(out, adds[j])
				j++
			}
		}
		out = append(out, sorted[i:]...)
		out = append(out, adds[j:]...)
		sorted = out
	}
	return sorted
}

// bulkBuild sorts the whole member list once, engine/f1raw's sortedHashes.build: one O(n log n) sort
// into fresh arrays, the whole destination folded in a single pass. It is what SortedHashBuild does
// after storeAlgebra has written every member.
func bulkBuild(hashes []uint64) []uint64 {
	out := slices.Clone(hashes)
	slices.Sort(out)
	return out
}

func BenchmarkBuildIncremental(b *testing.B) {
	h := members(buildN)
	b.ResetTimer()
	for range b.N {
		sink = incrementalBuild(h)
	}
}

func BenchmarkBuildBatched64(b *testing.B) {
	h := members(buildN)
	b.ResetTimer()
	for range b.N {
		sink = batchedBuild(h, 64)
	}
}

func BenchmarkBuildBulk(b *testing.B) {
	h := members(buildN)
	b.ResetTimer()
	for range b.N {
		sink = bulkBuild(h)
	}
}

var sink []uint64

// TestBuildsAgree pins the correctness the benchmark leans on: all three builds produce the same
// sorted order, so the bulk build is a drop-in for the incremental fold, not a different result.
func TestBuildsAgree(t *testing.T) {
	h := members(4096)
	want := bulkBuild(h)
	if got := incrementalBuild(h); !slices.Equal(got, want) {
		t.Fatal("incrementalBuild disagrees with bulkBuild")
	}
	if got := batchedBuild(h, 64); !slices.Equal(got, want) {
		t.Fatal("batchedBuild disagrees with bulkBuild")
	}
}
