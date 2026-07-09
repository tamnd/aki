// Package setmergecollect is the lab behind presizing the set-algebra merge collect buffers, the change
// that gives setMergeCollect (f1srv/set_algebra.go) its result and per-partition slices a capacity hint
// from the smaller source's cardinality instead of growing them from zero. It isolates the growslice
// churn a SINTER-256 CPU profile on the GamingPC gate box charged at ~12% of the reactor loop's time:
// on the hot single-partition intersect path every matched member did out = append(out, m) into a slice
// that started at cap 0, so the buffer reallocated and copied its backing array log2(result) times per
// SINTER, and at tens of thousands of SINTERs a second that copy churn is real CPU the merge never had
// to spend.
//
// # What the collect path does, and where the churn came from
//
// setMergeCollect is the shared body of SINTER/SDIFF/SUNION on the sorted-hash merge: it forces a
// synchronous fold so the per-set sorted member-hash arrays are current, then runs the op's two-pointer
// emitter once for an unpartitioned pair (the P=1 path the 256-member gate hits) or once per partition
// fanned across workers, appending every emitted member into a growing slice. Before this change both
// the P=1 result slice and each partition's local slice were made as make([][]byte, 0): no capacity, so
// the first append allocated a tiny backing array and every subsequent doubling reallocated and copied
// the pointers already collected. The intersection of two n-member sets overlapping in half is n/2
// members, so the P=1 SINTER-256 path grew its slice through roughly log2(128) doublings, each copying
// the pointers so far, all on the reactor loop goroutine that also had to write the ~8 KB reply.
//
// The fix reads plan.lo, the smaller source's cardinality that setMergeEligible already computes, and
// presizes: make([][]byte, 0, lo) for the P=1 result and make([][]byte, 0, lo/p+1) for each partition's
// local buffer. lo is an exact upper bound for intersect and diff (the result cannot exceed the smaller
// source) and a close lower bound for union (which appends past it, so the hint still removes the early
// doublings). One allocation of the right size replaces the doubling chain, and no matched member ever
// pays a copy.
//
// # What this lab models
//
// It reproduces the collect loop in isolation: build two equal-cardinality sets overlapping in half,
// pre-fold their member hashes into sorted arrays (the merge input shape), then two-pointer them while
// appending each match into a result slice built two ways.
//
//   - collectGrow: out := make([][]byte, 0), the pre-change buffer that grows from zero.
//   - collectPresize: out := make([][]byte, 0, lo), the post-change buffer sized from the smaller
//     cardinality.
//
// Both emit the members as []byte subslices of one arena, so the benchmark captures the pointer append
// and slice growth the live path pays, not member materialization (which is identical either way). The
// members are stable arena subslices, so nothing in the loop allocates except the result slice itself,
// which isolates exactly the growslice cost the presize removes.
//
// # What it shows
//
// At the gate's 256-member cardinality collectPresize does one allocation and no copies where
// collectGrow does ~8 allocations and copies ~128 pointers in total, so allocs/op drops from several to
// one and ns/op falls by the copy work, the per-op saving that on the reactor loop returns cycles to
// the reply write that was starving the loop's other connections. The win grows with cardinality: at
// 4096 members the grow path pays ~11 doublings and copies ~2048 pointers, all of which the single
// presized allocation avoids. The parallel benchmark runs the collect under GOMAXPROCS workers, each on
// its own arrays, to show the saving holds under the fanned-partition regime the larger sets take.
package setmergecollect
