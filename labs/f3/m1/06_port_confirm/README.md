# Lab 06: port-lab confirmations

Part of issue #543, the M1 set milestone, the last slice.
This is a confirmation, not a design study: the M1 kernels are all landed, so this slice reruns the three f1 port-lab bars against the shipped f3 code and freezes a verdict per bar.

## Question

F22's port list carried three measured f1 set floors into f3 (doc 11 sections 1.5 and 6.1):

- The dense-vector draw kernel: 4.8ns per draw at 100k members, 12.2ns at 1M (K11).
- The sorted-hash merge kernel: 5.78ms for a same-P 1M-by-1M intersection (K16).
- Maintenance economics: a steady-state insert-plus-remove pair at 411ns (K16, P=256).

The slice text asks one thing of each: does f3 match or beat the f1 number on the same shape?
These were measured floors in f1, not guesses, so a miss here is a real port regression and gets reported, not tuned away in this slice.

## Method

No new harness.
The three shapes already live as microbenchmarks in the engine/f3/set package, so this lab runs them rather than duplicating them.

- Draw: `BenchmarkSRandMember100k` and `BenchmarkSRandMember1M` (bench_test.go), the pure P10 vector draw, non-mutating, one vector slot and one record read.
  The 100k cell is the exact shape of the 4.8ns bar; the 1M cell carries the 12.2ns end.
- Merge: `BenchmarkMergeIntersect1M` (algebra_bench_test.go), a single flat 1M-by-1M intersection at overlap 0.5, ns per member streamed over 2M members.
  The 10k and 100k cells are run alongside to place the cache-resident rate.
- Insert: `BenchmarkSChurnMaintain4k` and `BenchmarkSChurnMaintain1M` (algebra_bench_test.go), a steady-state insert-plus-remove pair on a maintained set, cardinality held.
  The 4k cell matches the f1 bar's shape (see the caveat below); the 1M cell is the flat worst case.

Run them with:

```
go test -run x -bench 'BenchmarkSRandMember100k$|BenchmarkSRandMember1M$|BenchmarkMergeIntersect|BenchmarkSChurnMaintain' -benchtime 3s ./engine/f3/set/
```

## The like-with-like caveat

Two of the three f1 numbers are per-partition figures at P=256, and comparing them to an f3 flat single-table run would be dishonest.

- The 411ns insert pair is maintenance on one partition's sorted run.
  At P=256 a million-member set holds ~3906 members per partition, so the run the churn binary-searches is a few thousand entries that stay in cache, not a million entries that spill to DRAM.
  The faithful f3 shape is the 4k cell.
  f3's production partitioner derives P=4 at a million members, not 256, but the maintenance kernel is per-htable and identical either way, so the per-partition run size is the axis that matters, and 4k is the honest match to the f1 bar.
- The 5.78ms merge is a P=256 partition-parallel figure: 256 independent partition-pair merges fanned across workers (doc 11 sections 6.1 and 10.2, and the L12 line at 6.1 that says the single-thread merge clears the idealized probe by only 1.6x, so the 2x algebra gate is contingent on the fan-out, not the kernel alone).
  f3 has neither the per-partition merge wiring nor the fan-out substrate in the tree yet (both deferred by the partitioned-band and algebra-driver slices, #601 and #599).
  So the flat single-thread `BenchmarkMergeIntersect1M` cannot reach 5.78ms by construction, and its real read waits on the gate box with the fan-out on.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, `-benchtime 3s` for the churn cells and `-benchtime 2s` for the rest.
Absolute ns wander about 10-15% run to run from cache and GC noise.
These are darwin numbers.
The binding read is the GamingPC gate box; this lab freezes the darwin verdict and flags anything the box must settle.

| f1 bar | shape | f1 number | f3 darwin | verdict |
|---|---|---|---|---|
| Vector draw, 100k | SRandMember100k | 4.8 ns | 4.95 ns/op | match |
| Vector draw, 1M | SRandMember1M | 12.2 ns | 11.17 ns/op | beat |
| Merge, 1M-by-1M | MergeIntersect1M, flat single-thread | 5.78 ms (P=256 parallel) | 38.9 ms/op | miss on darwin, deferred to the box |
| Insert pair, per-partition | SChurnMaintain4k | 411 ns | 170.3 ns/op | beat |

Context cells, same run:

| cell | f3 darwin |
|---|---|
| SRandMember10k | 4.20 ns/op |
| MergeIntersect10k | 7.35 ns/member |
| MergeIntersect100k | 8.33 ns/member |
| MergeIntersect1M | 19.45 ns/member |
| SChurnMaintain1M (flat run) | 15.4 us/op |

## Verdicts, frozen

- Draw: match at 100k (4.95 against 4.8, inside the run-to-run noise band), beat at 1M (11.17 against 12.2).
  The P10 vector draw ported clean and holds its floor on this microarchitecture.

- Insert: beat.
  The per-partition insert pair at 170ns is 2.4x under the 411ns bar.
  The maintenance kernel (tail append, tombstone, amortized flush) ported without loss at the run size the f1 number was measured at.

- Merge: miss on darwin single-thread flat, real read deferred to the gate box.
  The flat 1M merge is 38.9ms, above the 5.78ms parallel bar and above f1's own 12ms flat single-thread number.
  The kernel itself is healthy where it stays cache-resident: 7.35 ns/member at 10k and 8.33 at 100k, holding the sequential-stream rate the design counts on.
  The 1M cell jumps to 19.45 ns/member because the two sorted arrays are 16MB each and the walk plus the byte-confirm slab reads go DRAM-bound, which is precisely the cost f1's P=256 partitioning cures by keeping each run in cache.
  So the miss is structural, not a tuning gap: the 5.78ms bar needs cache-resident per-partition runs and worker fan-out, and f3 has wired neither yet.

## Flag for the gate box

One number needs the box to separate a machine artifact from a real regression, and it is not tuned here per the slice rule.

f3's flat single-thread merge runs 19.45 ns/member at 1M against f1's ~6 ns/member (12ms over 2M walked).
On darwin that gap is DRAM latency: the byte-confirm reads a matched member's bytes from a random slab offset, and at 1M those offsets miss cache.
The candidate regression to settle on the box, with per-partition runs on, is whether f3's confirm-time slab read costs more than f1's did, or whether the whole gap closes once the runs are cache-resident and the fan-out is live.
The design's answer is partitioning plus fan-out; this slice records the flat-kernel gap so the gate run reads it deliberately rather than discovering it.

## What the gate run still owes

The M1 gate run on the GamingPC box, after the LTM campaign frees it, must read:

- The merge bar with the per-partition merge wiring and worker fan-out on, against the 5.78ms parallel target.
- The draw and insert bars on Linux/amd64 to confirm the darwin match and beat carry to the gate microarchitecture.
- The flat-merge slab-confirm gap flagged above, cache-resident and DRAM-bound side by side.
