# M2 lab 07: ZRANGE window walk, closure-chain and cache-scatter decomposition

## Question

The M2/M3 exit gate measured ZRANGE and ZRANGEBYSCORE at **0.50 to 0.72x** of Valkey on a 100-element window over a one-million-element zset, aki slower than the rival, a regression against v1's 1.59 to 2.20x.
Unlike LRANGE (M3 lab 07), this is not an algorithmic defect: the zset native band already streams a rank window with one counted seek then a leaf-chain walk (`zset/skiplist.go` `walkRange` calls `struct/tree.go` `WalkFromRank`, which runs `descendToRank` once and then follows the singly-linked leaves), so it is O(w), not O(w log n).
The loss is therefore a **constant factor**, and this lab prices where that constant lives so the fix is aimed rather than guessed.

The shipped per-element cost has three parts:

1. **Closure hops.** `walkRange` hands the tree a callback; that callback reads the record and calls a second callback (`rangeByRankWindow`'s `emit`) to append the member. Two indirect calls per element across the `struct` to `zset` package boundary that the compiler cannot inline. A fused walk that appends RESP directly in one loop deletes both.
2. **Record scatter.** The leaf stores a record ref; the walk reads `recs[ref]` for the slab offset, member length, and score bits. Refs are assigned in **insertion** order (`newRecord` appends) but the walk visits them in **score** (rank) order, so on a zset built in arbitrary score order the rank-order ref stream is a random permutation. `recs[ref]` is a scattered 16-byte load, a likely cache miss per element once the record vector outgrows the cache.
3. **Member scatter and RESP encode.** `slab[loc:loc+mlen]` is likewise scattered (loc is insertion-ordered), then the RESP bulk header and memcpy. This is the irreducible floor a range read must pay.

The fix for (1) is cheap and mechanical. The fix for (2) is a **layout** change and a slice of its own. The point of the lab is the ratio between them at the gate cell.

## Method

In-process, no server, no wire, no engine import.
A `recs` vector byte-identical in shape to `zset/skiplist.go`'s `natRecord` (`loc uint32`, `mlen uint32`, `bits uint64`), a member slab filled in insertion order, and an `order` array standing in for the leaf-chain rank order: a fixed-seed permutation for the scattered case (a zset built in arbitrary score order, the realistic case) and identity for the co-located case (members that happened to arrive already score-sorted).
Three kernels over a window:

- `clos_scat`: the shipped shape. An outer `walkRefs` marked `//go:noinline` and taking the callback by value, so the per-element call is a genuine indirect call the compiler cannot elide, faithful to (in fact pessimistic against) the real cross-package `tree.WalkFromRank`. Its callback reads `recs[ref]` and calls a second `emit` closure. Scattered layout.
- `fused_scat`: one loop, no per-element closures, appending RESP directly. Same scattered `recs[ref]` and `slab[loc]` reads, so the delta from `clos_scat` is exactly the two closure hops.
- `fused_seq`: the fused loop over the co-located (identity) layout, so `recs` and `slab` are read in order. The delta from `fused_scat` is exactly the cache scatter, the floor a fused walk cannot reach.

`main_test.go` proves the closure and fused kernels emit byte-identical RESP for every window over both layouts, so a fused walk is a pure performance change.

**Methodology note that changed the answer.** The first cut pinned the window at mid-list for every rep. That kept the same 100 records hot in cache across all 200k reps, so `recs[ref]` never missed and the scatter penalty read as zero (2% at 1M) while the whole cost looked like RESP encode. That is wrong: the gate hits windows all over the keyspace, so its scatter is cold. The lab now **sweeps the window start across the entire zset** (`starts` strides by `w` so one pass touches every record; the timed loop cycles through them), making the working set the whole 32 MB record-plus-slab and the scatter real. A fixed-window ZRANGE microbenchmark is misleading, and this is the single most important thing to get right when measuring a range read on a large collection.

## Result (Apple M4, go1.26)

```
     card  window      clos_scat   fused_scat    fused_seq    clos_win%  scat_pen%
    10000      10       15.873/e      9.460/e      7.560/e        40.4%      20.1%
    10000     100        7.360/e      5.956/e      5.815/e        19.1%       2.4%
    10000    1000        7.154/e      5.809/e      5.717/e        18.8%       1.6%
   100000     100        7.468/e      5.941/e      5.840/e        20.4%       1.7%
  1000000      10       38.172/e     30.110/e      6.324/e        21.1%      79.0%
  1000000     100       35.838/e     29.512/e      5.817/e        17.7%      80.3%
  1000000    1000       35.968/e     30.080/e      5.671/e        16.4%      81.1%
```

`clos_win%` is the fraction of the closure-path per-element cost the fused walk removes; `scat_pen%` is the fraction of the fused per-element cost that is the cache scatter (the sequential floor deleted).

Two readings, and they invert with cardinality.

**When the zset is cache-resident (10k, 100k), the closure hops are the whole cost.** The fused walk wins 18 to 20% and the scatter penalty is ~2%: with no misses to hide behind, the two indirect calls per element are what there is to cut.

**At the gate cell (1M, window 100), the cache scatter is 80% of the cost.** `fused_seq` holds at 5.8 ns per element (the same as at 10k) while `fused_scat` is 29.5, so the scattered `recs[ref]` plus `slab[loc]` reads add **24 ns per element**, four times the floor, and appear only at 1M where the 32 MB working set blows past the cache. The fused walk still wins its 18% (35.8 to 29.5), a real but secondary cut the memory stall mostly hides via out-of-order execution.

So the same 0.5x symptom at the gate is 80% a memory-layout problem and 18% a closure problem, and the two fixes stack: a fused walk over a rank-co-located layout would take the gate cell from 35.8 to 5.8 ns per element, ~6x on the walk kernel.

## Verdict (frozen)

The ZRANGE gate regression is a **cache-scatter** problem, not a closure-chain problem. Ranked:

1. **The gate lever is rank-order member co-location** (80% at 1M). The native band stores member bytes and records in insertion order and walks them in rank order, so a far ZRANGE chases a random permutation through `recs[ref]` and `slab[loc]`, one cache miss per element on a large zset. The fix is to lay the walked data out in rank order (member bytes and score bits contiguous along the leaf chain, the shape Redis's skiplist gets for free by holding the member inline in the node). This is a layout slice of its own with its own design and its own gate re-run, the zset twin of M1's deferred per-partition merge fan-out. Do not expect any closure-level change to move the 1M gate cell.
2. **A fused walk is a real but secondary complement** (18% at 1M, 20% on every cache-resident ZRANGE). A tree leaf cursor (`SeekRank` plus `Next`) that lets `zset` drive the walk with a plain loop instead of a callback removes both indirect hops. It is worth shipping, but it belongs folded into the layout slice, because a rank-co-located store is naturally walked by exactly that direct loop, and on its own it does not close the gate.

This lab bakes no constant; it redirects the range-read effort. It falsifies the cheap fix (a fused-walk-only PR would win 18% and still miss 2x) and pins the expensive one (layout), which is the whole reason to price a mechanism before implementing it.
The same conclusion transfers to ZRANGEBYSCORE, ZREVRANGE, and ZRANGEBYLEX, which all reduce to `rangeByRankWindow` over the same scattered store; it does not touch the point ops (ZSCORE, ZRANK) which do one lookup and never walk.
The end-to-end ZRANGE gain of any of these is smaller than the walk-kernel figure because the op also pays the reactor dispatch and reply delivery this kernel does not, and it is measured by a box A/B of the `zrange_c10k` / `zrange_c1m` aki-bench cells on the old versus new f3srv binary.
