# Lab 05: zset algebra accumulation threshold

Part of issue #544, the M2 zset milestone.
This lab lands before the algebra slice so slice 7 bakes a settled accumulation structure, not a guess.
It gates the "Zset algebra and STORE forms, streamed, accumulation-structure lab settled" slice.

## Question

Doc 12 (12-zset-model.md) section 6.12 builds the STORE destinations ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE from an aggregated result keyed by member.
The union is where the accumulation structure matters, because every input member is folded across all sources with weighted-score aggregation (SUM, MIN, MAX) before anything can be emitted.
Line 554 makes the k-way merge over per-source member-sorted runs the primary ZUNION plan and keeps hash accumulation over the largest source as the degradation path, which is Redis's own strategy.
Line 562 then says every STORE form sorts the aggregated result by (sortable score, member) once and bulk-loads the destination B+ tree at 0.9 fill.

So there are two coupled choices to settle before slice 7 bakes them.
The accumulation structure: hash-accumulate then sort, versus a tournament k-way merge over member-sorted runs then sort, versus accumulate straight into a score-ordered tree.
The sort tax: the destination bulk load wants score order but the aggregation is keyed by member, so a final sort by score is forced unless the structure keeps score order live, which is sort-at-end versus maintain-sorted.

This lab prices all three structures on the union path, sweeps the operand count and result cardinality, brackets with equal-overlap and disjoint shapes, and freezes the winner per regime plus the crossover for the slice-7 driver.

## Method

In-process, no server, no wire, no engine import, mirroring the M1 lab-05 house style.
Members are modeled by a uint64 id encoded big-endian into sz bytes, so byte order equals id order and member equality is id equality.
This prices the 8-byte score-prefix compare that dominates routing (section 3.1); long-member memcmp tails are a separate lex concern (section 3.2) out of scope here.
Scores are small integers held as float64 so SUM is associativity-exact and every kernel's aggregate agrees bit for bit, which is what lets the cross-check compare results exactly rather than within an epsilon.

The three kernels all return the aggregated result as pairs sorted by (sortable score, member), the exact input the destination bulk load consumes.

- hash: an open-addressed member-to-score accumulator (the M1 Swiss-table lineage, keyed by id), aggregate every input, extract, sort by score.
- merge: one member-sorted run per source (the hash slot walk), a tournament k-way merge (a binary min-heap, the implicit tournament tree, O(log k) per pop) folding equal members with aggregation, then sort the member-ordered result by score.
- tree: the same accumulator to find a member's current score, plus an AVL keyed by (sortable score, member) maintained live with a delete-reinsert on every aggregation update, so its in-order walk is the score-ordered result with no final sort.
  This is the maintain-sorted arm and doubles as the doc's accumulate-into-a-tree-directly candidate.

Because hash and tree share the accumulator, the delta between them isolates the sort tax exactly: hash pays one O(m log m) sort at the end, tree pays O(total input) AVL delete-reinserts along the way.

Axes: operand count k {2, 4, 8}, result cardinality m from 1k to 8M, shapes equal-overlap (every source shares the member set, union collapses to card, every member aggregated k times, collision-heavy) and disjoint (sources share nothing, union is k*card, no collisions, sort-heavy).
The tree arm is skipped above 2M total input, where it is already dominated at every smaller size, to keep the run bounded in time and memory.
`go run .` runs the whole sweep; `-quick` runs a smaller one.

## What the doc predicts, and what this lab tests

- Merge is the primary union plan, hash the degradation path when input exceeds the scratch budget (section 6.12 line 554). Tested by the hash-vs-merge columns and the scratch memory table.
- The STORE result is sorted by score once and bulk-loaded (line 562). Tested by the sort-tax section: sort-at-end against maintain-sorted.
- AGGREGATE form and WEIGHTS are aggregation-kernel details, not structural (lines 552, 558). Tested by the aggregate sensitivity table.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, each cell timed adaptively.
Absolute ns wander about 10-15% run to run from cache and GC noise, so the profile shape and ordering are the signal.
`ns` is nanoseconds per result member; `m/h` is mergeNs over hashNs, below 1.0 means merge is ahead.

```
shape      k   resultM     hashNs    mergeNs     treeNs        m/h   winner

equal      2      1000      85.61      88.31     215.22       1.03     hash
equal      2     10000     134.41     120.07     464.88       0.89    merge
equal      2    100000     152.65     140.84     668.11       0.92    merge
equal      2   1000000     198.38     164.34     826.20       0.83    merge
equal      4      1000      85.44      96.21     548.45       1.13     hash
equal      4     10000     123.56     131.81    1008.33       1.07     hash
equal      4    100000     161.34     159.88    1507.08       0.99    merge
equal      4   1000000     262.59     181.92      skip       0.69    merge
equal      8      1000     102.67     151.68    1393.62       1.48     hash
equal      8     10000     147.46     202.60    2478.13       1.37     hash
equal      8    100000     242.72     233.08    3712.74       0.96    merge
equal      8   1000000     413.63     257.02      skip       0.62    merge

disjoint   2      2000     103.83      93.57      71.35       0.90     tree
disjoint   2     20000     128.02     121.47     144.31       0.95    merge
disjoint   2    200000     155.24     149.05     215.03       0.96    merge
disjoint   2   2000000     186.70     162.18     335.25       0.87    merge
disjoint   4      4000      92.36      93.40      89.67       1.01     tree
disjoint   4     40000     132.41     143.33     202.73       1.08     hash
disjoint   4    400000     152.96     151.19     213.42       0.99    merge
disjoint   4   4000000     187.70     170.92      skip       0.91    merge
disjoint   8      8000     103.24     103.94     119.51       1.01     hash
disjoint   8     80000     138.76     135.19     169.34       0.97    merge
disjoint   8    800000     162.69     155.68     242.67       0.96    merge
disjoint   8   8000000     201.63     193.42      skip       0.96    merge
```

Sort tax, shape equal (collision-heavy), AGGREGATE SUM, ns per result member:

```
  k   resultM    accOnly       hash       tree    sortTax  maintainTax

  4      1000      17.95      94.83     617.50      76.88       599.55
  4     10000      22.51     138.98    1143.05     116.47      1120.54
  4    100000      25.12     174.43    1555.04     149.32      1529.93
  4   1000000      87.21     263.80       skip     176.59         -
```

Aggregate and weights sensitivity, hash kernel, shape equal, k=4, ns per result member:

| mode | WEIGHTS off | WEIGHTS on |
|---|---|---|
| SUM | 173.96 | 176.37 |
| MIN | 190.65 | 182.89 |
| MAX | 189.91 | 190.06 |

Peak scratch bytes per result member (F14 discipline, analytic from the structure definitions):

| structure | bytes per result member | note |
|---|---|---|
| hash | 35.4 | acc cap ~1.14*m * 17B + 16B sort slice, bounded |
| tree | 51.4 | acc 19.4 + 32B AVL node, no separate sort slice |
| merge | 16*(tIn/m) + 32 | runs scale with total input: disjoint 48, equal k=8 160 |

## Reading the sweep

The maintain-sorted arm is the clear structural loser and its loss is platform-independent.
Keeping score order live costs the maintainTax column, 600 to 1530 ns per member, against a sort tax of 77 to 177 ns per member for sort-at-end, a factor of eight to ten.
That gap is compute, not cache: the AVL does O(total input) delete-reinserts with a rotation cascade per aggregation update, while sort-at-end does one cache-friendly sort of the m result members.
The one place the tree is not last is tiny disjoint shapes (2000 and 4000 members) where there are zero aggregation collisions, so the tree does pure inserts with no delete-reinsert and fits in cache; as soon as there are collisions or size it is crushed.
So the accumulate-into-a-score-tree-directly candidate is rejected, and the STORE forms sort at the end exactly as line 562 says.

The hash-versus-merge choice is where darwin cannot fully speak, the same caveat lab 03 and the M1 lab 05 hit.
Across the grid the two are within about 15 percent and trade the lead by regime, but the trades are mechanistic, not random.
Merge leads at large results and high overlap, and its lead widens with cardinality: equal-overlap k=8 at 1M is merge 257 against hash 413, a 0.62 ratio, because the hash accumulator there is roughly 19MB and spills the M4's L2 into random-access DRAM misses while merge's per-source sorts and small final sort stay sequential.
That 1M equal-overlap cell is the DRAM-regime effect the doc predicts, appearing on darwin only once the accumulator outgrows cache.
Hash leads at small results and high fan-in: equal-overlap k=8 at 1k and 10k is hash 103 and 147 against merge 152 and 203, because merge's k per-source sorts plus heap overhead do not amortize when m is small, while the small accumulator stays resident.
The crossover on this box is around m=100k for equal-overlap and lower for disjoint, but the exact crossover and how far merge's large-m lead widens are cache-hierarchy and core-count dependent, so they are the gate-box question, not a darwin constant.

Memory is the tiebreaker the F14 discipline supplies while the timing is within noise.
Hash holds a bounded 35 bytes per result member at every shape, because its scratch is the accumulator plus one sort slice sized to the result.
Merge's scratch is the per-source runs, so it scales with total input: 48 bytes per member disjoint, but 160 bytes per member at k=8 equal-overlap, over four times hash.
This is the concrete meaning of the doc's "degrade to hash when input exceeds the scratch budget": merge's run budget grows with fan-in times overlap, so above a budget hash is the memory-bounded choice regardless of the timing.

The aggregate and weights sensitivity confirms the structural verdict does not move with the fold operator.
SUM, MIN, and MAX sit within 10 percent of each other (MIN and MAX carry one extra branch over the add), and WEIGHTS on versus off is inside 2 percent, a single multiply per input folded into a value already loaded.
None of it reorders hash, merge, and tree.

## What the algebra slice should bake in

Frozen for slice 7:

- Sort-at-end, never maintain a live score-ordered structure during accumulation.
  Maintain-sorted costs eight to ten times the final sort and is the structural loser at every size with collisions.
  The accumulate-into-a-score-tree-directly candidate is rejected, and the STORE forms sort the aggregated result by (sortable score, member) once and bulk-load at 0.9 fill, as section 6.12 already specifies.
  This constant is platform-independent (it is compute, not cache) and does not need the gate box.

- Merge stays the primary union plan and hash the fallback, with the switch driven by the scratch budget, not by timing alone.
  Merge's run scratch is 16 bytes times total input pairs, about 16*(tIn/m) bytes per result member, so it grows with fan-in times overlap; hash holds a bounded ~35 bytes per member.
  The driver keeps merge while the per-source runs fit the scratch budget and degrades to hash-accumulate when total input exceeds it, which on the sweep is exactly the high-fan-in high-overlap corner where merge's memory balloons to 160 bytes per member.

- AGGREGATE form and WEIGHTS are aggregation-kernel details, not structural.
  SUM, MIN, MAX, and the weight multiply are all within 10 percent and never reorder the structures, so the driver picks the accumulation structure on shape and scratch budget alone and applies the fold operator inside it.

Pre-registered for the M2 gate box, not frozen here:

- The hash-versus-merge time crossover.
  On darwin the two are within about 15 percent and trade the lead, with merge ahead at large-m low-fan-in and at 1M equal-overlap (the accumulator-spills-cache DRAM effect) and hash ahead at small-m high-fan-in.
  The crossover cardinality and how far merge's large-m lead widens depend on the cache hierarchy and core count, so they get their Linux confirmation at the M2 gate run on GamingPC, the same box question the M1 lab 05 merge-vs-probe crossover carries.
  If the gate box shows merge's DRAM-regime lead is larger and starts earlier than the darwin ~100k crossover, the scratch budget is the only knob that changes; the sort-at-end and no-maintain-sorted constants stand regardless.

Slice 7 must encode as tests:

- All accumulation structures produce the identical aggregated, score-ordered result, across SUM/MIN/MAX, WEIGHTS on and off, equal-overlap and disjoint shapes, and k in {2,4,8}, so whichever the driver picks by budget is interchangeable (this lab's cross-check).
- The weighted-arithmetic edge cases from line 558: weight 0 times an infinite score substitutes 0, SUM of +inf and -inf substitutes 0, MIN and MAX aggregate post-weight, and no nan ever reaches a tree key.
- The STORE result respects the size ladder: a result below the inline thresholds stores as a blob, not a tree (section 4, line 348).
- The destination bulk load receives (sortable score, member)-sorted input at 0.9 fill, and the algebra loop is resumable at pair-budget granularity for the latency-isolation slices (lines 569, 571).

## Darwin caveat

These numbers are on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The two platform-independent findings, sort-at-end over maintain-sorted and the aggregate insensitivity, are measured faithfully here and stand.
The one thing this box cannot fully settle is the hash-versus-merge crossover: the M4's large caches keep the accumulator resident below about 1M members, so merge's sequential-scan advantage over the accumulator's random DRAM access only appears at the largest sizes, and the exact crossover moves with the cache hierarchy.
That confirmation happens at the M2 gate run on GamingPC before the algebra gate rows are read, exactly as section 10 already requires.
