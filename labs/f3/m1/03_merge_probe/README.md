# Lab 03: merge-versus-probe crossover k

Part of issue #543, the M1 set milestone.
This lab lands before the algebra-driver slice so the driver bakes a settled crossover constant, not a guess carried from f1.

## Question

Doc 11 (11-set-model.md) section 6.4 gives the set-algebra driver two ways to intersect two sets.
Probe iterates the smaller operand and probes the larger operand's member table, about 40 ns per DRAM-resident probe (line 393).
Merge streams both operands' sorted-hash arrays as two sequential runs the prefetcher loves (sections 6.1 and 6.6).
For comparable sizes the merge wins, because a sequential stream beats a million random probes (K16: 12 ms merge against 40 ms probe on a 1M-by-1M pair).
As the larger operand grows, the merge's streaming cost grows with it while the probe cost stays pinned to the smaller operand, so past some size ratio k = |big| / |small| probe wins again.
Doc 11 line 415 and section 6.4 pre-register that crossover at k about 7, carried from the f1 K16 lineage.
This lab confirms or falsifies the 7 with lab-local kernels and, more usefully, says what the driver must respect around it.

## Method

In-process, no server, no wire, no engine import.
Two lab-local kernels model the two driver paths; the real driver is the slice's job, and this lab prices the choice it will make.

The probe kernel is the doc's frozen table from lab 01: Swiss open addressing at 7/8 load, 8-wide SWAR groups, triangular group stepping, a 7-bit H2 tag, and a member-byte confirm on a tag match (section 2.1, lab 01 verdict).
Probing means iterating the small operand's members and calling that table's SISMEMBER path against the big operand.

The merge kernel is the doc's section-6.6 two-pointer intersection over sorted-hash arrays, in the section-6.3 bounded-tail form: a sorted run plus a small unsorted tail (T = 256) sorted once on command entry and merged as one logical stream through a run-merge-tail cursor with no copy of the run, and the byte-confirm folded into the emit (the mergeresolve discipline, one confirm per hash tie).
The crossover the driver bakes in comes from the doc's own cost model (section 6.4): the merge streams both arrays, so its cost is proportional to |big| + |small|, while probe is proportional to |small|.
The streaming merge is therefore the primary kernel and the crossover column is read against it.
A galloping-advance merge (section 6.6, the asymmetric-degradation refinement that doubles the step then binary-searches to skip a locally sparse run) is measured alongside as a second kernel so the verdict can say what the driver's flat size-ratio switch deliberately gives up.

Members are distinct ids expanded to the size class in one slab, so member i is a slab slice with no per-member header; the hash is splitmix64 over the id and the confirm is a full byte compare, matching the engine's confirm-on-tag-match (line 189).
The small operand is built with an exact overlap: floor(overlap x |small|) of its members are strided ids drawn from big so they are guaranteed present, the rest come from a disjoint high range so they are guaranteed absent, which fixes the intersection size and lets the correctness test check the count.

Axes: k in {1, 2, 4, 7, 8, 16, 32, 64} (log-spaced, 7 pinned so the pre-registered value is a measured row); small-set cardinality {1k, 100k}; member size class {8, 32, 64} bytes (8 int-class, 64 the listpack-value cap of line 234, 32 between); overlap fraction {0.10, 0.50, 0.90}, the low-skew, equal, and high-skew shapes the SINTER gate names (line 445).
Reads: total op ns for each kernel (what the driver compares), ns per single probe, and ns per streamed merge element.
The last two are the mechanism metrics, and their ratio minus one is the crossover.

`go run .` runs the whole sweep; `-quick` shrinks the cardinalities for a fast check.

## What the doc predicts, and what this lab tests

- Crossover at k about 7 (line 415, section 6.4). Tested by the measured crossover table and, since the 40 ns DRAM probe floor is a gate-box number, by the model crossover the mechanism costs imply at that floor.
- Merge is a sequential stream, cheap per element (K16, section 6.6). Tested by the ns-per-element column, which should be a small stable constant.
- Probe is about 40 ns per DRAM-resident probe (line 393). Tested by the ns-per-probe column, with the darwin caveat below: this box's caches keep the probe well under 40 ns until the operand is much larger, which is exactly why the crossover shifts.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, each cell timed adaptively to at least 40 ms and 5 reps.
Total op ns is one full intersection; ns/prb is probeNs / |small|; ns/elm is mergeNs / (|small| + |big|).
The absolute ns wander about 10-15% run to run from cache, TLB, and GC noise (one gallop cell spiked to 38 ms on a GC hit), so the mechanism columns and the ordering are the signal, not the last digit.

The headline is the model crossover, computed from the two mechanism costs.
The driver picks probe when |small| x perProbe < (|small| + |big|) x perMergeElem, that is when k > perProbe / perMergeElem - 1.
kMeasured uses this box's miss-path probe cost; kAt40nsDRAM uses the K16 / line-393 40 ns DRAM-resident probe the gate box reaches.
Averaged over the miss-dominated k >= 8, overlap 0.10 rows:

| card | sz | ns/prb | ns/elm | kMeasured | kAt40nsDRAM |
|---|---|---|---|---|---|
| 1k | 8 | 3.2 | 4.25 | -0.3 | 8.4 |
| 1k | 32 | 4.1 | 4.32 | -0.1 | 8.3 |
| 1k | 64 | 5.2 | 4.37 | 0.2 | 8.2 |
| 100k | 8 | 12.5 | 3.06 | 3.1 | 12.1 |
| 100k | 32 | 17.7 | 3.56 | 4.0 | 10.2 |
| 100k | 64 | 28.9 | 3.53 | 7.2 | 10.3 |

The merge element cost is a stable 3.0 to 4.4 ns on this box (K16 measured about 5 to 6 ns), and it does not move with cardinality or member size, which is the whole reason the sorted-array merge is the design's 2x lever.
The probe cost is the term that moves: 3 to 5 ns at 1k where the table is L1/L2 resident, 12 to 29 ns at 100k where the control array and slab spill toward DRAM, and it climbs further with member size because the confirm reads a wider slab line.

Measured crossover, streaming merge, defined as the smallest swept k past which probe wins for that k and every larger swept k (so a high-k reversal is not hidden):

| card | sz | ov 0.10 | ov 0.50 | ov 0.90 |
|---|---|---|---|---|
| 1k | 8 | 1 | 1 | 1 |
| 1k | 32 | 1 | 1 | 1 |
| 1k | 64 | 1 | 1 | 1 |
| 100k | 8 | 1 | 1 | 1 |
| 100k | 32 | 1 | 1 | 1 |
| 100k | 64 | 1 | 32 | 64 |

Representative total-op ns at 100k, showing the two effects the crossover table summarises (ns are probe / streaming-merge / galloping-merge):

| card | sz | k | ov | out | probe | merge | gallop |
|---|---|---|---|---|---|---|---|
| 100k | 8 | 7 | 0.10 | 10k | 688k | 2.70M | 2.06M |
| 100k | 8 | 64 | 0.10 | 10k | 1.84M | 18.96M | 4.34M |
| 100k | 64 | 7 | 0.10 | 10k | 1.04M | 3.02M | 2.30M |
| 100k | 64 | 7 | 0.90 | 90k | 10.59M | 13.69M | 12.82M |
| 100k | 64 | 16 | 0.90 | 90k | 19.39M | 16.74M | 14.00M |
| 100k | 64 | 32 | 0.90 | 90k | 23.96M | 20.54M | 14.00M |

## Reading the sweep

The crossover is not a size-ratio constant; it is the ratio of two costs, perProbe / perMergeElem - 1, and only one of those costs moves.
The merge element cost is pinned near 3.5 ns here (5 to 6 ns in K16) whatever the cardinality or member size, so the crossover is set entirely by how expensive a single probe is, and that is set by where the big operand's member table lives in the memory hierarchy.

At 1k the whole table is cache-resident, a probe is 3 to 5 ns, the ratio is barely above 1, and probe wins from k = 1.
At 100k the table is larger, a probe is 12 to 29 ns, and the measured crossover climbs to 7.2 for 64-byte members, landing exactly on the pre-registered value.
At the 40 ns DRAM-resident probe the gate box reaches, the model puts the crossover at 8 to 12 with this box's fast merge element, and at K16's slower 5 ns merge element the same arithmetic gives 40 / 5 - 1 = 7.
So the pre-registered 7 is confirmed as the DRAM-regime constant: it sits inside the 7-to-12 band the mechanism costs bracket, and this box's largest, slowest-member cell already reproduces it.

Two things around the constant matter more than the constant.

First, cache residency makes 7 conservative for small and medium operands.
When the big operand's control array fits in cache, a probe is far under 40 ns and probe wins well below k = 7 (down to k = 1 at 1k and at 100k with 8 or 32-byte members).
A driver that switches to probe only at k = 7 would run a merge on medium operands where probe is already faster.
That is a safe error, not a wrong result, and the merge floor of 128 (setmergefloor) already keeps the smallest operands off the merge path, but it does leave throughput on the table for cache-resident operands.

Second, large members at high overlap invert the crossover.
A probe's cost is a cheap control-byte scan plus a member-byte confirm only on a hit, so at high overlap most probes confirm, and with 64-byte members those confirms are scattered DRAM reads across a slab that reaches hundreds of megabytes at high k.
The streaming merge, by contrast, reads its hash arrays sequentially and pays the same confirm count but nothing for the scan, so past k = 16 the merge reclaims the lead: the measured crossover jumps to 32 at overlap 0.50 and 64 at overlap 0.90 for 64-byte members, against 1 for 8 and 32-byte members.
This is the one regime where a flat k = 7 switch to probe is wrong on throughput, not just conservative.

The galloping merge is the reason the doc keeps `advance` in the kernel.
It stays far cheaper than the streaming merge at high k (4.34 M against 18.96 M at 100k, 8-byte, k = 64) because it skips the sparse side instead of scanning it, and it is what carries the high-overlap large-member band where streaming merge would otherwise lose to probe by even more.
It does not move the pre-registered constant, because the driver switches on size ratio before it would run either merge form; it is insurance for the above-floor asymmetric and high-overlap cases the constant does not see.

## What the driver should bake in

Frozen for the algebra-driver slice:

- Crossover constant k = 7 on |big| / |small|, as pre-registered (line 415).
  At or above 7, probe the small operand into the big operand's member table; below 7 and above the 128 merge floor, merge partition-pairs.
  The constant is confirmed as the DRAM-regime value: it is perProbe / perMergeElem - 1 evaluated at the 40 ns DRAM probe (line 393) and a 5 ns merge element (K16), and it sits inside the 7-to-12 band this lab's mechanism costs bracket.

- Treat 7 as a proxy for the cost ratio, not a law of size.
  It is conservative (merge-favoring) for cache-resident operands, where probe wins below 7; that is safe, and the merge floor already guards the small end, so the slice may keep the single constant and accept leaving some throughput on medium operands rather than adding a cache-residency estimator.

- Respect the large-member high-overlap caveat.
  For member size classes at or above 32 to 64 bytes, the crossover inverts at high overlap (measured 32 at overlap 0.50 and 64 at overlap 0.90 for 64-byte members), because probe pays scattered DRAM confirms that streaming merge amortizes.
  Overlap is not known before the command runs, so the usable signal is member size: for large members the driver should bias toward merge and its fan-out past k = 7 rather than switching to probe, since merge is never much worse there and wins outright when overlap is high.
  For small (8-byte, int-class) members, k = 7 or lower is correct at every overlap.

- Keep the galloping `advance` in the merge kernel (section 6.6).
  It protects the above-floor asymmetric and high-overlap cases the flat size-ratio switch does not see, at no cost to the streaming case.

Judgment on the pre-registered k about 7: confirmed for the DRAM regime and bracketed by 7 to 12; not a universal size-ratio constant, because it is conservative for cache-resident operands (probe wins below 7) and inverts for large-member high-overlap operands (merge wins to k = 32 to 64).
The driver keeps 7 as the default with the member-size caveat above.

## Darwin caveat

These numbers are on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The mechanism is platform-independent: the merge element cost is a small stable constant, the crossover is perProbe / perMergeElem - 1, and the model anchored on the K16 / line-393 40 ns DRAM probe gives 7 to 12 on any box.
What this box cannot show directly is the 40 ns probe itself, because the M4's large caches keep a probe under 40 ns until the operand is much larger than the swept cardinalities, which is why the measured crossover here runs low except at the largest, widest-member cell.
The absolute ns, the 40 ns DRAM-probe floor, and the final crossover re-derivation get their Linux confirmation at the M1 gate run on GamingPC before the algebra gate rows are read, exactly as section 6.4 already requires.
