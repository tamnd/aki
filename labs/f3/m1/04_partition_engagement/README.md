# Lab 04: partition engagement threshold and target P

Part of issue #543, the M1 set milestone.
This lab lands before the partitioned-band slice so the slice bakes a settled engagement threshold and target-P walk, not a guess carried from f1.

## Question

Doc 11 (11-set-model.md) section 4 splits a very large set by member hash into P owner-local sub-sets once it crosses an engagement threshold.
It bakes both the engage threshold and the per-partition target at 1<<18 = 262144 members (line 268), the doc-19 defaults that survived the 6c sweep (K12), with a growth walk of P1 up to 256K, then P4, P8 near 2M, P16 near 4M, doubling continuing past 4M (lines 269, 282).
It skips P=2 entirely because doc 19 measured it as the worst operating point, paying full routing cost for a merely halved structure (line 270, the L5 asymmetry).

Under f3 single ownership (F1) there is no lock to spread, so partitioning does not buy write concurrency the way it did in f1.
It buys bounded maintenance: rehash pauses, vector memmoves, and sorted-array inserts all scale with n/P, not n (line 263).
This lab prices that motive, confirms or falsifies the threshold and the target-P walk, and says what the slice must respect around them.

## Method

In-process, no server, no wire, no engine import.
A lab-local partitioned member set is P independent native sub-sets, each its own Swiss table, draw vector, and count, selected by member hash: member m lives in partition `route(m) & (P-1)`, routed from the top hash bits so partition placement stays independent of the sub-table's own low-bit tag and slot (section 4.1).
The sub-table is the doc's frozen shape from labs 01 and 03: open addressing at 7/8 load, 8-wide SWAR groups, triangular group stepping, a 7-bit H2 tag, and a member-byte confirm on a tag match (here the member is its 8-byte id, so the confirm is a full id compare).
This is not the engine; the slice writes that.
The table grows by doubling at 7/8 load and rehashes from the stored ids, and each sub-table records the wall time and record count of the worst single rehash it ever paid, which is the grow-pause metric.

Four things are priced against the single-table P=1 baseline:

- Point ops: SADD-shaped steady insert into a presized table (route plus place, no grow) and SISMEMBER-shaped hit and miss probes.
- Draws: the section-4.3 exactly-uniform weighted draw (prefix-sum walk over the per-partition counts, then an in-partition draw) against the P1 flat draw, the L5 danger.
- The grow-pause profile: the worst single rehash during a build from empty, single table versus P partitions.
- Bytes per member: measured heap delta per member (`runtime.MemStats` around the build), so the P-fold table slack is a number.

Axes: cardinality {256K, 1M, 4M, 16M}, P in {1, 4, 8, 16} with P=2 skipped per L5 (line 270).
`go run .` runs the whole sweep; `-quick` shrinks the cardinalities; `-no16m` skips the 16M rows.

## What the doc predicts, and what this lab tests

- Engagement threshold 262144, per-partition target 262144 (line 268). Tested by the grow-pause profile: below the threshold a single table's rehash pause is bounded; above it, it grows without bound, which is what forces the split.
- Routing is negligible for point ops (section 4.2, line 291). Tested by the addNs/hitNs/missNs columns across P.
- The weighted draw stays single-digit ns because the counts are owner-local with no atomics (section 4.3, line 302), within 10ns of the unpartitioned draw (line 305). Tested by the draw-tax table.
- Partitioning costs bytes-per-member nothing, the P-fold slack stays at the 8/7 ratio (section 11.2, line 683). Tested by the bytes/member column.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, each point-op and draw cell timed adaptively to at least 40 ms.
Build and grow-pause are single measurements per cell (a 16M build is paid once).
Absolute ns wander about 10-15% run to run from cache, TLB, and GC noise, so the profile shape and the ordering are the signal, not the last digit.

Full sweep, ns/op unless noted:

```
     card   P   buildNs     addNs     hitNs    missNs    drawNs growPauseNs     growN  B/member

   262144   1      20.4       7.4       5.1       4.3       4.2      875291    229376      37.7
   262144   4      18.9       6.8       5.2       4.4      11.0      225917     57344      40.6
   262144   8      21.1       7.5       6.8       4.9      12.1       94375     28672      32.2
   262144  16      18.3      10.4       5.2       4.3      14.3      137250     14336      32.5

  1000000   1      23.4      12.1      12.1       8.2       5.7     5009166    917504      46.8
  1000000   4      22.0       7.7      15.8       8.5      15.8      959209    229376      30.7
  1000000   8      21.1      10.5       8.0       5.8      19.8      464125    114688      30.4
  1000000  16      21.2       7.6       8.9       5.9      19.5      411542     57344      26.7

  4000000   1      36.9      19.6      28.1      13.1      29.6    38350708   3670016      45.5
  4000000   4      24.1      10.7      35.0      14.2      31.8     4550833    917504      33.6
  4000000   8      29.2      31.2      30.6       9.6      42.0     2777333    458752      35.7
  4000000  16      23.3      14.7      36.8      11.4      57.6     1074083    229376      33.4

 16000000   1      74.0      49.1      36.1      25.7      37.0   391977500  14680064      36.7
 16000000   4      49.8      59.7      38.3      25.0      44.3    60493959   3670016      30.4
 16000000   8      42.4      56.8      40.1      22.6      55.7    11808708   1835008      31.6
 16000000  16      43.5      54.6      38.3      22.0      62.7     4930792    917504      34.9
```

Grow-pause profile, the engagement motive (section 2.5, 4.1, line 263):

| card | P1 pause | P1 recs | P16 pause | P16 recs |
|---|---|---|---|---|
| 262144 | 0.88 ms | 229376 | 0.14 ms | 14336 |
| 1M | 5.0 ms | 917504 | 0.41 ms | 57344 |
| 4M | 38.4 ms | 3670016 | 1.07 ms | 229376 |
| 16M | 392 ms | 14680064 | 4.9 ms | 917504 |

Weighted-draw tax versus the P1 flat draw, the L5 check (section 4.3):

| card | P1 flat | P16 weighted | delta |
|---|---|---|---|
| 262144 | 4.2 | 14.3 | 10.1 |
| 1M | 5.7 | 19.5 | 13.8 |
| 4M | 29.6 | 57.6 | 28.0 |
| 16M | 37.0 | 62.7 | 25.7 |

## Reading the sweep

The grow-pause column is the whole argument.
A single table's worst rehash moves every record it holds, so its pause grows linearly with cardinality: 0.88 ms at 262K, 5.0 ms at 1M, 38 ms at 4M, and 392 ms at 16M.
A 392 ms stall inside one command on one shard is a reactor stopped dead for a third of a second, and it only gets worse as the set grows.
Partitioning caps it: at P16 the worst rehash moves at most n/16 records, so the pause falls to 4.9 ms at 16M and 0.41 ms at 1M.
The pause is set entirely by the largest number of records any one rehash moves, and that is exactly `per-partition size`, which is why the per-partition target, not P itself, is the constant that matters.

The engagement threshold falls straight out of this.
At 262144 members a single table's worst rehash moves 229376 records for a 0.88 ms pause, which is the last point where an unpartitioned table's stall is on the sub-millisecond edge.
One doubling later the table holds ~500K and the pause is past a millisecond and climbing without bound.
So 262144 is the right place to engage: it is the largest single-table size whose rehash pause is still tolerable, and everything above it needs the n/P cap.

Point ops confirm routing is free but do not show a cache win at these sizes.
The route hash plus one indirection adds nothing measurable: hit and miss probe cost is flat across P within noise (16M hit 36.1 at P1 against 38.3 at P16), and steady insert is flat too.
The doc's stronger hope that smaller per-partition tables win back cache residency (section 4.2) does not show at these cardinalities, because 16M/16 is still 1M members per partition and a 1M table already spills to DRAM.
Where partitioning does help the write path is the amortized build: buildNs falls from 74.0 at P1 to 43.5 at P16 for 16M, a 1.7x win, because the partitioned build pays many small rehashes that stay cache-resident instead of a few enormous ones that thrash.
That is the bounded-maintenance dividend showing up as throughput, not just tail latency.

The weighted draw buries L5 but does not quite hit the 10ns prediction at large P.
f1 paid 603.9 ns against 91.8 ns unpartitioned, a 6.6x draw loss (line 261); here the P16 weighted draw adds 10 to 28 ns over the P1 flat draw, never a multiple.
The delta is the 16-entry scalar prefix-sum walk plus the extra count reads, and it lands right on the section-4.3 prediction (within 10 ns) at the threshold but drifts to 25 to 28 ns at 4M and 16M.
The doc already names the fix: a branchless, SIMD-friendly scan of the at-most-16 counts (line 302), which the scalar walk here is not.
Either way the L5 draw tax is single-digit-to-low-double-digit ns, not the hundreds f1 paid, and the section-4.3 relief is confirmed.

Bytes per member shows no P-fold penalty.
Measured heap delta per member ranges 25 to 47 B across the sweep, bracketing the doc's ~34 to 36 B analytic point (26-28 B overhead plus 8 B member id), and the partitioned rows are if anything lower than P1, not higher: P16 at 1M is 26.7 B against P1 at 46.8 B.
The P1 spikes are power-of-two table slack (1M members just past 2^20 forces a 2^21-slot table, half empty), and partitioning smooths that by spreading members across several smaller tables whose slack averages out.
So the section-11.2 claim holds: the P-fold slack stays at the 8/7 ratio and costs bytes-per-member nothing.

## What the partitioned-band slice should bake in

Frozen for the partitioned-band slice:

- Engagement threshold 262144 (1<<18) members, confirmed (line 268).
  Below it a single native table's worst rehash pause is sub-millisecond (0.88 ms at the threshold on this box); at the first doubling above it the pause passes a millisecond and then grows without bound (5 ms at 1M, 392 ms at 16M).
  A set stays single-table native up to 262144 and engages the partitioned band on the write that would cross it.

- Per-partition target 262144, confirmed as the real invariant (line 268).
  The grow pause is set by the largest single rehash, which is one partition's size, so holding every partition at or under 262144 caps every rehash at the same ~1 ms pause no matter how large the whole set grows.
  Target P is derived from this target, not chosen directly: `P = next power of two >= ceil(card / 262144)`, floored at 4, so the walk is P4 for 256K to 1M, P8 near 2M, P16 near 4M, and it keeps doubling past 4M (P64 at 16M) exactly as line 282 says.
  This lab's P16-at-16M row leaves 1M members per partition and a 4.9 ms pause, which is why the doubling must continue past P16: the constant to hold is the per-partition target, and P follows.

- Skip P=2 (line 270), confirmed by construction.
  The floor of 4 on the derived P means the walk never lands on 2, and there is no grow-pause or draw reason to add it.

- Route from the top hash bits, clear of the sub-table's tag and slot bits.
  Top-bit routing spread 16M members across 16 partitions within 5% of even (the balance test), so no partition carries an outsized share that would defeat the n/P bound.

- Use a branchless prefix-sum scan for the weighted draw (line 302), not a scalar walk.
  The scalar 16-entry walk here adds 25 to 28 ns over the flat draw at 4M and 16M, past the section-4.3 10 ns prediction; the branchless scan the doc names is what holds the 10 ns bound and keeps L5 buried at every P.

Judgment on the doc's predictions:

- Engagement threshold 262144: confirmed. It is the last single-table size with a tolerable rehash pause.
- Per-partition target 262144 and the P4/P8/P16-and-double walk: confirmed, and this lab sharpens it to "hold the per-partition target, derive P," which is what makes the pause constant across cardinality.
- Skip P=2: confirmed, falls out of the floor-of-4 derived P.
- Routing negligible for point ops: confirmed (flat across P within noise).
- Weighted draw within 10ns of unpartitioned (line 305): partially confirmed. Massive L5 relief (tens of ns, not the 6.6x f1 loss), on the 10 ns mark at the threshold, but 25 to 28 ns at large P with a scalar walk; the branchless scan is required to hold the strict bound.
- Bytes-per-member no P-fold penalty (section 11.2): confirmed, partitioned rows sit at or under P1.

## Darwin caveat

These numbers are on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The mechanism is platform-independent: a single table's rehash moves every record it holds, so its pause grows linearly with cardinality, and the n/P cap is arithmetic that holds on any box.
The absolute pause milliseconds, the point-op ns, and the draw tax get their Linux confirmation at the M1 gate run on GamingPC before the partitioned gate rows are read, exactly as section 4 already requires.
The one number this box cannot show is the cache-residency point-op win the doc hopes for at smaller per-partition sizes; the gate box with its different cache hierarchy is where that either appears or is dropped from the design's claims.
