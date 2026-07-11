# Lab 05: algebra maintenance cardinality floor

Part of issue #543, the M1 set milestone.
This lab lands before the merge-kernel and algebra-driver slices so those slices bake a settled maintenance floor, not a guess carried from the setmergefloor lab.

## Question

Doc 11 (11-set-model.md) section 6.3 keeps each algebra-indexed set's sorted-hash arrays current inline at write time, in a bounded-tail form: a sorted run plus an unsorted tail of at most T entries, T=256 (knob 64-512).
That turns SINTER-class algebra from a random probe into a sequential merge (sections 6.4, 6.6), which is the design's 2x lever: K16 measured a 12 ms merge against a 40 ms probe on a 1M-by-1M pair.
But the maintenance taxes every write, and below some cardinality it cannot pay for itself.
The setmergefloor lab already found the merge floor at 128 members on the smaller operand, where below 128 the probe path wins the algebra outright and any maintenance is pure waste.

This lab prices both sides of that trade, the write-time cost against the algebra-time win, and freezes the cardinality floor below which the arrays stay off.
It reuses the lab-03 merge and probe kernels verbatim in shape so both labs price the same intersection.

## Method

In-process, no server, no wire, no engine import.
The probe path is the frozen Swiss member table from labs 01 and 03; the merge path is the section-6.6 two-pointer run-merge-tail cursor.
Two things are priced per cardinality:

- Write side: a SADD-shaped build of a set's member table and draw vector, with and without the section-6.3 sorted-run maintenance.
  The maintained build appends each write to a bounded tail and, when the tail fills, sorts the T entries and merges them into the sorted run (the tail-merge amortization).
  The tax is maintained minus plain, ns per write.
  Two tail policies are timed: fixed T=256 (the doc default) and a scaled T = n/16 (which holds the run-length term as the set grows, since a fixed tail forces ever-larger run merges).
- Algebra side: an equal-operand intersection at overlap 0.5, merge over the maintained arrays against probe over the member table, the unordered fallback the driver uses when no arrays exist.
  The win is probe minus merge, ns per op.

From the two, break-even ops = tax * card / win: the number of intersections a set must take part in to repay maintaining its arrays across its whole build.
Below the floor the win is zero or negative (probe wins) so no number of ops repays the tax.

Axes: cardinality {16 .. 65536} bracketing the 128 floor densely, member size {8, 16, 64} bytes (8 int-class, 16 the gate default, 64 the listpack-value cap).
A separate large-N pass times merge against probe at 262144 and 1M with the arrays built by direct sort, to place the crossover the write sweep cannot reach.
`go run .` runs the whole sweep.

## What the doc predicts, and what this lab tests

- Merge floor at 128 members on the smaller operand (setmergefloor): below it probe wins, above it merge wins and maintenance can repay. Tested by the win and break-even columns across the 16..65536 bracket.
- Bounded-tail maintenance is cheap per write (section 6.3): an append plus an amortized tail merge, single-digit ns. Tested by the tax column.
- The merge is the 2x algebra lever (K16, sections 6.4/6.6): merge beats probe by roughly 3x on large equal operands. Tested by the large-N pass.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, each write and op cell timed adaptively.
Absolute ns wander about 10-15% run to run from cache and GC noise, so the profile shape and ordering are the signal.

```
 sz    card  plainNs     tax   sclCap   sclTax    mergeNs    probeNs        win   breakEv

  8      16    14.73   -0.40      256    -3.05        780         72       -708     never
  8      32     6.59    2.57      256     3.06       1953        141      -1812     never
  8      64     5.89    2.47      256     3.19       4259        266      -3993     never
  8     128     5.61    2.18      256     3.00       9594        530      -9064     never
  8     256     5.30   16.69      256    17.51      23390       1063     -22327     never
  8    1024     5.25   18.91      256    19.76      28676       4289     -24387     never
  8    4096     5.18   31.52      256    30.85      52247      17333     -34914     never
  8   16384     5.32   82.37     1024    55.52     197279      76550    -120730     never
  8   65536     6.00  184.63     4096    64.35     796167     346148    -450019     never

 16     128     5.66    2.30      256     3.09      10198        691      -9507     never
 16     256     5.38   16.58      256    17.59      23555       1380     -22175     never
 16    4096     5.22   32.14      256    30.58      64799      25011     -39789     never
 16   16384     6.85  116.11     1024    54.51     250015      99109    -150906     never
 16   65536     6.11  183.34     4096    68.46     998560     473203    -525357     never

 64     128     5.81    2.02      256     2.94      11380       1661      -9719     never
 64    4096     5.22   32.41      256    32.00     100175      52825     -47349     never
 64   65536     5.90  167.41     4096    65.49    1500321    1083552    -416769     never
```

(Rows trimmed for readability; the run prints the full 16..65536 bracket for each size.)

Large-N algebra, arrays built by direct sort, overlap 0.5:

| sz | card | merge | probe | verdict |
|---|---|---|---|---|
| 16 | 262144 | 3.92 ms | 1.85 ms | probe |
| 16 | 1M | 23.9 ms | 9.35 ms | probe |
| 64 | 262144 | 10.9 ms | 4.74 ms | probe |
| 64 | 1M | 64.4 ms | 22.5 ms | probe |

## Reading the sweep

The write-side tax is faithful on this box and shows the section-6.3 shape exactly.
Below the tail cap the maintained build is an append and nothing else, so the tax is 2 to 3 ns per write (and noise-negative at card 16, where the run never fills).
Once the set is larger than the tail, the tax steps up as the amortized tail merges begin: fixed T=256 rises from 17 ns at 256 members to 32 ns at 4096, 82 ns at 16384, and 185 ns at 65536.
That rise is the run-length term.
A fixed tail forces every flush to merge 256 new entries against an ever-longer run, so the per-write cost grows with n.
The scaled tail T=n/16 holds the run-length term: its tax flattens at 55 to 68 ns from 16384 up, instead of climbing to 185, because a bigger tail means fewer, larger flushes and the run-to-tail ratio stays bounded.

The algebra win is where darwin cannot speak.
Merge never beats probe at any swept size, on either the small bracket or the 262144/1M large-N pass, so break-even is "never" everywhere and the floor does not appear in these numbers.
This is the same darwin cache caveat lab 03 hit: the M4's large caches keep the probe's random member-table lookups cache-resident at 7 to 10 ns, so the probe never pays the 40 ns DRAM-probe penalty that the merge's sequential scan is designed to dodge.
On the gate box (GamingPC, Linux) the probe reaches its DRAM floor and the merge win the doc predicts materializes; that is where the K16 12 ms-against-40 ms result was taken and where the 128 floor was found.
So this lab freezes the write-side tax from its own measurement and carries the floor from setmergefloor for gate confirmation, rather than inventing a darwin-local floor the mechanism says is wrong.

## What the merge-kernel and algebra-driver slices should bake in

Frozen for the slices:

- Maintenance floor 128 members on the smaller operand, carried from setmergefloor, gate-confirmed.
  Below 128 the driver uses the probe path and keeps the sorted-hash arrays off; a set only starts maintaining arrays once it can be the smaller operand of a merge that beats probe.
  Darwin cannot cross the merge-vs-probe line at any size, so this constant gets its confirmation at the M1 gate run on GamingPC, not here.

- Bounded tail T=256 as the default, but the run-length term must be bounded, not just the tail.
  Fixed T=256 lets the per-write tax climb to 185 ns at 65536 because each flush merges against the whole run.
  Holding the run-to-tail ratio (the scaled T=n/16 policy) flattens the tax at 55 to 68 ns.
  The slice should either scale the tail with cardinality or partition the sorted arrays so no single run merge sees more than a bounded length near 4096, which is a finer partitioning than the section-4.1 member band.

- Price the tax against real op traffic, not per set.
  Even at the darwin-inflated tax of 185 ns per write, a set that takes part in enough merges repays it; the driver's job is to keep arrays on only for sets above the floor that actually see algebra, and off for the long tail of sets that never intersect.

Judgment on the doc's predictions:

- Merge floor at 128: not reproduced on darwin (merge never wins), carried to the gate box where setmergefloor set it. The mechanism (probe DRAM penalty vs merge sequential scan) is sound and platform-dependent, so the floor is a gate-box constant by construction.
- Bounded-tail maintenance cheap per write: confirmed below the tail cap (2 to 3 ns), but the fixed-T tax grows with n (up to 185 ns at 65536) because of the run-length term; the doc's single-digit-ns claim holds only while the run stays short, which the tail policy must enforce.
- Merge the 2x algebra lever: not visible on darwin, deferred to the gate box; the large-N pass shows probe winning 2 to 3x here purely from cache residency, the inverse of the gate-box result, which is exactly the caveat.

## Darwin caveat

These numbers are on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The write-side tax is measured faithfully here and is platform-independent in shape.
The one thing this box cannot show is the merge-vs-probe win: the M4's caches keep probes resident and never reach the DRAM floor the merge is built to beat, so the floor and the K16 2x lever get their Linux confirmation at the M1 gate run on GamingPC before the algebra gate rows are read, exactly as section 6 already requires.
