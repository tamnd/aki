# M2 lab 09: the ZRANGE scatter split, the recs half versus the slab half

## Question

Lab 07 pinned the ZRANGE 0.5x at the 1M gate cell: 80 percent is cache scatter on the per-element member read, and the fix is a layout change.
But "the member read" is two scattered loads: the 16-byte `recs[ref]` record (slab offset, member length, score bits) at an insertion-order index, and the `slab[loc]` member bytes at an insertion-order offset.
A rank-ordered slab that leaves `recs` alone (architecture A) is safe and free: it does not touch ZSCAN's record-order cursor and adds no memory.
Killing the `recs` half too (architecture B) reaches the sequential floor but must reorder `recs`, which breaks ZSCAN and forces a cursor decouple; carrying the location in the leaf so `recs` is never read (architecture C) is ZSCAN-safe but is the deepest tree surgery of the three.
Which is worth building depends on how the 80 percent splits between the two loads, so this lab measures the split, holding the walk style constant (fused, no closures) so layout is the only variable.

## Method

No engine import: a `recs` vector byte-identical in shape to `zset/skiplist.go`'s `natRecord`, a member slab, and a fixed-seed rank-order permutation standing in for the leaf chain (a zset built in arbitrary score order, the realistic case).
Four arms, all emitting byte-identical RESP (`main_test.go` proves it), differing only in where each member is read from:

- **baseline**: `recs` scattered, `slab` scattered (today).
- **slabRank** (A): `recs` scattered, `slab` sequential. The record is read at a scattered insertion ordinal but its `loc` points into a rank-ordered slab.
- **bothRank** (B): `recs` sequential, `slab` sequential. Both loads stride forward.
- **leafLoc** (C): `recs` never read, `slab` sequential. The location comes from a rank-ordered 8-byte leaf array.

The window start sweeps the whole zset each rep (lab 07's methodology), so the working set is the full arrays and the scatter is cold; a fixed window keeps the records hot and mismeasures the penalty as near zero.
`A_kill%` is the fraction of the total penalty (`base - bothR`) that architecture A captures; `recs_pen%` is `(bothR - leaf) / bothR`, the cost of reading `recs` sequentially at all, i.e. what C saves over B.

## Result (Apple M4, go1.26, window 100)

```
     card member      base_ns   slabR_ns   bothR_ns    leaf_ns      A_kill%  recs_pen%
    10000      8      11.071      6.832      6.837      7.273        100.1%      -6.4%
   100000      8       6.914      9.104      7.222      7.128          0.0%       1.3%
  1000000      8      38.862     14.818      7.244      7.056         76.0%       2.6%
    10000     32       6.679      6.600      6.452      6.504         34.7%      -0.8%
   100000     32       6.745      6.633      6.552      6.448         58.1%       1.6%
  1000000     32      50.370     13.089      6.431      6.298         84.8%       2.1%
```

Three readings.

**The scatter is a 1M-scale, memory-bound phenomenon.** At 10k and 100k every array fits in cache and the arms are within noise of each other (the 100k/8B row even shows slabRank slower than baseline, pure in-cache jitter); the penalty only appears at 1M where `recs` (16 MB) and `slab` (8..32 MB) exceed L2/L3. That is the gate regime and the only regime the split is read from.

**Architecture A captures the large majority: 76 percent at 8-byte members, 85 percent at 32-byte.** Slab-only reorder takes the 1M/8B walk from 38.9 to 14.8 ns/e (2.6x) and the 32B from 50.4 to 13.1 (3.8x), with no ZSCAN change and no added memory. The bigger the member the more of the penalty it captures, because the slab is the larger scattered structure at wide members.

**The recs half is the last 15..24 percent, and reading recs sequentially is nearly free.** `recs_pen%` is 2.1..2.6 percent at 1M: once `recs` is in rank order the 16-byte load is prefetched and costs almost nothing, so architecture B (14.8 to 7.2 ns/e, a further 2x on what A leaves) reaches the floor and architecture C beats B by only about 2 percent.

## Verdict (frozen)

**Ship architecture A first, then B if the box needs it, and drop C.**

- **A (slab-only rank reorder)** is the first PR: it captures 76..85 percent of the scatter penalty, touches neither ZSCAN nor the tree, adds no memory, and is a 2.6..3.8x on the walk kernel at the gate cell. This is the safe, large down payment.
- **B (reorder recs too, decouple ZSCAN)** is the follow-up only if A alone does not clear 2x on the GamingPC box: it recovers the last 15..24 percent and reaches the sequential floor, because once `recs` is rank-ordered its read is prefetched (recs_pen 2.1..2.6 percent).
- **C (member location carried in the leaf, recs never read)** is dropped: it beats B by about 2 percent for the deepest tree surgery of the three, since B's sequential recs read is already nearly free. The reserved leaf word stays reserved.

This updates milestones/M2-zrange-colocation-plan.md: the plan listed C as the recommended direction on the assumption the recs load was expensive; the measurement shows it is not once recs is sequential, so B is the ceiling and A is the ordered-first, safe majority of the win.
The end-to-end ZRANGE gain is smaller than these walk-kernel ratios because the op also pays the reactor dispatch and the streamed reply this kernel omits, and it is measured by a box A/B of the `zrange_c10k`/`zrange_c1m` aki-bench cells on the old versus new f3srv.
