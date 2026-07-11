# Lab 07: F13 hot-key draw escalation

Part of issue #543, the M1 set milestone, the F13 follow-up the gate engaged.

The M1 gate read the hot-key SPOP row at 1.50M ops/s, inside the pre-registered 1.06M-2.1M band that makes the F13 escalation load-bearing rather than headroom (results/f3/m1-gate.md, PRED-F3-M1-SPOP).
This lab prices the escalation's single-thread cost before the slice bakes its one constant.

## Question

F13 splits one hot partitioned set's P sub-tables into k draw-groups so the draw can fan out across k owning workers (doc 11 section 5.4).
A draw becomes two levels: a scatter layer picks a group weighted by its share of the total count, then the group draws within its own partitions by the same exactly-uniform weighted prefix-sum the flat band already uses (section 4.3).
The escalation exists to lift the single-owner ceiling by fanning the draw across workers, so the single-thread draw must not regress: its payoff is the aggregate, not a per-op speedup.

Two things are asked of the lab.
First, does the two-level draw hold its single-thread cost against the flat draw at the P8 and P16 shapes the gate's hot-key row runs?
Second, what is the constant that gates engagement, so a set escalates only where the split is worth wiring?

## Method

In-process, no server, no wire, no engine import.
A lab-local partitioned set is P independent native sub-sets selected by member hash, each with its own draw vector and count, the same shape lab 04 used.
The escalation layer groups the P partitions into k contiguous draw-groups, each with a subtotal count, the scatter weight.

Two draws are timed per cell.

- Draw: the locate plus the drawn vector read, non-mutating, the SRANDMEMBER shape.
  Flat is the prefix scan over all P counts; escalated is the scatter scan over k group subtotals then the group-local walk over its span of partitions.
- Pop: the SPOP kernel, the draw plus the draw-vector swap-remove, reinserting the same ordinal so the cardinality holds steady across the measurement.
  The swap-remove is identical work in both arms, so the pop delta is the locate plus measurement noise.

Both locates use the branchless prefix-sum the engine ships (partition.go), where `end` runs the full prefix and `start` accumulates only the entries before the draw position.

Axes: cardinality {2M at P8, 4M at P16}, the hot-key shapes; k in {2, 4, 8} where k divides P.

Run it with:

```
go run ./labs/f3/m1/07_draw_escalation/
go test ./labs/f3/m1/07_draw_escalation/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process.
Absolute ns wander about 10-20% run to run from cache and GC noise, and the pop row wanders more because the random draw-vector read into a multi-million-entry vector is DRAM-bound and dominates the locate it wraps.
These are darwin numbers; the binding read is the GamingPC gate box.

| card | P | k | flatDraw | escDraw | drawTax | flatPop | escPop | popTax |
|---|---|---|---|---|---|---|---|---|
| 2M | 8 | 2 | 87.2 | 47.2 | -40.0 | 61.5 | 36.0 | -25.5 |
| 2M | 8 | 4 | 87.2 | 31.7 | -55.5 | 61.5 | 55.8 | -5.7 |
| 2M | 8 | 8 | 87.2 | 34.0 | -53.2 | 61.5 | 43.5 | -18.0 |
| 4M | 16 | 2 | 100.9 | 87.3 | -13.6 | 94.8 | 107.2 | 12.4 |
| 4M | 16 | 4 | 100.9 | 73.2 | -27.7 | 94.8 | 68.6 | -26.2 |
| 4M | 16 | 8 | 100.9 | 76.9 | -24.1 | 94.8 | 88.0 | -6.9 |

ns/op, single-thread; drawTax and popTax are escalated minus flat, so negative is an escalated win.

The single-thread draw does not regress; it improves.
The two-level draw touches fewer partition-pointers than the flat scan: flat dereferences all P sub-set pointers to read their vector lengths, while escalated reads k group subtotals from one contiguous array and then only its span of P/k pointers.
At P16 that is k plus 4 pointer touches against 16, and each touch is a potential cache miss, which is why the escalated draw runs 13-55ns under the flat draw across the sweep.
The pop row carries the same locate win under more noise, since its DRAM swap-remove is the larger and equal term in both arms.

## Fan-out projection

The single-owner ceiling is one over the escalated draw time; k workers each drawing within their own group with no cross-core read on the hot path lift it toward k times that, minus the epoch-refresh of the cross-group totals (doc 11 section 5.4).

| card | k | 1-owner Mops/s | k-worker projected Mops/s |
|---|---|---|---|
| 2M | 4 | 31.6 | 126.4 |
| 4M | 4 | 13.7 | 54.6 |
| 4M | 8 | 13.0 | 104.1 |

This is the arithmetic the gate row checks against, not a measured aggregate.
The real P16 aggregate needs the GamingPC box with the execution-model fan-out live, because the projection assumes perfect worker independence and a free epoch refresh, and only the box measures the barrier and the cross-core count read the doc bounds at one batch of staleness.
The single-owner Mops/s here already clears the 2.12M gate floor by an order of magnitude in the pure-kernel microbenchmark, which is the expected gap between the wire-bound gate number (1.50M) and the kernel: the gate's miss is the shared wire and dispatch path, and the fan-out is the lever for the aggregate when one owner does saturate.

## Verdicts, frozen

- escalateMinP = 8.
  A set escalates only at P8 or above, the multi-million-member band the hot-key SPOP row runs on.
  Below it a partitioned set is P4, whose flat draw is four L1-resident counts already, and a k-way split there leaves groups too small for the scatter to earn its scan; the floor keeps escalation aimed at the sets that actually saturate one owner.
  The two-level draw is confirmed at the floor (P8) and above (P16): it holds single-thread and, on this microarchitecture, wins by trimming partition-pointer touches.

- The two-level draw preserves exact uniformity.
  The composed scatter and group-local walk read the same prefix sum as the flat draw, so they return the same partition and slot for every draw position; the engine tests carry the whole-domain bijection and the chi-squared check (escalate_test.go), and this lab carries them lab-local (main_test.go).

- The single-thread no-regression bar is met, so escalation is safe to leave engaged once a hot key trips the trigger.
  The one-way policy (F4) means a cooled key keeps its escalated layout, and the draw stays at least as fast as the flat draw would have been, so there is no downside to holding the split.

## What the gate run still owes

The M1 gate run on the GamingPC box, after the execution model wires the worker fan-out, must read:

- The hot-key SPOP aggregate ops/s with k-way fan-out live, against the 2.12M floor and the 4.2M target, and whether it clears where the single owner read 1.50M.
- The chi-squared uniformity check on the escalated aggregate, since the doc permits the cross-group totals to be epoch-stale by one batch and the box is where that staleness is real.
- The draw and pop single-thread numbers on Linux/amd64, to confirm the darwin no-regression carries to the gate microarchitecture.
