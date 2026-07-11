# Lab 11: the cross-shard set-algebra gather tax

Part of the M1 set milestone: the F17 intent path (spec 2064/f3 doc 03 section 6.7, doc 11 section 6) carrying SINTER, SUNION, SDIFF, and SINTERCARD over operands that span shards, priced against the single-shard point path #620 shipped.

## Question

Lab 10 found the cross-shard SMOVE tax flat in the data: a two-key move touches a bounded amount of member data no matter the set size, so the tax is the barrier's fixed cost and nothing more.
The gather is the opposite by construction. It freezes every operand under the transaction's barrier, then clones each remote shard's operands into plain heap sets before the first key's owner runs the driver.
Cloning copies every member of every remote operand. So the question is not whether the tax is flat, it is how steep the per-remote-member slope is, whether the fixed barrier cost still dominates at the small operand sizes real sets usually are, and where the clone hurts most.

## Method

Live, in one process: `shard.New`, the real Sadd/Sinter/Sintercard handlers, one synchronous connection.
The co-located arm places K operands on one shard and runs the Sinter point handler; the cross arm places each operand on its own shard and runs `set.SinterCross` under `DoTxn`, the exact route dispatch takes.
Every operand is filled with the same members, so the intersection is the whole set and both the driver's probe and the clone's copy run at full width, the worst case and the cleanest read of the clone slope.
Three sweeps:

- Full table: operand count K in {2, 4}, per-operand cardinality in {8, 1024, 16384} (the bands).
- Per-remote-operand hop: per-operand n held at 8 so the clone copy is nearly free, K swept 2 to 8, isolating the fixed cost of each remote barrier hop.
- Clone versus compute: SINTERCARD LIMIT 1, which stops the driver after one member so the compute term collapses while the clone still copies every remote operand in full.

Wall clock over a 300ms floor.

Run it with:

```
go run ./labs/f3/m1/11_gather_cross/
go test ./labs/f3/m1/11_gather_cross/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, machine otherwise quiet.
Cells wander 5-15% run to run; the tax ratios are steadier than the cells except where noted.

Full-overlap operands, us per SINTER:

| K operands | per-operand n | co-located us | cross-shard us | tax |
|---|---|---|---|---|
| 2 | 8 | 3.90 | 20.60 | 5.3x |
| 2 | 1024 | 61.14 | 142.88 | 2.3x |
| 2 | 16384 | 348.39 | 1319.68 | 3.8x |
| 4 | 8 | 4.09 | 20.73 | 5.1x |
| 4 | 1024 | 54.52 | 255.47 | 4.7x |
| 4 | 16384 | 753.29 | 3173.01 | 4.2x |

Per-remote-operand hop, per-operand n 8, us per SINTER:

| K operands | co-located us | cross-shard us | tax |
|---|---|---|---|
| 2 | 3.92 | 11.91 | 3.0x |
| 3 | 3.99 | 14.39 | 3.6x |
| 4 | 4.39 | 18.26 | 4.2x |
| 6 | 4.69 | 20.57 | 4.4x |
| 8 | 5.38 | 28.49 | 5.3x |

SINTERCARD LIMIT 1, K 4, us per call (compute collapsed, clone full):

| per-operand n | co-located us | cross-shard us | tax |
|---|---|---|---|
| 8 | 5.16 | 17.50 | 3.4x |
| 1024 | 4.08 | 194.60 | 47.7x |
| 16384 | 4.29 | 2204.39 | 513.5x |

## Verdicts, frozen

- The cross-shard gather pays two terms: a fixed per-remote-operand barrier hop of roughly 3 to 4 us, and a clone copy proportional to every remote operand's cardinality.
  The hop term is visible in the small-operand sweep: each extra remote operand adds a few us and nothing else (11.9 us at K=2 to 28.5 us at K=8, n=8 throughout).
  The clone term dominates once the operands are large: at n=16384 the cross arm is 1.3 to 3.2 ms because it copies tens of thousands of members before the driver runs.

- The clone is unconditional, so a driver that would early-stop does not save the gather anything.
  SINTERCARD LIMIT 1 makes this stark: the co-located arm stays at ~4 us for any n because it stops after the first member, while the cross arm still clones all four operands in full and blows out to 47x at n=1024 and 513x at n=16384.
  This is the one place the gather is genuinely wasteful rather than merely taxed, and it is worth a watch item: a future slice could push LIMIT into the clone, or probe the remote operand in place from the smallest owner rather than copying it, so an early-stopping intersection never materializes an operand it will not finish reading.
  It does not block M1: cross-shard algebra over large operands is a rare shape (co-located keys are the common case dispatch keeps off this path entirely), and correctness is exact either way (gathercross_test.go pins the cross result byte-identical to the co-located one).

- No floor or threshold falls out of this lab, same as lab 10.
  The divert condition is exact (engage the gather only when the operands really span shards), the co-located path is untouched by construction, and the price the gather pays is the price the design names: freeze everything, copy what you do not own, run the same driver.
  The gate-box read is the only open question the lab defers: the clone slope is a memory-bandwidth term, so the gate box (further from the memory wall than this M4) will move the large-n rows, not the small-n hop rows.
