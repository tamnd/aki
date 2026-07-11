# Lab 08: partition-parallel merge fan-out

Part of issue #543, the M1 set milestone, the section-6.5 lever the gate made load-bearing.

L12 is blunt: the single-threaded sorted-hash merge beats the probe baseline by only ~1.6x, and 1.6x is not the 2x the symmetric-algebra gate asks (doc 11 section 6.5).
The answer is partition-parallel merge: same-P operands were split by the same member hash bits, so partition i of A can only match partition i of B, and the P partition-pair merges are independent read-only tasks under the command's barrier.
This lab prices that fan-out before the gate box reads it.

## Question

Two things are asked.

First, does cutting the merge into P partition pairs cost anything when they still run sequentially on one owner?
K16 measured the P=256 partitioned merge at 5.78ms against the flat 5.80ms at 1M, so the pre-registered answer is no, and it has to stay no because the engine now routes every eligible partitioned pair through the per-partition path whether or not workers exist to donate to.

Second, where is the work threshold below which the fan-out barrier costs more than it saves?
Doc 11 section 6.5 pre-registers it at 64k merge elements, the point where one worker's share exceeds typical barrier latency by 10x.

## Method

In-process, no server, no wire, no engine import.
A lab-local operand is P sorted runs of (hash, ordinal) entries split by the top log2(P) hash bits, members resolved from a flat slab of fixed 16-byte members, the section-6.1 arrays in their pure-run form (lab 05 priced the bounded tail; it is a small constant term either way).
The kernel is the section-6.6 two-pointer intersection: galloping advance past misses, equal-hash spans cross-confirmed by member bytes so a bare collision never counts.
Both operands are built same-P; the cross-P group slicing is engine-tested correctness (engine/f3/set/fanout_test.go), not a timing question.

Three executors run the same P pair merges and are checked to return identical counts.

- seq: one loop on the calling goroutine, the coordinating-owner form the engine ships today.
- spawn: k goroutines started per command, groups claimed off an atomic counter, joined on a WaitGroup.
  This overstates the donation cost, since the engine donates to already-running idle workers, so its crossover is the conservative bound.
- pool: k persistent workers parked on channels, kicked per command with static group striding, joined on a channel.
  This understates dispatch, so the true engine barrier sits between the two executors.

Axes: per-side cardinality 1M at overlap 0.5 with P in {4, 16, 64, 256} and k in {2, 4, 8} for the flatness and scaling read; per-side cardinality 2k to 512k at P=16 and k=4 for the crossover read.

Run it with:

```
go run ./labs/f3/m1/08_merge_fanout/
go test ./labs/f3/m1/08_merge_fanout/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process.
Wander is about 10-25% run to run, worst on the spawn arm at small P where a single straggler goroutine is the whole tail; single cells (not trends) occasionally spike with GC.
These are darwin numbers; the binding read is the GamingPC gate box.

Scale sweep, 1M-by-1M members, overlap 0.5, ms per command:

| P | seq ms | seq ns/elem | spawn k=2 | k=4 | k=8 | pool k=2 | k=4 | k=8 |
|---|---|---|---|---|---|---|---|---|
| 4 | 3.24 | 1.62 | 1.86 | 1.26 | 1.40 | 1.81 | 1.28 | 1.47 |
| 16 | 4.25 | 2.12 | 2.74 | 1.49 | 0.71 | 1.71 | 0.91 | 0.81 |
| 64 | 3.27 | 1.63 | 1.64 | 0.87 | 0.72 | 1.69 | 0.93 | 0.87 |
| 256 | 3.47 | 1.74 | 1.71 | 0.89 | 0.76 | 2.37 | 1.82 | 0.89 |

The sequential row is flat in P: 3.2-4.3ms whether the merge is one flat pass or 256 pairs, the K16 no-cost claim confirmed lab-locally, so routing partitioned pairs through the per-partition path costs the ownerless case nothing.
ns/elem counts both sides' entries (2n per pair), so 1.6-2.1 ns/elem here sits against the 18.22 flat Linux ns/member baseline as a microarchitecture gap (M4 L2 against the gate box's DRAM), not a code gap; the box re-reads this.
Fan-out at 1M lands 1.8-2.4x at k=2, 3.6-3.9x at k=4 (best cells), and 4.4-4.7x at k=8, where the P4 row shows the granularity wall: 4 groups cannot feed 8 workers, and one group per worker leaves the tail hostage to the slowest.

Crossover sweep, P=16, k=4, overlap 0.5, us per command:

| per-side n | merge elems | seq us | spawn us | pool us | spawn/seq | pool/seq |
|---|---|---|---|---|---|---|
| 2048 | 4096 | 4.8 | 4.3 | 5.1 | 0.89 | 1.06 |
| 4096 | 8192 | 9.2 | 6.6 | 9.4 | 0.72 | 1.02 |
| 8192 | 16384 | 18.9 | 11.5 | 17.3 | 0.61 | 0.91 |
| 16384 | 32768 | 37.5 | 16.5 | 22.4 | 0.44 | 0.60 |
| 32768 | 65536 | 76.5 | 30.2 | 42.2 | 0.40 | 0.55 |
| 65536 | 131072 | 159.4 | 56.2 | 74.8 | 0.35 | 0.47 |
| 131072 | 262144 | 384.0 | 120.5 | 132.0 | 0.31 | 0.34 |
| 262144 | 524288 | 836.3 | 224.7 | 232.4 | 0.27 | 0.28 |
| 524288 | 1048576 | 1860.7 | 552.7 | 584.2 | 0.30 | 0.31 |

The pool executor (the closer model of donated tasks) breaks even near 8k merge elements and wins clearly from 32k; the spawn executor, paying goroutine start per command, wins even earlier because a 4-way spawn is cheap on this core.
At the pre-registered 64k elements both executors are already at half the sequential cost or better.

## Verdicts, frozen

- The per-partition merge is free sequentially, so the engine keeps it unconditional for eligible pairs.
  The group loop is the fan-out structure; running it on one owner costs the same milliseconds the flat merge would, at every P swept, which is what makes wiring the path ahead of the worker donation safe.

- fanoutFloor = 65536 merge elements, the doc's pre-registered threshold, confirmed conservative.
  On this box the fan-out pays from about 32k elements even under the pessimistic spawn model, so 64k leaves margin for the gate box's slower barrier and protects the fairness bound (a donated task's latency cost to a worker's own tail) without giving up any merge the 2x gate needs; the 1M gate shape sits 16x above the floor.

- k=4 is the working fan-out for the 1M gate shape: 3.6-3.9x on 1M-by-1M, taking the ~3.3ms sequential merge to ~0.9ms, the section-6.5 arithmetic (5.78ms to ~1.5ms plus barrier on the gate box) reproduced in shape.
  k=8 adds little at P4 (granularity wall) and ~20% at P16+; the doc's half-the-pool cap is policy, not measured here.

## What the gate run still owes

The M1 gate run on the GamingPC box, after the execution model wires the F17 intent barrier and worker donation, must read:

- SINTER 1M-by-1M both-indexed at fan-out >= 2 against 2x min(rivals), plus the forced fan-out-1 row confirming or refuting the L12 ~1.6x prediction (doc 11 section 12, P6).
- The concurrent flat-SET row while algebra fans out, the empirical check on the fairness bound.
- The crossover on Linux DRAM, where seq ns/elem is ~10x this box's, so the break-even in elements may move; fanoutFloor stays 64k unless the box shows the barrier under 10x cheaper than one worker's share there too.
