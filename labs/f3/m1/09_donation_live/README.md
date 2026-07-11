# Lab 09: worker donation, live

Part of issue #543, the M1 set milestone: the section-6.5 fan-out measured through the shipped path after the F17 donation substrate (engine/f3/shard/donate.go) went live.

Lab 08 priced the partition-parallel merge with lab-local executors and froze fanoutFloor = 65536.
This lab reads the same lever end to end: Runtime, Conn, the real set handlers, the real member tables, the real donation barrier.

## Question

Two things are asked.

First, what k-way scaling does the donated group loop deliver live, against lab 08's 3.6-4.7x model?
The one-shard arm is the serial oracle (FanOut runs the same tasks inline there), so the sweep is lab 08's seq-versus-pool read with the engine's actual barrier and the engine's actual kernel in the middle.

Second, where does the escalated draw resolve (drawfan.go) start paying, against the tentative drawFanFloor = 2048?
Below the floor both arms run the flat serial sample, so those rows also read the gate's cost as a control.

## Method

Live, in one process: `shard.New`, the real Sadd/Sintercard/Srandmember handlers, one synchronous connection, co-located keys.
Operands are production-shaped: the real 262144-member engagement threshold, P from deriveP, 16-byte members, overlap 0.5, `SetAlgebraMaintain` on.
SINTERCARD rather than SINTER carries the merge metric: the count reply isolates the merge from the reply encode, which is owner-side buffer work donation does not touch.
The draw sweep engages F13 through the real EscalateHotDraws seam on a 2M-member set (P8), then times SRANDMEMBER count across the floor.
Wall clock per command over a 300ms floor, counts cross-checked every rep.

Run it with:

```
go run ./labs/f3/m1/09_donation_live/
go test ./labs/f3/m1/09_donation_live/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, machine otherwise quiet.
Wander is 10-30% run to run, worst on the one-shard arms where GC lands inside the timing window; ratios are steadier than cells.
These are darwin numbers; the binding read is the GamingPC gate box.

Live k-way scaling, SINTERCARD, overlap 0.5:

| per-side n | P | S | ms/cmd | vs S=1 |
|---|---|---|---|---|
| 1048576 | 4 | 1 | 41.19 | 1.00x |
| 1048576 | 4 | 2 | 22.04 | 1.87x |
| 1048576 | 4 | 4 | 22.07 | 1.87x |
| 1048576 | 4 | 8 | 19.68 | 2.09x |
| 2097152 | 8 | 1 | 67.13 | 1.00x |
| 2097152 | 8 | 2 | 44.81 | 1.50x |
| 2097152 | 8 | 4 | 40.56 | 1.66x |
| 2097152 | 8 | 8 | 37.36 | 1.80x |

The live ceiling on this box is about 2x, not lab 08's 3.6-4.7x, and the gap is the kernel's regime, not the barrier.
Lab 08's operand pair (~32MB) sits inside the M4's system cache, and its 1.6-2.1 ns/elem merges scale to 3.9x on plain goroutines; the engine's real tables (slab, recs, buckets, run arrays) put the same merge at ~14 ns/elem of dependent DRAM loads, and four plain goroutines with GC off get exactly 1.9x on the identical group tasks (27.9ms to 14.1ms).
The donation machinery itself is not the limit: the same FanOut with four 5ms compute-bound tasks scales 4.0x at S=8 (engine/f3/shard donation suite), and every group task in the anatomy runs on its own donee.
Amdahl's other term was the per-command stream build (eight ~13k-entry tail copy-sorts, 6.7ms serial at 1M), which this slice moved onto the same donated fan (fanout.go streams); the 1M row above includes that.

Escalated draw floor, SRANDMEMBER count, 2M-member set, P8, F13 engaged:

| count | inline us (S=1) | donated us (S=8) | donated/inline |
|---|---|---|---|
| 512 | 166.4 | 159.2 | 0.96 |
| 1024 | 704.6 | 714.0 | 1.01 |
| 2048 | 268.4 | 171.4 | 0.64 |
| 4096 | 571.1 | 285.9 | 0.50 |
| 8192 | 1074.5 | 514.4 | 0.48 |
| 16384 | 2130.8 | 969.8 | 0.46 |
| 32768 | 4146.8 | 2002.1 | 0.48 |

Below the floor the two arms are the same serial code and read 0.96-1.01, the control.
At the floor the donated resolve already runs at 0.64 of inline, deepening to ~0.46-0.50 (2-2.2x) as the count grows, the same memory ceiling the merge shows.
The inline column steps down at 2048 because the code path changes there too: the two-phase draw (indices first, then a batched resolve) beats the interleaved per-member sample even run inline.

## Verdicts, frozen

- drawFanFloor = 2048, confirmed from the tentative value drawfan.go shipped with.
  The donated arm already pays 1.6x at the floor and the below-floor control rows are flat, so the gate costs nothing where it does not fire.

- Live donated scaling on darwin M4 is ~2x for the 1M and 2M algebra shapes and 2-2.2x for the escalated draws, and that is the box's memory system, not the donation barrier: the substrate scales 4x on compute-bound tasks and the plain-goroutine control reproduces the 1.9x exactly.
  Lab 08's 3.6-4.7x stands as the cache-resident model; the gate box (Linux DRAM, different MLP budget) must re-read this, and its 18.22 ns/member flat baseline suggests the parallel headroom there is larger, not smaller, since the serial arm starts further from the memory wall.

- The per-command stream build rides the donated fan (fanout.go streams), frozen as part of this slice: at production tail bounds it was a ~20% serial term at 1M and is read-only over the frozen operands, so it fans under the same contract as the group loop.

## What the gate run still owes

- The M1 gate rows on the GamingPC box: SINTER 1M-by-1M both-indexed at fan-out >= 2 against 2x min(rivals), the forced fan-out-1 row, and the concurrent flat-SET fairness row (doc 11 section 12, P6).
- The serial merge kernel's ~14 ns/elem live (against lab 08's ~2 lab-local) is the bigger lever this lab exposed: memberByOrd's dependent loads dominate, so a prefetch or ordinal-batched confirm pass is a candidate round-3 item if the box shows the same profile.
- Per-command allocation in the stream build (tail copies and group slots) is untuned; if the box shows GC in the tail, thread the shard scratch through mergeStream.
