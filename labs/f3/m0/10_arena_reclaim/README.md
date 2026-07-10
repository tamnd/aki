# Lab: the arena reclaim threshold

Spec 2064/f3, M0 gate follow-up, lab 10.

## The question

The arena never reclaimed a replaced or deleted record: DEL and overwrite only adjusted the ledger, so sustained SET at 4KiB walked the bump allocator to ErrFull and killed five M0 gate cells (results/f3/m0-gate.md, issue #542).
The reclaim slice adds in-place reuse on the separated band, per-segment dead accounting, and a dead-fraction compactor at the drain boundaries.
What dead fraction should make a segment a compaction victim: 1/8, 1/4, or 1/2?

## Method

`go run . -pass <p> -frac <f>` runs one cell per invocation, in process, no server, no wire, so maxrss is honest per cell.
The loop emulates the shard worker's boundaries: batches of 1024 ops (the drainPassCap x batchCap drain-pass ceiling), the O(1) ArenaTight check between batches, and the idle trigger (ArenaReclaimable at the 1MiB floor) every 64 batches.
The reported pause is the batch wall time, compaction included, the drain-to-drain gap a client would see.
Both passes fill 1M keys and then run to 3x arena turnover in written bytes.

Pass one (`-pass inplace`) is the gate workload itself: sustained 4KiB uniform overwrite.
With in-place reuse landed it generates no dead bytes at all, because a fresh run's reserved capacity is exactly its aligned length and a same-size overwrite always fits, so it proves the steady state but cannot sweep a threshold.

Pass two (`-pass churn -frac {off,1/8,1/4,1/2}`) makes the compactor work: uniform-random key, value size a coin flip between 512B (embedded) and 4KiB (separated), so about half the writes change band and must republish the record, leaving dead bytes behind live neighbors in every segment.
One key in eight is pinned, written at fill and never again, the long-lived residents every real cache holds: their records keep segments from dying whole, so the fully-dead backstop alone cannot save the workload and relocation has to earn its keep.
`-frac off` disables the threshold entirely and shows what the emergency paths do on their own.
Arena 4GiB, about 2.4GiB live after fill.

## What the sweep broke, twice

The first sweep run killed two cells and both deaths changed the compactor; the numbers below are from the fixed code.

Unbudgeted selection wedges a tight arena.
The 1/8 cell picked dozens of barely-dead victims at once, copied their survivors into the last free segments without draining any victim, and died at 0.18x turnover with the arena wedged.
Victim selection is now budgeted: most-dead segments first, cumulative survivor bytes bounded by the free tail space, and the pass loops so each drained victim funds the next round.
engine/f3/store/reclaim_test.go TestCompactBudgetedWhenTight pins it.

A high threshold starves the tight path.
The 1/2 cell died at 1.78x turnover with a third of the arena dead, every byte of it in segments under the threshold, which breaks the backpressure contract that ErrFull means live bytes genuinely exceed capacity.
A tight arena now widens eligibility to any segment with dead bytes; the threshold only schedules the proactive work.

## Results

Apple M4 (4P + 6E), 24GiB, macOS, Go 1.26, in process, quiet box.
Two runs per cell; ops/s moved a few percent between runs, the shape held.
On a loaded box absolute ops/s roughly halved and pause p99 picked up 30 to 50ms scheduler noise in every cell, compacting or not, so only same-condition comparisons mean anything.

Pass one, 4KiB uniform overwrite, 1M keys, 3x turnover (the workload that returned ErrFull before the slice):

| run | ops/s | gap p50 | gap p99 | gap max | compact passes | arena |
|---|---|---|---|---|---|---|
| 1 | 1.60M | 587µs | 1.12ms | 1.5ms | 0 | flat at 4.05GiB |
| 2 | 1.60M | 584µs | 1.14ms | 4.7ms | 0 | flat at 4.05GiB |
| 3 | 1.68M | 566µs | 1.04ms | 5.2ms | 0 | flat at 4.05GiB |

Every overwrite lands in place; the compactor never fires because there is nothing to reclaim.

Pass two, band-flip churn with a pinned eighth, 5.6M ops, 3x turnover, run 1:

| frac | ops/s | gap p50 | gap p99 | gap max | passes | segs freed | compact time | arena used | maxrss |
|---|---|---|---|---|---|---|---|---|---|
| off | 0.87M | 517µs | 36.3ms | 45ms | 91 | 169 | 3.29s | 3.97GiB | 4.04GiB |
| 1/8 | 0.98M | 426µs | 38.0ms | 53ms | 82 | 873 | 3.23s | 2.67GiB | 3.06GiB |
| 1/4 | 1.03M | 428µs | 36.7ms | 45ms | 76 | 789 | 2.92s | 2.72GiB | 3.13GiB |
| 1/2 | 1.25M | 451µs | 1.1ms | 99ms | 46 | 275 | 1.84s | 3.59GiB | 4.05GiB |

Run 2:

| frac | ops/s | gap p50 | gap p99 | gap max | passes | segs freed | compact time | arena used | maxrss |
|---|---|---|---|---|---|---|---|---|---|
| off | 0.93M | 456µs | 35.8ms | 101ms | 91 | 169 | 3.35s | 3.97GiB | 4.05GiB |
| 1/8 | 0.99M | 424µs | 38.2ms | 48ms | 82 | 873 | 3.20s | 2.67GiB | 3.06GiB |
| 1/4 | 1.03M | 426µs | 36.8ms | 48ms | 76 | 789 | 2.94s | 2.72GiB | 3.13GiB |
| 1/2 | 1.25M | 444µs | 979µs | 109ms | 46 | 275 | 1.87s | 3.59GiB | 4.05GiB |

The shape: a relocating pass costs 30 to 40ms (a 1M-entry index walk plus up to 64MiB of survivor copies), so p99 shows it whenever passes exceed one batch in a hundred.
off and 1/2 push the arena to its ceiling and live off the tight-path widening; off never reclaims proactively at all, and 1/2 reclaims so late that maxrss equals the arena and the deepest emergency rounds show up as 100ms max gaps.
1/8 and 1/4 keep the proactive path in charge: a gigabyte lower footprint, bounded worst gaps, and the throughput cost of the extra passes is a few percent.
1/8 does the same work as 1/4 in more, smaller passes and ends slightly slower for slightly less footprint.

## Verdict

Freeze 1/4: arenaSegDeadNum = 1, arenaSegDeadDen = 4 in engine/f3/store/reclaim.go.
It edges 1/8 on ops/s at the same pause p99 and effectively the same footprint, and it does not buy 1/2's fifth of throughput at the price of riding one segment from full with the emergency widening doing all the reclaim, the regime that returned ErrFull before the widening existed.
The sweep's real product is structural either way: budgeted most-dead-first victim selection and the tight-path eligibility widening are what turned every cell from a death into a steady state.
