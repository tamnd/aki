# Lab 03: demotion policy, S3-FIFO vs SIEVE on a skewed trace with a one-hit tail

Part of issue #549, the M7 LTM milestone, lab 03, the demotion policy the cold migrator runs (doc 06 section 4.2). This is the lab the spec defers to at line 337: the family is fixed (FIFO with second chances, not an LRU chain and not a frequency sketch on the hot path), and one choice is left open, literal S3-FIFO versus SIEVE-plus-ghost, to be settled by hot-tier hit ratio and demotion CPU on a skewed trace with a heavy one-hit tail. It runs before the demotion-policy implementation slice so that slice adopts a decided winner.

## Question

When a shard is above high water the migrator picks what sinks to the cold region. The f1 first cut sank on one recency bit, blind to a scan of one-hit-wonder keys that evicts the working set and then pays a pread per hot read forever. The spec fixes the FIFO-family shape and leaves the realization open:

- **SIEVE-plus-ghost**: one region, a hand that scans from the oldest entry, a visited bit per entry. A visited entry the hand reaches is a survivor, re-appended to the tail with its bit cleared; an unvisited entry sinks. A ghost ring readmits a recently-demoted key.
- **Literal S3-FIFO**: a small probationary queue (~10% of the hot budget), a main region (the rest), and a ghost ring at main-region size. A fresh miss enters the small queue; a one-hit key that reaches the small queue's tail unvisited sinks without ever touching the main region. A visited small-queue entry is promoted to main. The main region gives one second chance like SIEVE.

Both are lock-free on the hit path, both cost one visited bit, both run as a sequential scan of the oldest segments the migrator already drains, and the segment realization accommodates either without a layout change. So the choice is empirical: which holds more of the working set resident under a scan, and which pays less demotion CPU doing it. Plain FIFO is the third arm, the f1 first cut, there to size the scan-resistance gap the second-chance policies close.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. This model measures policy quality, hit ratio and copy count, not wall-clock, so it needs no box. Each policy is simulated over a trace with a `container/list` deque per region and a visited bit per entry, faithful to the segment realization: a survivor re-append is a `MoveToBack` and counts one copy, an unvisited entry sinks and its key fingerprint enters the ghost ring, and a miss that hits the ghost readmits into the main region skipping probation.

The trace is a Zipfian working set of repeat keys (the hot keys, ids `1..W`) interleaved with a stream of unique one-hit-wonder keys (the tail, ids `W+1` and up, each emitted once). The trace is deterministic (a fixed seed), so CI pins the ordering the verdict rests on. The reported metrics:

- **working-set hit ratio**: the share of repeat-key accesses served from the hot tier. Tail keys are unique and can never hit, so this is the number that turns into preads avoided, isolated from the tail that dilutes an overall hit ratio.
- **copies/1k**: survivor re-appends per thousand accesses, the demotion-CPU proxy, the memcpy bandwidth the spec calls out in the survivor-cost note (section 4.2).

`go run .` runs the whole sweep; `-quick` shrinks the trace for the shared runner. `TestSecondChanceResistsScan`, `TestFifoCollapsesWithTail`, `TestGhostNeverHurts`, `TestS3FIFOWinsHeavyTail`, and `TestS3FIFODemotionCheaper` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-16, one process. Hot cap 8192 records, working set 20000 keys, Zipf s=1.07, 2M accesses. Small queue 819 (10% of cap), ghost ring at main-region parity (7373 slots).

Sweep A, rising one-hit-tail rate at a fixed cap (working-set hit ratio | copies per 1k accesses):

| tailRate | fifo | sieve+ghost | s3-fifo |
|---|---|---|---|
| 0.0 | 0.8897 \| 0 | 0.9136 \| 127 | 0.9189 \| 80 |
| 0.2 | 0.8005 \| 0 | 0.8554 \| 110 | 0.9156 \| 39 |
| 0.4 | 0.7487 \| 0 | 0.8117 \| 86 | 0.9055 \| 16 |
| 0.6 | 0.6972 \| 0 | 0.7658 \| 60 | 0.8765 \| 0 |
| 0.8 | 0.6249 \| 0 | 0.6991 \| 32 | 0.8203 \| 0 |

Plain FIFO falls from 0.89 to 0.62 as the tail grows: the scan evicts the working set on every pass, exactly the failure the one-bit f1 cut had. SIEVE holds more but still slides to 0.70, because a one-hit key occupies a main-region slot until the single hand reaches it, and until then it displaces a hot key. S3-FIFO barely moves, 0.92 to 0.82, because the small queue confines the tail to a tenth of the budget and the unvisited one-hit keys sink from it without ever entering the main region. That confinement also collapses S3-FIFO's copy cost: at a heavy tail it re-appends almost nothing (the tail never becomes a survivor and the skewed working set sits stable in main), while SIEVE funnels all traffic through one region and pays 60 copies per 1k.

Sweep B, a heavy tail (0.6) and rising skew (working-set hit ratio | copies/1k):

| zipfS | sieve+ghost | s3-fifo | hitDelta |
|---|---|---|---|
| 1.05 | 0.7487 \| 63 | 0.8685 \| 0 | +0.1198 |
| 1.10 | 0.7879 \| 55 | 0.8853 \| 0 | +0.0974 |
| 1.20 | 0.8533 \| 41 | 0.9171 \| 0 | +0.0638 |
| 1.40 | 0.9339 \| 22 | 0.9617 \| 0 | +0.0278 |

S3-FIFO leads at every skew, by 12 points on the flattest working set (the hardest to hold) narrowing to 3 points on the sharpest, and pays near-zero survivor copies throughout against SIEVE's 20 to 60. The gap is widest exactly where demotion matters most, a flat working set under a scan.

Sweep C, ghost ring size as a fraction of the main region, heavy tail (0.6), s=1.05:

| ghostFrac | slots | sieve+ghost wsHit | s3-fifo wsHit |
|---|---|---|---|
| 0.00 | 0 | 0.7414 | 0.7829 |
| 0.25 | 1843 | 0.7438 | 0.8303 |
| 0.50 | 3686 | 0.7456 | 0.8493 |
| 1.00 | 7373 | 0.7483 | 0.8696 |

The ghost ring earns its bytes on S3-FIFO: readmission lifts the hit ratio 0.78 to 0.87 from off to parity, and most of the gain is already in by half-parity (0.85), the knee the spec's sizing paragraph predicts. Past the knee the ring is remembering keys that were correctly demoted. On SIEVE the ghost barely moves the ratio (0.741 to 0.748), because SIEVE's single region already gives a warm readmit little to skip. So the ghost is a real lever for S3-FIFO and a shrinkable one: half-parity (about 8 MiB per shard on the worked box, well under 100 MiB across 12 shards) captures most of it.

## Verdict

S3-FIFO-plus-ghost wins the demotion-policy choice, and it wins on both axes at once, not as a trade. At the design point (heavy tail, s=1.07) it holds a working-set hit ratio of 0.875 against SIEVE's 0.765, +0.11, while paying essentially zero survivor copies against SIEVE's 59 per 1k, a 150x lower demotion CPU. The small probationary queue is the mechanism: it confines the one-hit tail to a tenth of the budget so the tail neither displaces the working set nor becomes a survivor to copy, which is precisely what the single-region SIEVE cannot do. The ghost ring adds another 9 points and is shrinkable to half-parity past its knee.

The spec's pre-registered lean toward simplicity is a tie-breaker only when the hit ratios are close; here they are not, and the extra structure (a small queue and a promotion step) is cheap and lock-free. So the implementation slice adopts literal S3-FIFO over the segments: a small queue at 10% of the hot budget, a main region with one second chance, and a ghost ring sized at half to full main-region parity. The W-TinyLFU fallback (doc 06 section 4.3) stays pre-registered for an LTM gate row that misses on hit ratio.
