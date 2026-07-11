# Lab 04: end-leaf caching for the pop paths

Part of issue #544, the M2 zset milestone.
This lab lands before slice 6 (pops and random: ZPOPMIN, ZPOPMAX, ZMPOP, ZRANDMEMBER, ZREM with inline delete) so the slice bakes a settled caching decision, not a guess.

## Question

ZPOPMIN and ZPOPMAX hammer the extreme leaves of the counted B+ tree, and doc 12 section 6.7 sketches the fast form: walk the edge, emit pairs, delete the run as one bulk leaf trim with a single ancestor count fixup per touched path.
The open questions for slice 6 are what a pop costs without any cached state, whether cached end-leaf state pays for itself once inserts and deletes interleave with the pops, what events must invalidate that state, where the ZMPOP batch win saturates, and whether the pop and ZREM paths carry a p99 shoulder, because PRED-F3-M2-ZREMTAIL gates the p99, not the mean.
This lab prices all five and freezes the answer.

## Method

In-process, no server, no wire, benched against the real engine/f3/struct tree (#610) through its exported API only, at the frozen geometry from lab 01: 256-byte branches at arity 16, 512-byte leaves at 31 entries, u32 counts.
Scores are distinct 8-byte sortable keys from a splitmix64 bijection over a counter, members nil, the same convention as the tree's own benchmarks, so the Members callback never runs on a descent.

One limitation shapes everything and is stated up front.
The tree exports no leaf handles and this lab adds no hook, so through the public API a pop is two descents: a counted select to rank 0 (or n-1) to learn the extreme entry, then a routed delete of that exact key.
A cached end run can only delete the first descent; the routed delete stays.
Every cached-arm number here is therefore a lower bound on what an engine-internal fused pop can win, and the delete share of the batch table reads as the floor the fused form attacks.

The cached state is emulated as an exact run: the cache always mirrors the tree's first k entries, any write at or below its bound is an edge write it must observe, and a unit test proves the mirror against a sorted-slice model under 10k mixed ops.
So the maintenance costs in the interleave table are real, not modeled.

Four sweeps:

- Pure pops across cardinality 1k, 100k, 1M, 4M: the bare select leg, uncached ZPOPMIN and ZPOPMAX, the cached run at k=31 (one leaf), and a uniform-random ZREM as the contrast row between the hot edge spine and a cold random descent.
- Batch drain at 1M: ZMPOP COUNT=c as one primed run of c plus c deletes, c from 1 to 124, split into the prime share and the delete share.
- Interleave at 1M: pops mixed with writes at pop fractions 90, 50 and 10 percent, plus an adversarial shape where every insert lands strictly below the current minimum, each under three policies: naive (re-descend per pop), invalidate (any edge write drops the run), absorb (edge writes edit the run in place).
- Per-op latency quantiles for the p99 read.

The op schedule is deterministic and identical across the three policies, the harness panics if their tree evolution ever diverges, and every pure-pop loop carries an ascending tripwire.
`go run .` runs the whole sweep; `-quick` shrinks it for a fast check.

## What the doc predicts, and what this lab tests

- Counted descent in tens of ns at small cardinality (section 2.5). Tested by the sel0 column; confirmed, and it stays small even at 4M because the edge spine never leaves cache.
- Pops as an edge walk plus a bulk trim with one count fixup per touched path (section 6.7). Tested indirectly: the batch table shows the seek amortizes and the per-entry delete share is the remaining cost, which is exactly the term the fused form collapses.
- ZREM p99 shoulder from v1's deferral (section 6.10, 7.6 to 8.1 ms against Redis 6.4 in v1). Tested by the quantile table; the f3 tree shows no shoulder, p99 stays within 2x p50 on every arm.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, 200k ops per cell.

Pure pops, ns/op:

| card | sel0 | popmin | popmax | run31 | remRand |
|---|---|---|---|---|---|
| 1k | 8.8 | 79.2 | 76.0 | 66.1 | 54.7 |
| 100k | 13.7 | 104.4 | 111.2 | 94.4 | 185.5 |
| 1M | 13.5 | 109.2 | 125.1 | 98.8 | 300.2 |
| 4M | 17.2 | 128.4 | 149.6 | 113.0 | 603.9 |

Batch drain at 1M, ns per popped entry:

| count | prime | delete | total |
|---|---|---|---|
| 1 | 31.8 | 177.1 | 208.9 |
| 2 | 13.9 | 112.0 | 125.8 |
| 4 | 7.5 | 107.2 | 114.7 |
| 8 | 5.5 | 101.8 | 107.3 |
| 16 | 3.9 | 106.5 | 110.4 |
| 31 | 3.2 | 99.4 | 102.6 |
| 62 | 2.9 | 95.1 | 98.0 |
| 124 | 2.7 | 95.6 | 98.3 |

The c=1 and c=2 rows carry two timer reads per tiny group, roughly 40 ns spread over c pops, so they overstate; compare rows from c=4 up, and compare c=1 against the popmin column of the first table instead.

Interleave at 1M, ns/op, cache k=31, 400k ops per cell:

| workload | naive | inval | absorb | invals/1k (inval) | reprimes/1k (inval/absorb) |
|---|---|---|---|---|---|
| pop90/wr10 | 145.4 | 127.9 | 130.3 | 6.83 | 32.64 / 28.83 |
| pop50/wr50 | 202.6 | 187.4 | 189.5 | 20.44 | 28.93 / 15.58 |
| pop10/wr90 | 226.0 | 249.5 | 220.4 | 8.15 | 9.20 / 3.15 |
| adv pop50/ins50 | 104.1 | 123.9 | 96.8 | 249.71 | 249.71 / 0.07 |

Per-op latency quantiles, ns, 100k samples:

| arm | p50 | p99 | max |
|---|---|---|---|
| popmin @1M | 125 | 208 | 12792 |
| run31 @1M | 125 | 209 | 4708 |
| popmin @4M | 125 | 209 | 9541 |
| run31 @4M | 125 | 250 | 10292 |
| remRand @1M | 250 | 500 | 10917 |

The darwin timer ticks at about 41.7 ns, so the quantile values are quantized to multiples of it; the per-op timer itself costs 20 to 30 ns of each sample.

## Reading the sweep

The find leg is nearly free, which is the surprise that decides the lab.
SelectAt(0) costs 9 to 17 ns from 1k to 4M, barely growing, because sustained end pops keep the whole left spine, five branch nodes and the edge leaf, permanently in L1.
Contrast remRand: the same routed delete on a uniformly random key grows from 55 to 604 ns as the descent goes DRAM-bound.
The edge path is compute-bound at every cardinality this lab reaches; only the random path is cache-bound.
So a cached end pointer, whose entire value is skipping the find leg, can buy at most those 9 to 17 ns, and the run31 column confirms it: the cached run beats naive popmin by 10 to 15 ns everywhere, 10 to 13 percent, no more.
The routed delete is the real cost, and the batch delete share pins it at 95 to 100 ns per entry at 1M.

The maintenance tax is real and the invalidate policy pays it wrongly.
At pop90 both cached policies win as expected.
At pop10/wr90 invalidate loses to no cache at all, 249.5 against 226.0, because uniform writes land inside the 31-entry bound often enough (8.15 drops per 1k ops) that the pops mostly re-prime for nothing.
The adversarial shape is worse: every insert is a new minimum, invalidate drops the run 250 times per 1k ops, and loses 19 percent to naive.
Absorb never loses badly, sits within 3 percent of the best column on every uniform row, and wins the adversarial row outright (96.8 against 104.1) because the front-slot edit is O(1) and the run hands the fresh minimum straight back.
Answer to the tax question: yes, keeping cached state valid can cost more than it saves, but only under the drop-on-any-edge-write discipline; state that absorbs edge writes in place never goes negative on any shape tried.

The batch win is a prime-amortization story and it saturates at one leaf.
The prime share falls 31.8, 13.9, 7.5, 5.5, 3.9, 3.2 as c grows and is flat past c=31; the delete share barely moves.
Draining beyond one leaf per seek buys under 5 percent more.
So ZMPOP wants the section 6.7 form at leaf granularity: seek the edge once, emit up to a leaf's worth, trim, fix counts once per touched path, repeat per leaf until count is satisfied.

There is no p99 shoulder.
Sustained end pops make the edge leaf borrow from its right sibling almost every op once it drains to min fill, and that borrow is invisible: popmin p99 is 208 to 250 ns against a 125 ns p50, under 2x, at both 1M and 4M.
remRand holds the same 2x ratio (500 against 250).
The max column spikes to 5 to 13 microseconds a few times per 100k ops, consistent with scheduler and GC noise, not with any structural cliff.
v1's ZREM shoulder came from deferral; the f3 tree deletes inline and the shoulder is simply absent, which is what PRED-F3-M2-ZREMTAIL needs.

## Darwin caveat

Absolute ns get their Linux confirmation at the M2 gate run on GamingPC.
The structural findings travel: the find leg being a cache-resident spine walk, the invalidate policy losing under write-heavy shapes, absorb never losing, the batch saturating at leaf capacity, and the absent p99 shoulder are all compute-bound orderings with wide margins, not cache-size accidents.
The one cache-bound number is the remRand growth curve, and it only serves as contrast here.

## Verdict

Frozen for slice 6:

- Cache: yes, but cache the end-leaf identity, not an entry run.
  Keep the leftmost and rightmost leaf ordinals per tree, 8 bytes, nothing else.
  An entry-run mirror in engine code would be the invalidate-or-absorb machinery this lab priced, and the priced answer is that its win through any find-skipping design caps at the 9 to 17 ns find leg, which is not worth carrying list state for.
  A leaf ordinal gets the same skip for free: an insert or delete that lands in the edge leaf keeps the ordinal valid, which is exactly the absorb behavior that never lost a row.
- Invalidation rule: structural events on that leaf only, meaning split, merge or borrow that replaces it, drain to empty, or arena compaction.
  Re-validate lazily by re-descending the spine on next use; the re-descent costs the sel0 column, 9 to 17 ns, so eager repair is never worth complexity.
- The real lever is the fused pop, not the cache.
  Two API descents cost 109 ns at 1M and the delete leg alone is 95 to 100 of it, so slice 6 must implement pop as one pass: land on the cached end leaf, take the extreme entry, trim it inline, and fix ancestor counts on the way that one spine walk already knows.
  This lab's cached arm is a lower bound; the fused form is the section 6.7 shape and the gap it can close is the entire select leg plus the routed descent's binary searches.
- ZMPOP batch form: drain per leaf.
  One seek per leaf, emit up to 31 entries, one bulk trim, one count fixup per touched path, repeat.
  Saturation is at c=31; chasing larger per-seek batches buys under 5 percent and is not worth a second code path.
- ZRANDMEMBER rides SelectAt on a uniform rank and inherits the remRand descent cost, DRAM-bound at large cardinality, which is fine and unaffected by any of this.
- p99: no shoulder exists to defend against; inline delete holds p99 within 2x p50 through the worst borrow-every-op regime, so the ZREMTAIL p99 clause is safe from the structure side and needs no deferral machinery.

What slice 6 must encode as tests, surfaced by this lab's own suite:

- Model-checked pops: interleave pops, inserts and deletes against a sorted-slice model and assert every ZPOPMIN and ZPOPMAX returns exactly the model extreme, with tree Check passing after the churn (this lab's churn test shape).
- Adversarial edge inserts: alternate an insert strictly below the current minimum with a pop and assert the pop returns the fresh key every time, so a stale cached leaf can never serve a wrong extreme.
- Leaf-boundary batches: ZMPOP with count above leaf capacity must cross leaf boundaries with counts and links intact, verified by Check and by rank agreement with the model.
- Drain and reuse: pop a tree to empty, refill it, pop again; the cached ordinals must survive or re-validate through the empty transition.
- Cross-policy determinism where the engine keeps any cached state: the same op schedule with the cache disabled must produce the identical tree, byte for byte in the arena walk, so the cache is provably read-path-only.
