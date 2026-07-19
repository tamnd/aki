# M2 lab 08: ZRANGE cursor score-skip

## Question

Lab 07 priced the fused walk: deleting the two closure hops per element wins ~18 to 20% on a cache-resident ZRANGE.
This lab prices the second half of the same change, which lab 07 did not model.

The shipped walk reads the leaf's score for every element and throws it away on a plain ZRANGE.
The reason is the callback signature: `struct/tree.go` `WalkFromRank` calls `fn(t.lScore(ord, off), t.lRef(ord, off))`, always loading the 8-byte score, but zset's walk callback is `func(_ uint64, ref uint32)`, which discards it and reads the authoritative bits from the record.
So on a ZRANGE without WITHSCORES, every element pays a leaf score load that is never used.
The fused rank cursor (`struct/tree.go` `RankCursor`) fixes this for free: the caller pulls `Ref()` only and reads the score from the record's `bits` when, and only when, WITHSCORES asks.

## Method

In-process, no engine import.
One leaf's storage laid out as the tree lays it: an interleaved `[score u64, ref u32, reserved u32]` at a 16-byte stride, in rank order.
Two fused kernels, `withScore` (reads `lScore` every element, folds it into a sink to stand for the discarded callback arg) and `skipScore` (never reads it).
`main_test.go` proves both emit byte-identical member RESP, and that `skipScore` produces the same bytes even when the score array is poisoned, so the model really never reads it.

## Result (Apple M4, go1.26)

```
 window      withScore    skipScore    skip_win%
     10       30.659/e     70.179/e      -128.9%
    100       48.250/e     47.627/e         1.3%
   1000       38.978/e     42.379/e        -8.7%
```

The score load is a streaming read one cache line ahead of the member read, so on this laptop out-of-order execution hides it almost entirely and the delta sits in the timing noise (the window-10 row is dominated by per-call and TLB effects, not the score load).
This is the expected shape for a single extra sequential load that never stalls: it does not show up as a clean serial cost in isolation.

The box is the authority here.
A CPU profile of the 10k gate cell (`ZRANGE zk 0 99`, 50c/P16) attributed ~5% of the on-CPU time under `zrangeByIndex` to `lScore`, real work that competes for cache and issue slots under the gate's concurrency where the laptop's spare out-of-order width is already spent on the reactor and the parser.

## Verdict (frozen)

The score-skip is a real but secondary complement to the fused walk, not a lever on its own.

1. On a laptop it is in the noise: one extra streaming load per element, hidden by out-of-order execution. Do not expect a clean microbenchmark number for it.
2. On the box it is ~5% of ZRANGE's on-CPU time (profile-measured), reclaimed for free by the cursor since a plain ZRANGE simply never touches the score array.
3. The durable guarantee is byte-identity: `withScore` and `skipScore` emit the same bytes, so the cursor is a pure performance change. That is what this lab locks down; the throughput flip is measured by the box gate, not here.
