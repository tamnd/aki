# m0/31 shared hop-transport node pool

The GamingPC tiny-set memory gate loads ~1.9M single-member sets at 512
connections, pipeline depth 16. At that fan-in the reactor producer
(parse -> dispatch -> take) outruns the writer's recycle, so more hop-transport
nodes are outstanding at once than any per-connection L1 free list can hold. An
alloc profile of that cell put `shard.newBatch` at 98% of all allocation: nearly
every command minted a fresh ~6 KB node, and that transient burst, not the
dataset, drove peak VmHWM above redis and valkey. The dataset itself already
wins used_memory after the arena-embed flip (PR #1257); the peak was the only
sub-metric on the wrong side of the bar.

The fix is a second tier under the per-connection free channel: a runtime-wide
`sync.Pool`. `take` drains L1 first and falls through to the pool; `recycle`
fills L1 first and overflows to the pool instead of dropping the node to the
collector. L1 stays the contention-free steady path; the pool bounds retained
nodes to actual concurrency and kills the churn.

## What this measures

`BenchmarkBatchNodeChurn` in `engine/f3/shard` (take/recycle are unexported, so
the benchmark lives with them) runs the burst regime: 64 nodes outstanding per
round against an L1 of 8, eight times deeper than the free list. Two arms differ
only in the overflow rule.

- `l1only` drops an overflow node to the collector, the pre-change behavior.
- `shared` returns it to the runtime pool, the change.

## Result

```
BenchmarkBatchNodeChurn/l1only-10     21439    56431 ns/op   745487 B/op   168 allocs/op
BenchmarkBatchNodeChurn/shared-10    982880     1210 ns/op        0 B/op     0 allocs/op
```

The shared pool takes the churn from 168 allocations and ~745 KB per round to
zero once warm. That is the allocation the arena-embed flip's memory column
needed gone: with the burst transient eliminated, peak VmHWM settles to the live
index working set instead of the reactor's node backlog.

## Run

    bash labs/f3/m0/31_shared_batch_pool/run.sh
