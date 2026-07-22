# M0 lab 29: MGET fan-out scatter allocation elision

Spec: 2064/f3 doc 03 section 6.2 (tier-one multi-key fan-out). Gate row: M0-G10
(MGET 1.93x redis, a near-2x miss).

## Question

A multi-key command (MGET, MSET) is a tier-one fan-out: the reader routes each
key to its owning shard and splits the command into per-shard sub-commands that
all carry one reply sequence. The original scatter in `engine/f3/shard/fan.go`
`DoFan` built that split by allocating, every command:

- a fresh `order` routing slice (`make([]int, len(keys))`),
- a fresh `subs` sub-command list that grows by append,
- a fresh `argv` and `pos` buffer per shard touched.

On the gate cell (MGET of 16 keys against a 1M-key space, 8 shards) that is on
the order of 30-40 small allocations per command. The single-threaded rival
walks 16 dict entries and builds one array reply with none of it. Under the
gate's `GOGC=20` that churn is exactly the kind of per-command waste that pins a
throughput row just under 2x.

## Elision

Route once into reader-owned scratch, count the sub-commands in a cheap
allocation-free pass (so the coordinator countdown is still final before the
first enqueue), then scatter out of one reused `argv` and one reused `pos`
buffer. `enqueueFan`'s node builder (`hopBatch.add`) copies every argument's
bytes into its own span table synchronously, so both scratch buffers are safe to
reset and reuse the moment the enqueue returns. The MGET position blob is
appended to `argv` in the scatter's own scope, so a one-time growth of the reused
backing persists across sub-commands instead of reallocating on every flush.

The scratch lives on `Conn` (`fanOrder`, `fanArgv`, `fanPos`), reader-goroutine
only, like the rest of the reader state.

## Method

In-process, no server, no wire, no engine import. The two scatter shapes are
reproduced verbatim from `fan.go` (the make-per-command build and the
reused-scratch build). The enqueue is modelled by the same byte copy `b.add`
does into a node span table (a reused sink), so the consumer side allocates
nothing and only the scatter's own churn is measured. `TestScatterEquivalence`
asserts the reused-scratch scatter hands every owner a byte-identical slice of
the command across key counts 1..257 and both MGET and MSET, so the change is
proven safe before its throughput is read.

## Result (go test -bench, apple dev box, noisy; pattern is the signal)

| shape     | old alloc/op | new alloc/op | old ns/op | new ns/op |
|-----------|-------------:|-------------:|----------:|----------:|
| MGET 16   |           37 |            0 |     ~3200 |     ~1125 |
| MSET 16   |           29 |            0 |     ~2700 |      ~911 |

Scatter allocations go to zero on the steady state (`TestScatterNewAllocFree`);
the scatter itself runs ~2.5-3x faster in isolation. The absolute ns are a small
fraction of the ~350 ns end-to-end per-op budget, so the box gate decides how
much of MGET's 1.93x -> 2x gap the removed GC pressure buys. This lab proves the
mechanism (zero-alloc, byte-identical scatter); the throughput verdict is the
GamingPC M0-G10 run.

## Box gate (GamingPC, reactor vs CF16-frozen rivals, dual gen, median of 3)

MSET is redis-benchmark's builtin 10-key test; MGET is a 16-key custom command
(`key:__rand_int__` x16) over a primed 1M keyspace. N=3M per generator, c256 P16.

| row  | reactor | vs redis 8.8 | vs valkey 9.1 | gate (min) |
|------|--------:|-------------:|--------------:|-----------:|
| MSET |  958160 |        2.24x |         1.84x |      1.84x |
| MGET |  665520 |        1.42x |         1.17x |      1.17x |

## Verdict

The elision is unconditionally correct (proven byte-identical) and strictly
reduces per-command allocation and CPU on every MGET/MSET, so it ships. On the
box it clears 2x against redis on MSET (2.24x) but not against valkey (1.84x),
and MGET stays coordination-bound (1.17x vs valkey).

The residual is not allocation, it is fan-out coordination: a 16-key command
scattered across a 1M keyspace touches on the order of eight shards, so one
client command becomes ~eight internal hops (enqueue + wake + partial + gather +
reorder), while the single-threaded rival amortizes one parse over sixteen dict
lookups in-thread. Removing the per-command allocations does not remove the hops.

Two named levers remain, neither a physics wall:
- MSET is one step from the valkey bar (needs ~+9% on reactor). The FanOK reply
  gather and per-shard enqueue are the next surfaces.
- MGET wants co-located reads (reader reads owner shards without a hop, guarded
  for the single-writer invariant) or hop-batching of many clients' sub-commands
  to the same shard. That is a shard-dispatch arc, tracked separately.
