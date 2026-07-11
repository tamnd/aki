# Lab 10: the cross-shard SMOVE tax

Part of the M1 set milestone: the F17 intent path (spec 2064/f3 doc 03 section 6.7) carrying its first command, SMOVE over a source and destination on different shards, priced against the single-shard fast path #602 shipped.

## Question

What does a cross-shard SMOVE cost relative to the co-located one, and is that cost the barrier's fixed overhead or something that grows with the data?
The divert is only defensible if the co-located path stays untouched (the DrainExecute gate reads that separately) and the cross-shard path pays a bounded, size-independent tax for its two arms, three barrier hops, coordinator handoff, and loopback reply.

## Method

Live, in one process: `shard.New`, the real Sadd/Smove handlers, one synchronous connection.
The measured unit is the ping-pong pair (SMOVE there, SMOVE back), so both keys' state is byte-identical at every rep boundary and both arms move the same member through the same band forever.
The co-located arm runs the Smove point handler on a same-shard pair; the cross-shard arm runs SmoveCross under DoTxn on a split pair, the exact route dispatch takes.
Both sets are prefilled to the swept cardinality, which walks the bands: 8 (listpack), 1024, 65536 (hashtable).
The pipelined arm keeps 16 commands in flight before the first drain, reading throughput separately from latency.
Wall clock over a 300ms floor, the :1 reply checked every rep so a lost ball kills the run.

Run it with:

```
go run ./labs/f3/m1/10_smove_cross/
go test ./labs/f3/m1/10_smove_cross/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process, machine otherwise quiet.
Cells wander 5-15% run to run; the tax ratios are steadier than the cells.

Synchronous ping-pong, us per SMOVE:

| per-set n | S | co-located us | cross-shard us | tax |
|---|---|---|---|---|
| 8 | 2 | 3.59 | 9.61 | 2.7x |
| 1024 | 2 | 3.55 | 10.26 | 2.9x |
| 65536 | 2 | 3.73 | 9.35 | 2.5x |
| 8 | 8 | 3.52 | 10.74 | 3.1x |
| 1024 | 8 | 3.56 | 9.32 | 2.6x |
| 65536 | 8 | 3.55 | 9.23 | 2.6x |

Pipelined, depth 16, per-set n 8, us per SMOVE:

| S | co-located us | cross-shard us | tax |
|---|---|---|---|
| 2 | 0.60 | 3.99 | 6.7x |
| 8 | 0.57 | 3.93 | 6.9x |

## Verdicts, frozen

- The cross-shard tax is a flat ~6 us of fixed cost per command on this box, invariant across three orders of magnitude of set size and across shard counts.
  That is the shape the design predicts: two arm hops, an off-worker coordinator handoff, three barrier hops, and a loopback node, none of which touch the member data.
  The band work itself is the same term on both arms, which is why the rows do not move with n.

- Pipelining hides most of the co-located cost (3.6 to 0.6 us) and much less of the cross-shard cost (9.6 to 4.0 us), so the tax ratio widens to ~7x at depth 16.
  That is inherent to this workload, not a defect: the pipelined transactions all touch the same two keys, so the barrier serializes them by ticket while the co-located point ops batch through one owner.
  Cross-shard transactions over disjoint key pairs would overlap; a same-pair ping-pong is the worst case by construction.

- No floor or threshold falls out of this lab.
  The divert condition is exact (engage the intent path only when the pair really is cross-shard), the co-located path is untouched by construction, and 10 us for a two-owner atomic move is the right order for the substrate.
  The follow-up operand-gather slice inherits this price per remote operand hop and should re-read the table then.
