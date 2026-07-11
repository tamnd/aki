# Lab 04: the cross-shard LMOVE hop cost

Part of the M3 list milestone: the F17 intent path (spec 2064/f3 doc 03 section 6.7, doc 13 section 5) carrying LMOVE and RPOPLPUSH when the source and destination keys span shards, priced against the single-shard point path slice 6 shipped.

## Question

The gather (lab 11) copies every member of every remote operand, so its tax rises with the data.
The move is the opposite by construction: an LMOVE touches exactly one element no matter how deep the two lists are.
So the question is whether the cross tax is flat in the data, the way lab 10 found the cross-shard SMOVE flat, and what the fixed price of the three-hop remote path is in absolute us.
The cross path runs three owner hops under the barrier: the source hop peeks its end element and clones it, the destination hop pushes the clone, and a final source hop removes the moved element and drops an emptied source.
That price is the whole cost the design adds over a co-located move, so name it honestly.

## Method

Live, in one process: `shard.New`, the real Rpush/Lmove handlers, one synchronous connection.
The co-located arm places both keys on one shard and runs the Lmove point handler; the cross arm places each key on its own shard and runs `list.LmoveCross` under `DoTxn`, the exact route dispatch takes.
Each timed call moves one element and the next call moves it back, so both lists hold their depth across the whole run and never drain, and the number measured is the steady per-move cost.
Two sweeps:

- Bands: element size and list depth swept across the inline listpack band (small and wide elements, shallow) and the native chunked deque (deep, multi-chunk), so the move runs the same band code the point path does.
- Directions: the four LMOVE directions at one band, since each touches a different pair of ends but pays the same three hops.

Wall clock over a 300ms floor.

Run it with:

```
go run ./labs/f3/m3/04_lmove_cross/
go test ./labs/f3/m3/04_lmove_cross/
```

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-12, one process, machine otherwise quiet.
Cells wander 5-15% run to run; the tax ratios are steadier than the cells.

Cross-shard LMOVE hop cost, one element moved per call, us per move:

| band | element bytes | depth | co-located us | cross-shard us | tax |
|---|---|---|---|---|---|
| inline small | 8 | 16 | 4.69 | 10.60 | 2.3x |
| inline wide | 200 | 16 | 4.87 | 12.78 | 2.6x |
| native | 100 | 400 | 4.23 | 10.51 | 2.5x |

Cross-shard LMOVE by direction, inline band (8B x 16), us per move:

| direction | co-located us | cross-shard us | tax |
|---|---|---|---|
| LEFT LEFT | 4.19 | 10.48 | 2.5x |
| LEFT RIGHT | 4.09 | 10.80 | 2.6x |
| RIGHT LEFT | 4.17 | 10.42 | 2.5x |
| RIGHT RIGHT | 4.27 | 10.91 | 2.6x |

## Verdicts, frozen

- The cross-shard move tax is flat in the data, exactly as lab 10 found for SMOVE.
  A move touches one element, so the deep native list (depth 400) prices the same as the shallow inline list (depth 16): the co-located arm holds near 4.5 us and the cross arm near 11 us across every band.
  There is no per-element slope because there is nothing per-element to copy beyond the one moved element, which the source hop clones into heap storage the way the co-located move already clones its popped bytes.

- The three-hop remote path costs about 6 to 7 us over the co-located move, roughly 2 us per hop.
  The co-located arm is one owner step at ~4.5 us; the cross arm is the barrier plus three hops (source peek and clone, destination push, source pop and drop) at ~11 us, so the added ~6.5 us is the price of leaving the single-owner fast path.
  This sits just under lab 11's ~3 to 4 us per remote-operand hop, which is the expected shape: a move hop does a single end op where a gather hop clones a whole operand, and the barrier setup is amortized over three hops here rather than paid per operand.

- The direction does not move the cost.
  All four LMOVE directions land within noise of each other (2.5 to 2.6x), because the hop count is three regardless of which ends the pop and push touch.
  RPOPLPUSH is the RIGHT LEFT row and prices the same, since it is LmoveCross with fixed directions.

- No floor or threshold falls out of this lab, same as labs 10 and 11.
  The divert condition is exact (engage the intent path only when the two keys really span shards), the co-located path is untouched by construction, and the price the cross path pays is the price the design names: freeze both keys, capture the element the source read discovers, publish it at the destination, then remove it at the source.
  The push-before-pop ordering keeps the transient state element-in-both, which is invisible under the barrier and is the on-theme choice for a list where an element in neither list would be the phantom-hole analog the P9/L15 lesson warns against.
  The gate-box read is the only open question the lab defers: the tax is a fixed hop cost, not a bandwidth term, so the gate box (further from the memory wall than this M4) should move the absolute us but hold the flat-in-data shape.
