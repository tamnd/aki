# Cross-shard BITOP: streaming hops vs gather residency

Co-located BITOP (lab 03) streams a chunk at a time so it holds only
(sources + 1) chunks resident no matter how long the bitmaps are. Cross-shard
BITOP keeps that memory bound, but now every chunk it reads is a hop to a
source's owner and every chunk it writes is a hop to the destination's owner.
The obvious alternative is to gather each source whole in one hop per source
shard, run the op over the full length on the coordinator, and write the result
back. That form pays far fewer hops but holds every source and the result whole,
which is exactly the multi-hundred-MiB peak the memory bar forbids.

This lab prices both sides so the slice's choice is visible: the measured
resident bytes and the modeled hop latency for a three-source AND over a length
sweep, two source shards, five microseconds modeled per shard hop.

```
size       streamHops   gatherHops      streamMiB      gatherMiB    streamLat    gatherLat
1MiB               50            3      0.25/0.25      4.01/4.00        250µs         15µs
16MiB             770            3      0.25/0.25    64.01/64.00       3.85ms         15µs
256MiB          12290            3      0.25/0.25 1024.00/1024.00      61.45ms         15µs
```

`streamMiB` and `gatherMiB` are `measured / model`: the measured column is
`TotalAlloc` for the pass (streaming reuses its buffers, so its total allocation
equals its peak), the model column is the `(sources + 1)` formula. They agree.

The streaming coordinator holds a flat 0.25 MiB peak across the whole sweep,
while the gather coordinator climbs to a full gigabyte at 256 MiB and three
sources. The price is hops: streaming's hop count grows with the length while
gather's stays flat, so at a few microseconds per hop the streaming form trades
some cross-shard latency for the bounded memory the bar demands.

Cross-shard BITOP is the rare case (co-located BITOP never hops at all) and the
residency is the hard constraint, so aki streams. The hop chatter is the priced,
bounded-per-chunk cost, not a regression on the co-located fast path.

## Run

```
go run ./labs/f3/m6/04_bitop_cross_hops
go test ./labs/f3/m6/04_bitop_cross_hops
```

`main_test.go` pins the hop-count and peak formulas the verdict rests on, and
checks the streaming assembly is byte-for-byte the whole-buffer answer over
unequal-length sources.
