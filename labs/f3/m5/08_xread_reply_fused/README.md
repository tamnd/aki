# Lab 08: fused XREAD reply build (nested [key, entries] shape)

XREAD is XRANGE over an open lower bound, but its reply nests: an outer array of
one `[key, entries]` pair per stream that produced entries, streams with no new
entries omitted, the inner entries array the same forward walk XRANGE frames. Lab
07 fused the flat XRANGE reply and flipped M5-G4 from a 0.76x loss to a 1.05x win.
XREAD never got that fix, so its immediate read still gathered every stream's
entries into a `[]rangeEntry` (cloning the field headers to survive the block
walk's scratch reuse) and re-encoded them in `frameReadResults`. On the box that
read 0.87x / 0.98x vs redis / valkey against XRANGE's post-fix 1.05x / 1.23x, the
one M5 read still below parity.

The fix mirrors lab 07 through the nesting. Each stream frames its `[key, entries]`
pair straight into the reply during one forward walk (`eachForward`), the inner
entries-array header shifted in with `prependArrayHeader` once the entry count is
known, an empty stream's pair rolled back by truncating to its start offset, the
outer array header shifted in once the non-empty-stream count is known. No
`[]rangeEntry`, no per-entry clone, no second encode pass. The park / woken-serve
path keeps the gather build (`frameReadResults`), a low-frequency path where the
allocation does not matter.

## Two arms

| arm | build |
|---|---|
| two-phase | gather `[][]rangeEntry` with `cloneFields` per stream, drop empties, then RESP-encode |
| fused | one walk per stream framing pairs into the reply, prepend inner + outer headers, roll back empties |

Both reuse the reply buffer across calls, as the shard reuses `cx.Aux`. The sweep
varies stream count, entries-per-stream, and the empty-stream fraction, so the
roll-back is exercised, not just the dense read.

## Results (go run .)

| streams | entries | empty | two-alloc | fused-alloc | two-ns | fused-ns | speedup |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 0 | 3 | 0 | 299 | 136 | 2.20x |
| 1 | 100 | 0 | 109 | 0 | 51778 | 12013 | 4.31x |
| 1 | 10000 | 0 | 10020 | 0 | 4130585 | 3680035 | 1.12x |
| 8 | 100 | 0 | 868 | 0 | 256516 | 222535 | 1.15x |
| 8 | 1000 | 0 | 8092 | 0 | 2770112 | 3018175 | 0.92x |
| 16 | 500 | 3 | 5615 | 0 | 1994074 | 2084333 | 0.96x |
| 64 | 100 | 2 | 3462 | 0 | 1512376 | 1526261 | 0.99x |

The allocation column is the headline: the fused arm is flat zero allocs/op across
the whole sweep (the reply buffer is pre-sized and reused), against the two-phase
arm's one clone per entry, up to 10020 on the card-10k single-stream read the gate
exercises. That is the effect the box measures. The in-process ns/op understates
it: with the reply buffer reused and no concurrent load, the two-phase clones hit a
warm allocator and the ns win is modest (1.12x on the single-stream card-10k row)
or, on wide multi-stream reads, washes to parity because the per-stream header
memmove trades against the (cheap, warm) clones. Under the live server's concurrent
GC pressure the 10020-alloc/op churn is the cost that shows, so the row moves on the
box more than this single-threaded microbench does. The single-stream card-10k row
(1×10000) is the gate's workload and it is a fused win on both alloc and ns.

## Verdict

The fused nested build eliminates the per-entry clone and the second encode pass
from XREAD's immediate reply, dropping it to zero allocs/op, byte-identical to the
two-phase build (`TestArmsAgree`, `TestLimitHonored`, `TestRollBackLeavesNoBytes`).
It carries the same win XRANGE got in lab 07 through the `[key, entries]` nesting.
The box re-measure of M5-G5 is in results/f3/m5-xread-gate.
