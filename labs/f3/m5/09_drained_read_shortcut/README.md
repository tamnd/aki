# Lab 09: drained-cursor read short-circuit

A polling `XREAD` from `$` and a caught-up `XREADGROUP` consumer both ask for
entries newer than an ID that is already the newest. The read is empty, but the
original path did work to find that out: `readAfter` resolved to a forward range
walk over `(after, +inf]`, which seeks the tail block the after-ID lands in and
walks every entry in it, testing each against the exclusive lower bound, all
failing, before running off the end with nothing. On a card-10k stream the tail
block holds hundreds of entries, so every empty poll walked hundreds of entries to
return nil. On the box an `XREADGROUP` poll that had drained its group read redis
at 2.27M nil returns/s against aki's 279K, an 8x gap on the group-empty fast path.

Redis makes an O(1) not-newer check first (group cursor vs stream last-id) and
returns nil without walking. The fix is the same: `readAfter` and the XREAD
immediate loop return nil the moment the after-ID is at or above `s.lastID`, before
any seek or walk.

## Two arms

| arm | work |
|---|---|
| walk | seek the tail block, walk every entry testing the lower bound, return nil |
| shortcut | one cmp: `after >= lastID`, return nil |

## Results (go run .)

| tail-entries | walk-ns | shortcut-ns | speedup |
|---|---|---|---|
| 16 | 14 | 2 | 7.0x |
| 64 | 42 | 2 | 21.0x |
| 256 | 225 | 2 | 112.5x |
| 1024 | 835 | 2 | 417.5x |
| 4096 | 3597 | 10 | 359.7x |

The walk arm scales with the tail block's entry count; the shortcut is flat. The
box `XREADGROUP`-drained gap (redis 8x) sits in the low end of this curve, the
effective tail-block walk per empty poll.

## Verdict

The not-newer short-circuit removes the whole tail-block walk from an empty poll,
correctness anchored by `TestArmsAgreeOnDrained` (a drained cursor reads empty both
ways) and `TestArmsAgreeWithNewEntry` (a below-last after-ID falls through to the
real walk, so a genuine new entry is never swallowed). It benefits both the polling
XREAD-from-`$` and the caught-up XREADGROUP consumer. Box re-measure in
results/f3/m5-xread-gate follow-on.
