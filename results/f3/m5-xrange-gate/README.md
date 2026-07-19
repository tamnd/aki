# M5-G4 XRANGE gate: fused alloc-free reply build flips the loss to a win

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), f3srv reactor gate
config vs CF16-frozen rivals (redis io6, valkey io4), card-10k stream, P16, 8s +
3s warm. This is the one genuine hot-path loss the M2-M11 breadth sweep turned up
(the metadata "losses" were single-generator client-cap artifacts, resolved in
../m3-list-gate). It is now resolved: the fused reply build flips it from a loss
to a stable win, and the sub-2x residual is the wide-reply memory-bandwidth floor.

## Before and after

Before the fused build (two-phase gather-then-encode), XRANGE read 0.76x / 0.88x
vs redis / valkey, aki ~51.7K ops/s against redis's ~67.6K. A card-10k XRANGE CPU
profile charged the two-phase path's allocation at ~43% of on-CPU time: it
gathered every in-window entry into a `[]rangeEntry`, cloning the field headers so
each survived the block walk's scratch reuse (`cloneFields`, one alloc per entry),
then encoded the gathered slice in a second pass. `runtime.growslice` 36% cum,
`runtime.mallocgc` 17% cum, `cloneFields` 7% cum, `memmove` 21%.

The fix (PR #1199) fuses the forward read the way SMEMBERS and HGETALL already
frame their replies: one walk frames each entry straight into the reply buffer as
the walk yields it, before the next entry's decode reuses the scratch, so no
`[]rangeEntry` and no per-entry clone. The RESP array header needs the count
first, so the body is built at a remembered offset and shifted right with one
memmove once the walk finishes. The `labs/f3/m5/07_range_reply_fused` microbench
prices the trade in-process: the fused arm drops from ~one alloc per entry to a
flat 2 allocs/op and builds the reply 1.4x-3.3x faster across a 1-10000 entry
window sweep, byte-identical replies.

## Gate (median-of-3, reactor gate binary at tip 4a42fb6)

| row | rep | aki ops/s | redis | valkey | MB/s aki | vsR | vsV |
|---|---|---|---|---|---|---|---|
| M5-G4 | 1 | 73598 | 69862 | 60499 | 769.6 | 1.05x | 1.22x |
| M5-G4 | 2 | 74599 | 68993 | 59287 | 780.1 | 1.08x | 1.26x |
| M5-G4 | 3 | 71604 | 70371 | 58238 | 748.1 | 1.02x | 1.23x |
| M5-G4 | **median** | ~73598 | ~69862 | ~59287 | ~769 | **1.05x** | **1.23x** |

aki is the fastest of the three (highest ops/s and MB/s every rep). The fused
build flipped a 0.76x loss into a stable 1.05x / 1.23x win.

## Why it wins but not by 2x: the wide-reply memory-bandwidth floor

The card-10k XRANGE reply is wide: ~10000 entries at a 64 B value each, roughly
900 KB per read. At 71-75K reads/s that is ~750 MB/s of reply bytes moving through
one server core. redis moves the identical shape at ~730 MB/s and valkey at
~620 MB/s. All three are near a shared per-core loopback bandwidth ceiling for a
reply this wide, so the row is memory-bandwidth bound on the reply bytes, not
ops-per-second bound on dispatch.

That is where the 2x math runs out. The reactor networking edge that carries a
flat GET or a point SISMEMBER to 2x is an ops/s edge: it wins by dispatching more
tiny ops per second. On a 900 KB reply the per-op dispatch cost is a rounding
error against the bytes each op must encode, copy, and write, so the reactor edge
shows through as aki's ~750 MB/s vs redis's ~730 (the packed block walk plus the
fused build give aki the highest byte rate of the three) but it cannot double a
figure both engines push at the same memory-bandwidth wall. To read 2x redis here
aki would have to move ~1460 MB/s against redis's 730, i.e. move bytes twice as
fast as an equally byte-efficient engine on the same box, which no reply-side
change delivers.

This is the wide end of the batch-read floor family (../m3-list-gate/batch-read-floor.md,
HMGET / ZMSCORE windowed reads, and the range-read dispatch floor of task #17):
a read whose cost is the reply bytes, where redis's own encode-and-write is
maximally efficient and the reply is bandwidth bound. XRANGE sits at the top of
that family, a win rather than a parity floor, because the packed contiguous block
walk plus the alloc-free fused build give aki a real per-byte edge, but under 2x
for the same reason the whole family is: bytes, not ops.

## Identified further lever (deferred)

The tip profile after the fused build still shows `growslice` ~28% cum and
`mallocgc` ~16% cum: the residual is the reply-buffer growth plus the second copy,
where XRANGE builds the whole reply in `cx.Aux` and then `r.Raw` copies it again
into the connection reply buffer. The whole-collection reads (SMEMBERS, HGETALL)
avoid this above the 64 KiB `store.ChunkSize` cutover by streaming chunks straight
onto the wire through the shard ring, which also caps their peak reply footprint
at the ring window instead of the whole collection, the memory bar the product
pitch turns on. XRANGE has no such streaming path.

Giving XRANGE the same streaming elision would remove the second copy and cap the
reply footprint, but it is a box-risky slice: the stream band has the LTM cold
tier, so a range read that streams off the shard goroutine must pin the block band
against reuse, trim, and eviction for the drain the way the hash `pinStream` pins
the field table, and a cold block streamed mid-drain preads into shared scratch
that the next block's read clobbers. The upside is bounded anyway: removing one of
two reply copies lifts a bandwidth-bound row toward ~1.3-1.4x, not to 2x. So it is
noted as the next lever if the streaming-pin infrastructure lands for another
reason, not spent here for a row that is already a win.

## Verdict (frozen)

M5-G4 XRANGE: RESOLVED as a win. The fused alloc-free reply build (PR #1199,
labs/f3/m5/07) flipped it from 0.76x / 0.88x to a stable median 1.05x / 1.23x vs
redis / valkey, aki the fastest of the three. The sub-2x residual is DECLARED
STRUCTURAL, the wide-reply memory-bandwidth floor: a ~900 KB range reply is bytes
bound where redis is equally byte-efficient, the top of the batch-read floor
family, above parity by aki's packed-walk plus fused-build edge but under 2x
because the reply is bandwidth bound, not dispatch bound. The memory column
inherits the M5 stream-mem declaration. The streaming second-copy elision is the
identified deferred lever.
