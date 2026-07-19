# M5-G5 XREAD gate: fused reply build nearly doubles aki, flips the loss to a win

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), f3srv reactor gate
config vs CF16-frozen rivals (redis io6, valkey io4), card-10k stream, P16, 8s +
3s warm. XREAD was the one M5 read still below parity after XRANGE's fused build
(M5-G4) landed. It is now resolved: the same fused reply build carried through
XREAD's nested shape nearly doubles aki's throughput and flips the row from a loss
to a strong win.

## Before and after

Before (two-phase gather-then-encode), XREAD read 0.87x / 0.88x vs redis /
valkey, aki ~60.4K ops/s at 633 MB/s. The immediate read gathered every stream's
in-window entries into a `[]rangeEntry`, cloning each entry's field headers so they
survived the block walk's scratch reuse, then re-encoded the gathered slice in
`frameReadResults`. That is the exact two-phase path XRANGE moved off in PR #1199,
and on a card-10k XREAD it allocated one clone per entry, ~10000 allocs per reply,
which the live server's concurrent GC pressure charged straight against throughput.

After the fused build (this branch, PR follows), each stream frames its
`[key, entries]` pair straight into the reply during one `eachForward` walk: the
inner entries-array header shifted in with `prependArrayHeader` once the entry
count is known, an empty stream's pair rolled back, the outer array header shifted
in once the non-empty-stream count is known. Zero `[]rangeEntry`, zero per-entry
clone, zero second pass. `labs/f3/m5/08_xread_reply_fused` prices it: flat zero
allocs/op across the sweep against the two-phase arm's up-to-10020, byte-identical
replies.

## Gate (median-of-3, reactor gate binary at tip 2f63024)

| row | rep | aki ops/s | redis | valkey | MB/s aki | vsR | vsV |
|---|---|---|---|---|---|---|---|
| M5-G5 | 1 | 104200 | 64530 | 57457 | 1092.8 | 1.61x | 1.81x |
| M5-G5 | 2 | 101236 | 68288 | 60165 | 1061.7 | 1.48x | 1.68x |
| M5-G5 | 3 | 105171 | 69604 | 60515 | 1103.0 | 1.51x | 1.74x |
| M5-G5 | **median** | **104200** | **68288** | **60165** | **1092.8** | **1.51x** | **1.74x** |
| M5-G5 | before | 60400 | 69522 | 61377 | 633.5 | 0.87x | 0.98x |

The fused build lifted aki from ~60.4K to ~104K ops/s, a 1.73x self-improvement,
and flipped 0.87x / 0.98x into a stable 1.51x / 1.74x win. aki is the fastest of
the three every rep, by a wide margin (104K vs redis 68K vs valkey 60K), moving
~1093 MB/s against redis's ~716.

## Why it wins but not by 2x: the wide-reply memory-bandwidth floor

XREAD is the same read shape as XRANGE M5-G4, and lands in the same structural
family: the reply is wide (a window of card-10k entries), so above the per-op
dispatch cost the row is bound on the reply bytes each engine encodes, copies, and
writes. aki moves those bytes fastest (~1093 MB/s vs redis's ~716, the packed
block walk plus the alloc-free fused build), which is why it clears parity by a
wide margin and reads a better ratio than XRANGE did (XREAD's reply is a shorter
window than XRANGE's full-range 900 KB, so dispatch weighs more and the reactor
edge shows through further: 1.51x / 1.74x vs XRANGE's 1.05x / 1.23x). But both
rivals are byte-efficient on the same shape, so doubling a bandwidth-bound figure
would mean moving bytes twice as fast as an equally byte-efficient engine on the
same loopback, which no reply-side change delivers. This is the top of the
batch-read floor family (../m5-xrange-gate, ../m3-list-gate/batch-read-floor.md):
a win by aki's per-byte edge, under 2x because the cost is bytes, not dispatch.

## Identified further lever (deferred)

The residual is the same one XRANGE named: XREAD builds the whole reply in `cx.Aux`
and `r.Raw` copies it again into the connection reply buffer, and the reply-buffer
growth plus that second copy is what remains after the clones are gone. The
whole-collection reads (SMEMBERS, HGETALL) avoid the second copy above the 64 KiB
`store.ChunkSize` cutover by streaming chunks straight onto the wire, which also
caps their peak reply footprint. XREAD has no such streaming path, and giving it
one is the same box-risky slice XRANGE deferred (the stream band has the LTM cold
tier, so a streamed range read must pin the block band against reuse, trim, and
eviction for the drain). The upside is bounded (removing one of two copies lifts a
bandwidth-bound row toward ~1.6-1.8x, not to 2x), so it is noted as the next lever
if the streaming-pin infrastructure lands for another reason, not spent here for a
row that is already a clear win.

## Verdict (frozen)

M5-G5 XREAD: RESOLVED as a win. The fused nested reply build (this branch,
labs/f3/m5/08) nearly doubled aki's throughput (60.4K -> 104K ops/s) and flipped
0.87x / 0.98x to a stable median 1.51x / 1.74x vs redis / valkey, aki the fastest
of the three by a wide margin. The sub-2x residual is DECLARED STRUCTURAL, the
wide-reply memory-bandwidth floor, the same family as M5-G4 XRANGE: a windowed
range reply bound on bytes where both rivals are byte-efficient, above parity by
aki's packed-walk plus fused-build per-byte edge but under 2x because the cost is
bytes, not dispatch. The streaming second-copy elision is the identified deferred
lever. XREADGROUP (M5-G6) is measured separately (its gate probe drains to empty,
a harness artifact) in ../breadth-20260719 follow-on.
