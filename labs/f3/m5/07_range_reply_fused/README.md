# Lab 07: fused XRANGE reply build vs two-phase gather-then-encode

Part of issue #547, the M5 stream milestone, lab 07, the XRANGE/XREVRANGE reply-build path (doc 14 section 6.3). This is the lab the M5-G4 range-read gate row depends on: it prices the reply-build strategy before the change lands in `range.go`, per the labs-per-perf-change rule. It backs the fused single-walk path that flips the range read off its two-phase alloc floor.

## Question

XRANGE resolves its bounds to a `[lo, hi]` window and walks the block band's entries in order. The block walk (`walkIn`) yields each entry's field headers as views into a scratch slice it reuses per entry, so a view is only valid until the walk decodes the next entry. The original reply build took two passes over that:

1. a forward walk that gathered every in-window entry into a `[]rangeEntry`, cloning the field headers (`cloneFields`) so a gathered entry survived the scratch reuse, then
2. a second pass that RESP-encoded the gathered slice into the reply buffer.

That is three allocation sources on the resident hot path: the `[]rangeEntry` slice grows, `cloneFields` allocates one header slice per entry, and the reply buffer grows. A box CPU profile of a card-10k XRANGE read them back as ~43% of the on-CPU time (`growslice` 36% cum, `mallocgc` 17% cum, `cloneFields` 7% cum, `memmove` 21%), against a 0.76x / 0.88x gate loss vs redis / valkey.

The whole-collection reads (SMEMBERS, HGETALL) already avoid this: they frame each element straight onto the wire during the read, no gather, no clone. The question is whether XRANGE can do the same. The wrinkle is the RESP array header, which needs the entry count first, and the forward walk does not know the count until it finishes. So: can the range read fuse its two passes into one walk that frames each entry as the walk yields it, and pay only a single header shift for the count it cannot know upfront, and does that actually beat the two-phase gather.

## Method

In-process, over a source that models the engine's walk faithfully: a block whose walk yields field-header views out of a scratch slice it overwrites per entry, so the two-phase arm MUST clone to stay correct (the exact reason `cloneFields` exists in `range.go`) and the fused arm must consume each entry before the next decode. Both arms reuse the reply buffer across calls, exactly as the shard reuses `cx.Aux`, so the measured delta is the gather slice plus the per-entry clones plus the second encode pass, minus the one header-shift `memmove` the fused arm adds.

Two arms over the identical entry source and reused reply buffer:

- **two-phase**: gather `[]rangeEntry` with `cloneFields`, then RESP-encode the gathered slice, the original path.
- **fused**: one walk framing each entry into the reply buffer, then `prependArrayHeader` shifts the body right to insert the array header once the count is known, the shipped path.

`go run .` runs the alloc sweep and the ns/op sweep across window sizes from 1 to 10000 entries, two 8-byte-named 16-byte-valued fields per entry. `-quick` shrinks the windows for the shared runner. `TestArmsAgree`, `TestPrependArrayHeader`, `TestPrependAtOffset`, `TestReusedBufferNoAlias`, and `TestFusedFewerAllocs` are what CI drives: they hold the byte-for-byte equivalence that makes the fused arm a drop-in, pin the header-shift helper across digit-width changes and non-zero body offsets, and assert the fused arm allocates strictly less.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-19. Two fields per entry, 8-byte names, 16-byte values. Reply buffer reused across ops (the `cx.Aux` shape).

Allocs per reply, lower is better:

| window | two-phase | fused |
|---|---|---|
| 1 | 3 | 1 |
| 10 | 16 | 1 |
| 100 | 110 | 2 |
| 1000 | 1013 | 2 |
| 10000 | 10021 | 2 |

The two-phase arm allocates roughly one per entry: the `[]rangeEntry` slice growth plus one `cloneFields` header slice per gathered entry, so a 10000-entry window pays ~10000 allocations before it writes a byte of reply. The fused arm is flat at 2 regardless of window, the reused reply buffer's own amortized growth, because it never gathers and never clones. This is the growslice + mallocgc + cloneFields the box profile charged at ~43% of on-CPU time, removed at the source.

ns per reply, lower is better:

| window | two-phase | fused | speedup |
|---|---|---|---|
| 1 | 1286 | 598 | 2.15x |
| 10 | 4012 | 1223 | 3.28x |
| 100 | 32259 | 18751 | 1.72x |
| 1000 | 320020 | 223811 | 1.43x |
| 10000 | 4020102 | 1776851 | 2.26x |

The fused arm builds the reply 1.4x to 3.3x faster across the sweep. The win is largest at the small windows where the per-entry clone and gather-slice growth dominate the fixed reply-encode work, and stays above 1.4x even at the 1000-entry window where the RESP-encode of the payload is the bigger share. The one header-shift `memmove` the fused arm adds is a single body-sized copy per reply, far under the per-entry clone-and-grow it replaces, so it never shows up against the arm it beats.

## Verdict (frozen)

Fuse the forward range read: one walk frames each entry into the reply as the walk yields it, `prependArrayHeader` inserts the count-first array header with one `memmove` once the walk finishes. No `[]rangeEntry`, no per-entry `cloneFields`. This removes the per-entry allocation floor the box profile charged at ~43% of on-CPU time and builds the reply 1.4x-3.3x faster in-process. The reverse read (XREVRANGE) keeps the two-phase gather, because it must buffer a block's entries to walk them backward, and it is the colder direction. The cross-check that makes this safe is the byte-for-byte reply equivalence `TestArmsAgree` holds across the window sweep. Shipped in `engine/f3/stream/range.go`; the box re-measure of M5-G4 is recorded in `results/f3`.
