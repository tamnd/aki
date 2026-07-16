# Lab 02: the segment-alignment pad

Part of issue #550, the M8 `.aki` single-file milestone, lab 02, the segment grid the spec lays out (doc 07 section 3). It is the per-perf-change lab the on-disk layout owes: the append writer rounds every segment up to a 4KiB boundary, and the per-perf-change rule wants the space-amplification claim measured, not asserted.

## Question

Every segment in the `.aki` file starts on a 4KiB boundary (akifile `SegmentAlign`), so a segment of `h+p` bytes (a 64-byte header plus its payload) rounds up to the next 4KiB, and the pad is wasted disk and page-cache. The alignment buys sector-whole, read-modify-write-free durable writes and a torn write that can never damage a neighbor. Two questions decide whether the pad is worth that:

- How much does the pad amplify a segment's on-disk footprint, and at what payload size does it fall to something negligible?
- What does the tightly packed rival that wastes no pad pay instead, and is that price worse than the pad?

## Model

The lab imports no engine package. It is pure arithmetic over doc 07 section 3's layout:

- a segment's on-disk span is `span = ceil((h+p)/A) * A` for alignment `A`, so the space amplification is `span/(h+p)` and the pad is `span-(h+p)`;
- an aligned span is always a whole number of 512-byte sectors (4096 and 16384 are both multiples of 512), so every durable write lands on whole sectors: zero read-modify-write, and a torn tail sector can only be the segment's own;
- the tightly packed rival wastes no pad but starts and ends mid-sector, so it pays a read-modify-write on the head and tail partial sectors it shares with its neighbors, and a torn write there corrupts a neighbor.

## Verdict

At the shipped layout (A=4096, header=64, sector=512):

- **A 64-byte value one per segment amplifies 32x** (a 3968-byte pad around 128 logical bytes). That is unaffordable, which is exactly why the hot path keeps values in the arena and the file never holds point values one to a segment.
- **The 64-byte header tips a round payload just past a boundary, so the pad is a near-constant ~4032 bytes, not a fixed fraction.** An 8KiB log window still costs 1.488x, a 32KiB cold chunk 1.123x, a 256KiB run 1.015x. So only tens-of-KiB batches amortize the pad well, and the amplification first crosses below 1.10x at a payload near 3712 bytes. The design implication is real: the file must write large batches, and the small transient log window pays a visible pad until it compacts away.
- **Sizing a segment to fill whole units erases the pad entirely.** A payload of `units*A - header` bytes leaves zero pad and a flat 1.0x, which is the refinement a power-of-two payload misses. The cold-chunk packers can lean on this when they choose a chunk size.
- The pad is bounded: a span wastes strictly less than one 4KiB unit, so the worst case is a single padded sector-run. The packed rival saves that pad but pays two partial-sector read-modify-writes per segment and risks a torn write bleeding into a neighbor, which the aligned grid rules out by construction.

The design's shape follows from the model: the file is a store of large batches, not point values, and the alignment that would be ruinous per-value is a small overhead per-batch that a tight chunk size can drive to zero. The pad is the price of never doing a read-modify-write and never letting a torn write cross a segment.

## Run

```
go run .            # full sweep, shipped alignment
go run . -quick     # short sweep
go run . -align 512 # the packed-ish low alignment, for contrast
```
