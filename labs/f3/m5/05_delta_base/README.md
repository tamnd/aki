# Lab 05: the ID delta base, base-delta vs successive-delta

Part of issue #547, the M5 stream milestone, lab 05, the ID delta base choice (doc 14 section 3.3). This is the lab the section 3.3 block codec rests on: it settles whether a sealed block should delta each entry's ID against the block firstID (base-delta, the shipped form) or against the entry before it (successive-delta), the decision doc 14 currently makes on the independent-decode argument alone. It is the encoding companion to labs 01, 02, and 04, which fixed the block geometry and the reclaim path; this one prices the ID field those blocks carry.

## Question

A sealed block stores its entries in the master-delta form of section 3.3: the first entry whole (the master, with field names), then same-schema entries as a flags byte plus the two ID deltas plus the value frames. The ID delta is two varints, the ms gap as an unsigned varint and the seq gap as a signed zigzag varint (a millisecond rollover resets seq to zero, so the gap can go negative). The open question is what the gap is measured against.

The shipped codec measures it against the block firstID: base-delta. Doc 14 section 3.3 justifies this by independent decodability, "every entry is independently decodable, a mid-block seek never decodes the entries before it." The alternative, the form a plain delta stream takes, measures each gap against the immediate predecessor: successive-delta. For a strictly increasing ID run the successive gap is never wider than the base gap and is usually far smaller, so successive-delta should pack tighter; the cost is that an entry decodes only after the one before it, the property section 3.3 wanted to keep.

So the questions: how many ID bytes does each base cost across the ID shapes a real stream produces, does successive-delta's smaller varints actually cost anything to decode, and is the independent-decode property base-delta buys worth the bytes it spends, given the block already re-masters every 128 entries.

The ms field settles structurally. The base gap `id.ms - first.ms` is never smaller than the successive gap `id.ms - prev.ms`, because `first.ms <= prev.ms`, and an unsigned varint's length never shrinks as its value grows, so successive-delta's ms bytes are provably at most base-delta's, entry for entry. The seq field has no such clean bound (a rollover can make either gap the larger zigzag), so the seq side is what the sweep has to measure.

## Method

In-process, no server, no wire, no engine import, the same lab-local model labs 01, 02, and 04 use. The two encoders mirror `engine/f3/stream/id.go` exactly, `binary.AppendUvarint` for the ms gap and `binary.AppendVarint` for the zigzag seq gap, and `TestEncodersMatchStdlib` pins the lab's byte pricing to those standard-library writers so the columns cannot drift from the real codec. Only the ID bytes differ between the two bases, the flags byte and the value frames are identical, so the lab prices the ID field alone, which is the whole of the difference.

Six ID shapes span the range a stream produces: `dense-1000/ms` is the benchmark-shaped auto-ID burst (1000 entries per millisecond, a full block is one millisecond of seq 0..127), `burst-100/ms` and `one/ms` are lighter auto-ID producers, `slow-10ms` is a 100 Hz producer whose block spans over a second, `sparse-idle` is bursty traffic with random idle gaps, and `explicit-wide` is user-supplied IDs seconds apart. Each generator fills one full 128-entry block deterministically from a fixed seed. The size columns are exact byte counts; the decode columns build the real byte streams and walk them, reconstructing every ID, so the accumulation cost of successive-delta is charged in full.

`go run .` runs the whole sweep. `-quick` shrinks the decode rep count for the shared runner. `TestEncodersMatchStdlib`, `TestRoundTrip`, and `TestSuccessiveNeverLarger` are what CI drives; `TestRoundTrip` decodes each entry at its own offset under base-delta (exercising the independent-decode property the spec cites) and in order under successive-delta.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process. 4096/128 block geometry, one full 128-entry block per shape. The byte columns are exact and reproduce on any box, this is deterministic varint math, not a throughput measurement; the nanosecond columns are single-box and timing-noise sensitive, read them as the shape.

ID bytes per full block, base-delta vs successive-delta:

| pattern | baseB | succB | base/e | succ/e | save/e | save% |
|---|---|---|---|---|---|---|
| dense-1000/ms | 320 | 256 | 2.500 | 2.000 | 0.500 | 20.0% |
| burst-100/ms | 292 | 257 | 2.281 | 2.008 | 0.273 | 12.0% |
| one/ms | 256 | 256 | 2.000 | 2.000 | 0.000 | 0.0% |
| slow-10ms | 371 | 256 | 2.898 | 2.000 | 0.898 | 31.0% |
| sparse-idle | 501 | 346 | 3.914 | 2.703 | 1.211 | 30.9% |
| explicit-wide | 565 | 472 | 4.414 | 3.688 | 0.727 | 16.5% |

Successive-delta is never larger and usually smaller. On the benchmark-shaped `dense-1000/ms` case it saves 0.5 B/entry (a base gap to firstID grows to seq 127, a two-byte zigzag, while the successive gap is always +1, one byte); the win widens as the block spans more time, to ~0.9 B/entry at `slow-10ms` and ~1.2 B/entry on `sparse-idle`, where base gaps against a distant firstID run to three and four bytes. The one tie is `one/ms`: 128 consecutive milliseconds keep every base gap under 128, a single byte either way. There is no pattern where base-delta wins a byte, exactly as the ms-field bound predicts, and the seq field never reverses it across these shapes (`TestSuccessiveNeverLarger`).

Full-block decode, 200000 reps:

| pattern | baseNs | succNs | succ/base |
|---|---|---|---|
| dense-1000/ms | 316.8 | 287.9 | 0.909 |
| burst-100/ms | 334.5 | 321.7 | 0.962 |
| one/ms | 338.1 | 320.1 | 0.947 |
| slow-10ms | 436.8 | 313.7 | 0.718 |
| sparse-idle | 415.5 | 356.6 | 0.858 |
| explicit-wide | 509.9 | 432.4 | 0.848 |

Successive-delta is no slower to decode, and at high rep counts marginally faster: both bases walk the same 128 varint pairs, but successive-delta's are fewer bytes, so the buffer is smaller and the Uvarint reads shorter. The margin is small and noisy (a `-quick` run puts a couple of shapes a hair over 1.0), so the honest read is decode parity, not a decode win. The point is the negative: successive-delta's smaller footprint costs nothing on the read path.

## Verdict

The lab contradicts the shipped choice. Successive-delta packs the stream ID strictly tighter, 0.5 B/entry on the benchmark-shaped dense case and up to 1.2 B/entry on time-spread blocks, and decodes no slower. The only thing base-delta buys over it is per-entry independent decodability, and that property does not pay for the bytes it spends, because the block already re-masters every 128 entries: the block is the seek unit, not the entry. A mid-block lookup already walks from the block's own master (`walk` decodes from pos=0), and the most that independent per-entry decode could save inside one block is a sub-microsecond 128-entry walk, the very cost the decode column shows is already at parity. Spending 0.5 to 1.2 B/entry to protect an intra-block seek the codec never performs is the wrong trade against a hard memory bar whose whole pitch is less memory for the same data. So:

- The recommendation is successive-delta: delta each same-schema entry's ID against the immediate predecessor, not the block firstID. The ms gap is provably never wider and the seq gap holds smaller across every representative shape, so the change only ever removes bytes, and it removes the most on exactly the blocks that span the most time, where base gaps against a distant firstID are widest.
- The independent-decode rationale in doc 14 section 3.3 does not survive the 128-entry re-master. Section 3.3 keeps per-entry independence to make a mid-block seek cheap, but the block cap already bounds any seek to a single master's worth of entries, and every block op walks from the master anyway, so the property is latent and unused. The successive form keeps the tombstone story unchanged: a deleted entry keeps its ID bytes as the chain link and the section 6.5 gc rewrite (lab 04) re-encodes the whole block regardless, so nothing on the delete path relies on per-entry independence either.
- The byte win is modest on the headline dense case (0.5 B/entry, ~1.6% of a ~31 B/entry block) and larger on sparse and time-spread streams, and it is free on the read path. It is a real contribution to the memory bar, not a throughput lever, so it lands as its own correctness-tested codec slice with a doc 14 section 3.3 rewrite, not folded into a perf change.

This closes the section 3.3 lab. The follow-up slice switches `putIDDelta`/`readIDDelta` and the block `walk` to a predecessor base, re-encodes the seq gap against the running ID instead of the firstID, and rewrites the doc 14 section 3.3 rationale from independent decodability to smaller-gap packing; the block header keeps firstID for the directory and `covers`, which are block-granular and unaffected.
