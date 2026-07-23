# cascade: the lightweight scheme ladder and the sampled selector

Milestone B4 lab 01 (spec 2064/sqlo1 doc 04 section 11, tracking issue #724).

## Question

B4 compresses gen-C extent groups through a per-group scheme choice: try the cheap encodings first, keep one only if it clears a minimum-win floor over raw, and fall through to zstd otherwise.
Before the slices bake constants, three things need pricing on Redis-shaped values rather than the columnar corpora the BtrBlocks numbers come from: which lightweight schemes actually earn a slot on the ladder, whether the 8 percent floor is doing any work, and whether a 1 percent sample of a group picks the same scheme the full group would.

## Method

Six generated shapes stand in for Redis value populations: counters (256 hot decimal counters), timestamps (near-monotonic unix-ms decimals), u64s (8-byte little-endian near-sorted), uuids (canonical 36-char), json (small templated bodies), and mixed (40/20/20/20 counters/timestamps/uuids/json interleaved).
Five codecs are real implementations, not arithmetic: raw framing, dict, dict+RLE, FOR bit-packing over 1024-value blocks (decimal-ascii and 8-byte-binary modes, with a strict applicability gate that refuses anything a re-format could corrupt), and klauspost zstd at SpeedDefault over the raw framing.
Round-trip tests pin every codec over every shape plus adversarial corpora, a fuzz sweeps the bit-packer across all widths and block boundaries, and the selection rule is tested to never return an inapplicable encoder.
The encoder arm prices each codec per shape (ratio, encode and decode ns per value, decode best of 5).
The select arm runs the doc 04 rule over fixed-size groups across a floor sweep and a group-size sweep.
The sample arm picks from a strided sample (max of rate x n and 8 values) and scores match rate and byte penalty against the full-group oracle.

## Run

    ./run.sh > cascade.csv    # encoder pricing x 6 shapes; floor sweep 0.00..0.24 x 6 shapes; gsize 64/256/1024/4096 on mixed; sample rates 0.01/0.05/0.20 x 6 shapes
    go test ./...             # round trips, forpack refusal table and fuzz, selection sanity, dict aliasing

CSV note: the four group-size rows print without a gsize column; in run order they are gsize 64, 256, 1024, 4096 (ratios 0.3806, 0.3226, 0.3013, 0.3005).
The two trailing sample rows are follow-up probes at rate 0.01 with gsize 1024 and 4096.

## Verdict (local, Apple Silicon, 2026-07-23)

FOR bit-packing owns every integer shape and earns its slot outright: timestamps 0.197 ratio at 26 ns/val encode against zstd's 0.328 at 79 ns, counters 0.242 against 0.339, u64s 0.355 against 0.307.
The u64s case is the one place zstd edges the ratio, and forpack still wins the slot there because it encodes 3x cheaper and decodes level; the slice should keep scheme 3 first for integer-shaped groups without a zstd tiebreak.
Dict pays only on counter-like low-cardinality groups (0.456 ratio) but it buys the cheapest decode of any scheme at 1.9 ns/val, so it stays on the ladder as the read-path option; dict+RLE never beat plain dict on any shape here and survives only as the doc 04 scheme 2 slot for sorted-run inputs this corpus does not model.
Zstd is the string-shape workhorse: json 0.187, mixed 0.298, uuids 0.548.
Uuids at 0.548 is the weakest zstd result in the sweep and the only measured candidate for the boxed FSST slice; nothing else here justifies it.

The 8 percent floor is non-binding on every shape measured: the floor sweep from 0.00 to 0.24 changes the selection on nothing, because a lightweight scheme either wins by 60 percent plus or is inapplicable.
Keep the spec's 8 percent; it costs nothing on real shapes and exists to refuse degenerate small wins the corpus cannot manufacture.

Selection windows want to be big: mixed at 64-value groups lands 0.381 total ratio against 0.300 at 4096, a 27 percent byte cost from splitting zstd's context and paying per-group headers.
The compaction slice should select per extent stream (thousands of values), not per small group.

Sampling accuracy is an absolute-count effect, not a rate effect: at rate 0.01 the picker matches the oracle 63.9 percent with a 44 percent byte penalty on 256-value groups, but 79.6 percent with a -0.3 percent penalty at 4096-value groups, where 1 percent is ~40 samples.
The homogeneous shapes match 100 percent at every rate; only mixed groups are hard, and there the penalty vanishes once the sample clears ~40 values because misses land on near-tied schemes.
So the spec's 1 percent sample rate stands, with a floor of ~40 absolute samples, applied to the extent stream rather than per tiny group, and the per-group 8 percent minimum-win recheck retained as the correctness backstop.
