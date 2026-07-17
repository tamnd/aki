# footerread: what a cold segment open costs, and how big footers really are

## Question

Doc 03 section 12 asks what opening a segment cold costs (tail GET, footer GET, first block GET) against footer size and object size, and whether the tail read merges with the footer read; the doc predicts a speculative read of the last 64 KiB hits above 95% for typical footers.
Doc 05 section 2.3 adds the takeover angle: footer reads for a whole group are one ranged GET per segment, so the speculative size decides whether that is one GET or two.
The open question the doc arithmetic does not settle is bloom placement: at 10 bits per key, a bloom over member keys on a 64 MiB segment of 200 B records is about 420 KiB by itself, while a bloom over chunk keys is under 4 KiB, so the lab runs both arms.

## Method

Segments are built through the real encoder (obs1.BuildSegment plus obs1.AppendSegment), not modeled, so footer bytes include every entry, bloom, and trailer byte as encoded; a pin test round-trips a built object through obs1.ParseSegment.
Cells sweep record size {200 B, 2 KiB} x nominal records per chunk {128, 512, 2048} x segment payload target {16, 64, 128 MiB} x bloom arm {chunk keys, member keys}, 8 segments per cell with per-chunk jitter (records per chunk uniform 0.5x to 1.5x nominal, key length 12 to 24 B).
The measured fact per segment is F, the bytes from the end of the last block to the end of the object, which is exactly what a speculative tail read must cover.
For each speculative size S in {16, 32, 64, 128, 256, 512 KiB}: hit rate P(F <= S), GETs per open (2 on hit, 3 on miss), dollars per million opens through sim.S3StandardPrices, wasted speculative bytes, and open latency p50/p99 from 20000 draws of sequential sim.S3Standard.Get lognormals (the lab reimplements the two-line quantile map against the sim constants and pins it, since the sim draw is unexported).

## PRED-OBS1-O1C-FOOTER (filed before the scored run)

1. Chunk-bloom footers at 64 MiB: rpc 512 and 2048 cells land under 45 KiB at both record sizes, so S = 64 KiB hits 100% there; the 200 B x 128 rpc cell lands at 100 to 140 KiB (about 2600 chunk entries plus 525 block entries) and needs S = 128 KiB.
2. The doc's 64 KiB above-95% claim HITS on the doc 09 typical shape (64 MiB, chunk-key bloom, a few hundred records per chunk and up) and MISSES on the full grid: 200 B x 128 rpc at 64 and 128 MiB and 200 B x 512 rpc at 128 MiB sit entirely above 64 KiB.
3. Member-key bloom at 200 B records is footer-dominant and unmergeable: F bands 110 to 160 KiB at 16 MiB, 380 to 480 KiB at 64 MiB, 750 to 950 KiB at 128 MiB; no S up to 256 KiB reaches the 64 MiB cells.
4. Open latency, 2 sequential GETs on hit: p50 42 to 56 ms, p99 170 to 260 ms; 3 GETs on miss: p50 64 to 84 ms, p99 200 to 320 ms.
5. Request dollars are noise either way: $0.80 per million opens on hit vs $1.20 on miss, and a 10 GiB group takeover at 64 MiB segments is about 160 footer GETs, $6.4e-5; the S decision is a latency and simplicity call, not a dollar call.
6. Verdict prediction: the tail read merges with the footer read at a default S = 128 KiB (covers every 64 MiB chunk-bloom cell, wastes at most ~125 KiB of free-in-dollars bytes), and the footer bloom stays over chunk keys; member existence belongs to the keymap, not the footer.

## Run

    ./run.sh            # full sweep, writes footerread.csv
    go run . -quick     # smoke

## Results

Full sweep in footerread.csv (36 cells, 8 segments each, 288 real segments through the encoder and footer decoder).
Footer size is essentially deterministic per shape: within every cell the max sits within about 2 KiB of the median, so hit rates are step functions of S, and 8 segments per cell was plenty.

Chunk-bloom F at 64 MiB payload: 200 B x 128 rpc 119.6 KiB, 200 B x 512 39.5 KiB, 200 B x 2048 10.1 KiB, 2 KiB x 128 15.9 KiB, 2 KiB x 512 4.0 KiB, 2 KiB x 2048 1.1 KiB.
At 128 MiB the small-chunk corner grows to 239 KiB (200 B x 128) and 79 KiB (200 B x 512); everything else stays under 32 KiB.
Member-bloom F at 200 B records: 106 to 132 KiB at 16 MiB, 421 to 526 KiB at 64 MiB, 840 to 1052 KiB at 128 MiB; at 2 KiB records 42 to 56 KiB at 64 MiB and 83 to 111 KiB at 128 MiB.

S = 64 KiB hits 100% on every 64 MiB chunk-bloom cell except 200 B x 128 rpc; S = 128 KiB hits 100% on every 64 MiB chunk-bloom cell and misses only the 128 MiB small-chunk corner.
Open latency: 46.3 to 47.8 ms p50 and 208 to 233 ms p99 on hit (2 GETs), 73.6 to 75.3 ms p50 and 272 to 287 ms p99 on miss (3 GETs); $0.80 vs $1.20 per million opens.

Scoring: predictions 1, 2, 4, 5, and 6 HIT (the chunk-arm bands, the 64 KiB claim splitting exactly on the three predicted cells, both latency bands, the dollars, and the verdict shape).
Prediction 3 MIXED: direction and the unreachability of the 64 MiB member-bloom cells held, but the 128 rpc cells broke the bands high (526 KiB at 64 MiB against a 480 ceiling, 1052 at 128 MiB against 950, and 106 at 16 MiB just under the 110 floor) because the chunk entries stack on top of the bloom more than the estimate carried.

## Verdict

The tail read merges with the footer read: default speculative tail S = 128 KiB, which covers every 64 MiB chunk-bloom shape with at most 128 KiB of free-in-dollars waste and turns the cold open into 2 sequential GETs (about 47 ms p50) instead of 3 (about 75 ms).
The doc's 64 KiB above-95% claim holds only for the doc-typical shape (64 MiB, a few hundred records per chunk and up); 128 KiB is the honest constant across the grid.
The footer bloom stays over chunk keys; a member-key bloom at 200 B records is 420 KiB and up at the 64 MiB target and cannot ride any sane tail read, so member existence belongs to the keymap (doc 05 section 2.1), not the footer.
Member blooms only fit the footer when records are large (2 KiB records stay under 56 KiB at 64 MiB), which is a shape-conditional the default should not depend on.
Since footer size is deterministic per built segment, the fold knows it exactly at publish time: carrying footer_off (8 bytes) in the manifest segment entry makes the common open 2 exact GETs with zero speculation risk, and the S = 128 KiB speculative read becomes the fallback for manifest-less discovery (takeover and recovery walking objects directly).
