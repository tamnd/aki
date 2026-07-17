# zstdworth: what compression buys and costs

Milestone O1c lab 02 (spec 2064/obs1 doc 03 sections 5 and 12, doc 09 section 3).

## Question

What do zstd levels 1/3 (level 9 as context) buy in storage dollars and cold-read bytes, against what they cost in fold CPU and decode time, at the 128 KiB segment-block unit and at WAL sections of 4 KiB to 1 MiB, on value corpora spanning the compressibility range?
This gates the comp defaults for segment blocks and WAL sections separately, and feeds CG2's storage line.

## Codec under test

The obs1 import boundary keeps codec modules out of the module graph, so the lab drives the system zstd CLI (the reference C implementation) in its in-memory benchmark mode, with -B cutting the input into independent blocks of exactly our unit size.
Speeds are therefore the C reference's; a Go-side codec at engine time will compress slower, while ratios carry because the format fixes them per level.

## Prediction (PRED-OBS1-O1C-ZSTD, lab-scoped, filed before the scored run)

Disclosure: the harness was debugged with -quick smoke sweeps (2 MiB corpora, one-second reps) before this prediction was frozen, and the smoke exposed the jsonsess segment cells and the jsonsess 4 and 64 KiB WAL cells, so those bands are smoke-calibrated; the other corpora, the 1 MiB WAL cells, and the 32/512 KiB context cells were not seen.

1. Segment blocks at 128 KiB, zstd-1 ratio by corpus: jsonsess 6 to 6.5x (smoke-calibrated), text2k 2.2 to 3.2x, numser 2 to 3x (near-random digits cap it at entropy coding), randbin at most 1.005x stored slightly expanded, which is the row the comp-0 fallback exists for, mixed 2.7 to 3.7x by the byte-weighted blend.
2. Levels beyond 1 buy nothing here: levels 2 and 3 land within 3% of level 1's ratio on every corpus, some cells inverted below it (short structured records favor the greedy match finder), and level 9 lands within 10% of level 1 at 8 to 12x the compress CPU. Level 1 strictly dominates for obs1's units.
3. Speeds on compressible corpora at the 128 KiB unit: zstd-1 compress at or above 900 MB/s and decompress at or above 2 GB/s single-thread, so the decode tax is at most 65 us per block, under 1% of a cold GET, and fold-side compression costs at most 1.2 vCPU-s per ingested GiB, around 0.1% of one core at doc 09 example A ingest.
4. WAL sections: ratio is monotone in section size; 4 KiB sections land at 45 to 65% of the same corpus's 128 KiB segment ratio and their compress speed drops about 3x on per-block startup; 1 MiB sections land within 15% of the segment ratio either side, the frame headers and the larger window pulling opposite ways.
5. The storage line: mixed at zstd-1 stores at 27 to 37% of raw, cutting $0.023 to between $0.006 and $0.009 per raw GB-month, and CG2's 1.35x garbage multiplier scales with stored bytes so the saving survives steady state.
6. Context cells: mixed ratio at 32 KiB blocks lands within 10% below the 128 KiB ratio, at 512 KiB within 5% above it, the window saturating beyond the record-repetition scale.

Expected verdict: segment blocks default comp 1 zstd-1, levels 3 and 9 declined since there is no ratio to buy on this data at 1.5 to 10x the CPU; WAL sections default comp 0 at tight flush cadences, since small sections give up half the ratio and WAL objects die at the fold, with the comp field earning its byte only if batching regularly produces sections of 64 KiB and up, to be revisited with real cadence data at O4.

## Method

Corpora, 32 MiB each, deterministic from seeded splitmix64: jsonsess (the doc 09 example A shape, ~200 B session objects whose field names repeat across values while ids do not), text2k (2 KiB english-like text from a fixed word list), numser (short numeric serials), randbin (200 B incompressible values), mixed (60/20/10/10 by bytes).
Segment cells benchmark the concatenated values at -B131072 (plus 32 and 512 KiB context cells on mixed); the real packer ends blocks at value boundaries while -B cuts at exact offsets, a sub-value difference per block that is immaterial to ratio at these units.
WAL cells wrap every value in a modeled frame, a 24 B header carrying sequence, kind, lengths, and a real CRC plus a 16 B random key, standing in at the same order as the frame-overhead lab's exact wire sizes, then benchmark at -B4096, -B65536, and -B1048576.
Levels 1, 2, 3 and 9 run per cell via zstd -b -q with two-second reps; level 0 is the identity baseline and is not run.
Dollars go through sim.S3StandardPrices.Bill so the O5 E-cloud refit moves this lab automatically; GET bytes saved is a latency and bandwidth story, not a dollar one, since S3 Standard prices GETs per request.

## Run

    ./run.sh            # full sweep into zstdworth.csv, needs zstd on PATH
    go run . -quick     # small corpora, smoke only

## Results

Pending the scored run.

## Verdict

Pending the scored run.
