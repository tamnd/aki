# blocksize: the 128 KiB compression block on trial

Milestone O1c lab 01 (spec 2064/obs1 doc 03 sections 5 and 12, doc 05 sections 3-4).

## Question

What does the segment block size 32/64/128/256/512 KiB do to the point-read GET bill through a byte-budgeted block cache, and to scan request count and fetch waste, across uniform, raw Zipfian, and hot-tier-absorbed Zipfian reads?
This confirms or moves the 128 KiB default before the fold slice bakes it into the packer.

## Prediction (PRED-OBS1-O1C-BLOCK, lab-scoped, filed before the scored run)

Reasoning: a point miss buys one GET regardless of block size, so the dollar column is the miss rate times the GET price and the whole game is the hit rate; the cache holds a fixed byte fraction, so the cached block count falls linearly in block size, and under a Zipfian head the captured traffic follows the truncated-zeta mass of roughly that many hot blocks, which moves logarithmically.

1. Uniform rows: hit rate equals the budget fraction at every block size, within a point; $/M cold ops about $0.39 at 2% and $0.36 at 10%. Block size is invisible to the bill here.
2. Raw Zipfian (theta 0.99, no hot tier), 200 B values, 2% budget: hit 45 to 60% at 32 KiB, falling with block size by the log law: 128 KiB lands 8 to 12 points under 32 KiB, 512 KiB lands 18 to 25 points under it. At 10% budget the same shape shifted up, 55 to 65% at 32 KiB.
3. Tail-Zipfian (hot tier absorbs the top 10% of ranks, the doc 05 serving shape): the head that made block size matter is gone; hit lands within 5 points of the uniform floor at every block size, and the spread across block sizes stays under 5 points. This is the row the deployment lives on.
4. The 2 KiB corpus tracks the 200 B corpus within about 8 points at equal budget and block size, because the cached block count, not the keys-per-block count, is what sets the captured mass.
5. Scans: requests per scan equal the fragment count at every block size (16 MiB coalescing swallows every candidate size), and the fetch ratio follows 1 + blockBytes/fragmentBytes: a fragmented 64 KiB scan (4 fragments) reads about 3x its span at 32 KiB and about 33x at 512 KiB; a 1 MiB scan in 4 fragments reads 1.5x at 128 KiB and 3x at 512 KiB.
6. Stated arithmetic, not measured: the 64 MiB segment's block index is 20 B per block, so 32 KiB blocks cost a 40 KiB index line against 10 KiB at 128 KiB and 2.5 KiB at 512 KiB, plus fourfold directory entries.

Expected verdict: 128 KiB stands. The deployment row (tail-Zipfian) is block-size-flat, so shrinking blocks buys real hit rate only in the no-hot-tier bracket; growing them past 128 KiB pays scan waste linearly and raw-Zipfian hit logarithmically, with no request-count win anywhere.

Disclosure: the model was debugged with -quick smoke sweeps (64x smaller keyspaces) before this prediction was frozen; the smoke showed the qualitative shape (uniform flat, raw Zipf falling, tail-Zipf at the floor) but its absolute hit rates are not the scored scale. The scored run is the full sweep below.

## Method

Pure cache model, no store: keys of one fixed value size lie contiguous in fold order (popularity scattered by a bijective scramble, so fold order is independent of rank), chunks never span blocks, a point miss fetches exactly one block, and dollars go through sim.S3StandardPrices.Bill so the O5 E-cloud refit moves this lab automatically.
The cache is plain SIEVE over whole blocks with a byte budget, standing in for the doc 05 two-touch doorkeeper (that admission policy lands with the async-cold-read slice; scans bypass the cache here by construction, so the pollution it guards against is out of frame); blocks are counted at raw size because compression is the zstd-worth lab's business; the hot tier is idealized as the exact top ranks.
Corpora: 2^24 keys at 200 B (3.4 GiB cold, the doc 09 example A value shape) and 2^21 keys at 2 KiB (4.3 GiB cold); budgets 2% and 10% of cold bytes; Zipfian theta 0.99 by the YCSB draw; 2M warm plus 8M measured ops per point cell; 200k sampled scans per scan cell.

## Run

    ./run.sh            # full sweep into blocksize.csv
    go run . -quick     # tiny keyspaces, smoke only

## Results

Full sweep in blocksize.csv, run 2026-07-17, 2M warm plus 8M measured ops per point cell.
The rows the gate hangs on, hit percent by block size 32/64/128/256/512 KiB:

| cell | 32 | 64 | 128 | 256 | 512 |
|------|----|----|-----|-----|-----|
| uniform, any corpus, 2% | 2.0 | 2.0 | 2.0 | 2.0 | 2.0 |
| raw zipf, 200 B, 2% | 45.6 | 42.5 | 38.7 | 34.5 | 30.4 |
| raw zipf, 200 B, 10% | 56.8 | 54.0 | 51.1 | 47.8 | 44.2 |
| raw zipf, 2 KiB, 2% | 54.0 | 49.9 | 45.4 | 41.1 | 36.6 |
| tail zipf, 200 B, 2% | 2.0 | 2.0 | 2.0 | 2.0 | 2.0 |
| tail zipf, 2 KiB, 10% | 11.0 | 10.6 | 10.4 | 10.2 | 10.1 |

The raw-Zipfian fall is close to linear in the log of the block size, 3 to 4 points per doubling in every raw row.
Scans landed on the closed form exactly: requests per scan equal the fragment count in all twenty cells (coalescing swallows every candidate size), and the fetch ratio is 1 + blockBytes/fragmentBytes to three digits, so a fragmented 64 KiB scan reads 9x its span at 128 KiB and 33x at 512 KiB, while a 1 MiB scan in 4 fragments reads 1.5x at 128 KiB.

## Verdict

PRED-OBS1-O1C-BLOCK scores HIT on substance with two disclosed band details.
Uniform rows sat on the budget fraction at every block size with the bill at $0.392 and $0.360 per million cold ops; the tail-Zipfian rows, the deployment shape, collapsed onto that same floor with the spread across block sizes under one point, which is the load-bearing finding: once the hot tier absorbs the head, the block cache's hit rate stops caring about block size and the GET bill is set by the budget fraction alone.
The band details: the raw-Zipfian fall-off came in shallower than predicted (128 KiB lands 6.9 points under 32 KiB against the predicted 8 to 12, 512 KiB 15.2 under against 18 to 25), an error on the side that favors the default; and the 2 KiB corpus runs 8 to 9 points above the 200 B corpus at equal budget, grazing the 8-point band, because the smaller keyspace concentrates more mass in the head, so the cached-block-count claim holds directionally but corpus normalization moves the level.
Gate outcome: the 128 KiB default STANDS, and the fold slice bakes it.
Below 128 the win is 3 to 7 raw-Zipfian points that vanish in the deployment row, bought with a fourfold block index and directory line; above 128 the scan fetch ratio doubles per step on fragmented reads and the raw-Zipfian rows give up 4 points per doubling with no request-count win anywhere.
