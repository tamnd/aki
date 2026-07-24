# ledger: the zset, list, and stream rows of the per-type ledger measured

## Question

Doc 08 section 9 is the table every capacity plan and gate row reads from, and the O2a ledger lab pinned its string, hash, and set cells on the landed engine.
The O2b slices landed the remaining planes: the zset dual projection, list position runs, stream entry runs, and the scan plan.
Do the remaining rows hold as landed: zset ZSCORE at 1 GET and ~0.6 resident B per element, ZRANK at 0 to 1 GETs of boundary block, ZRANGEBYSCORE at 1 plus the coalesced ceil, list LINDEX at 1 GET and ~0.3 B per element, stream XRANGE windows on dense runs at ~0.3 B per entry, and member misses definitive?

## Method

The O2a ledger stance end to end: a durability-booted server builds a 20000-member zset (scores a bijective permutation so every rank is arithmetic), a 20000-element RPUSH list, and a 20000-entry explicit-ID stream over real RESP, then string ballast pressures the resident cap until all three keys hold chunk placements in the published fold ledger, the coldness proof.
Cells then score through the rebuilt keymap, the directory, the real cold reader, and the run planners, the plane the serving node reads: ZSCORE through the kind-restricted member projection, ZRANK's boundary half through ZsetRankFloor plus one run block, ZRANGEBYSCORE spans through ScanRanges and the scan fetcher, LINDEX through the positional prefix sums, XRANGE windows through the ID-range floor, and a member-miss cell on strangers.
Because demotion is partial by design (ends and hot margins stay resident), one disclosed unbilled prep walk per type streams the folded projection once, pins it byte-for-byte against the built corpus, and gives the cells plan-relative references; its GETs are reported separately and never scored.
Resident share per type is the directory's 32 B per-chunk weight over the folded element count.

## Envelope disclosure

The table's miss row is key-level, answered by keymap absence at zero GETs with nothing to measure; the miss cell here is the stricter member-level question on a key that exists, where the discriminator resolve may honestly buy one block to say no.
ZRANK's other GET, fetching the member's own score first, is the ZSCORE cell; the zrank cell prices the boundary half the table row describes.
Commands are not in the loop below the residency line: command-level cold wiring for collection planes is the parked bucket fall-through follow-up, so the cells call the planners the way that wiring will.

## Prediction (PRED-OBS1-O2B-LEDGER, filed before the scored run)

1. zscore: exactly 1.0000 GETs per op with every returned score bit-exact against the build arithmetic, the table's 1-GET member-chunk row.
2. zrank boundary: exactly 1.0000 GETs per op with every plan-relative rank exact, at or under the table's 0-to-1 row, since distinct scores settle inside the floor run.
3. zrangebyscore: at or under the table's 1 plus ceil(run bytes over 16 MiB) per span, which for these ~500-element spans means at most 2 and an expected 1.0000 because ScanRanges coalesces the whole span into one range; every span returns exactly its elements.
4. lindex: exactly 1.0000 GETs per op with every value byte-exact against the built list, the table's positional-run row.
5. xrange: at most 1.5 GETs per 100-entry window on average, one dense-run block plus an occasional boundary crossing, with every window exactly its 100 arithmetic IDs and values.
6. zscore miss: at or under 1.0000 GETs per op with 100% of strangers definitively absent, the member-level bound the key-level 0-GET row implies.
7. Resident shares at or under the table: zset dual at or under 0.6 B per folded element, list at or under 0.3, stream at or under 0.3.

Kill line: any cell above its table value, any wrong answer or non-definitive miss, or cold reader errors or unresolved reads means the ledger does not describe the landed engine and the exit gate stops until it is understood.

## Calibration disclosure

The harness smoke (4000-element corpora, 200 samples) ran during development before this file was committed and confirmed the mechanics: all cells at 1.0000 GETs with 100% correct answers and shares 0.08, 0.25, and 0.25.
The bands above are the table's own values, not tuned to that smoke; the scored run below is a fresh full-size execution.

## Run

    ./run.sh

## Results

One scored run, full size, folded after 22 pressure rounds, prep walks billed 3 GETs (ledger.csv):

| cell | ops | GETs | GETs/op | correct |
|---|---|---|---|---|
| zscore | 2000 | 2000 | 1.0000 | 100% |
| zscore_miss | 100 | 100 | 1.0000 | 100% definitive |
| zrank_boundary | 2000 | 2000 | 1.0000 | 100% |
| zrangebyscore | 20 | 20 | 1.0000 | 100% |
| lindex | 2000 | 2000 | 1.0000 | 100% |
| xrange_window | 20 | 20 | 1.0000 | 100% |

Resident shares over the folded projections: zset dual 0.0708 B per element (8 chunks over 3616 folded members), list 0.2500 (24 over 3072), stream 0.2500 (24 over 3072).
Cold reader: 2100 fetches, 2100 block GETs, 100 misses (the miss cell), 0 unresolved, 0 errors.

Band scoring:

1. HIT: zscore at exactly 1.0000 GETs per op, all 2000 scores bit-exact against the build arithmetic.
2. HIT: zrank boundary at exactly 1.0000 GETs per op, all 2000 plan-relative ranks exact, inside the table's 0-to-1 row.
3. HIT: zrangebyscore at exactly 1.0000 GETs per span, under the 1-plus-ceil bound, every span exactly its elements; one disclosure, the spans came out near 90 elements rather than the filed ~500 because only 3616 members folded under this run's demotion pattern, which changes neither the bound nor the expected value since both are span-size-independent below 16 MiB.
4. HIT: lindex at exactly 1.0000 GETs per op, all 2000 values byte-exact against the built list.
5. HIT: xrange at exactly 1.0000 GETs per window, under the 1.5 band, every window exactly its 100 arithmetic IDs and values; no window happened to cross a block boundary.
6. HIT: member misses at exactly 1.0000 GETs per op, all 100 strangers definitively absent.
7. HIT: shares 0.0708, 0.2500, and 0.2500 against the table's 0.6, 0.3, and 0.3.

## Verdict

HIT on all seven bands, the kill line untouched.
The doc 08 section 9 rows for zset, list, and stream describe the landed engine: every point cell bills exactly one block GET with exact answers, spans and windows plan to the coalesced minimum, member misses are definitive at one block, and the resident surcharge sits at a quarter to a tenth of the table's allowance.
With the O2a lab's string, hash, and set cells, every row of the per-type ledger that claims a number is now measured on the landed plane.
