# zsetdual: dual-projection write amplification and rank math

## Question

Doc 08 section 5 prices the sorted set as two projections of the same elements: member chunks keyed by the member hash serving ZSCORE, and score runs in IEEE754 total order serving range and rank.
The projection doubles the zset's cold bytes against a plain hash, and doc 09 carries that 2x weight; the claim it buys is rank flatness, ZRANK answered by resident prefix sums over per-run counts plus at most one boundary block.
This lab prices both sides before the zset slices land: how exact the 2x is, what ZSCORE and ZRANGEBYSCORE bill on the dual layout, what ZRANK costs with the score runs against what it costs without them, and whether a one-pass fold keeps the two projections consistent under churn.

## Method

Three decades, 10^5 to 10^7 members, deterministic scores with duplicates and negatives in the mix.
Both projections build with the real store chunk-frame codec at the baked 16 KiB target onto the counting sim, chunks never spanning 128 KiB blocks; the element packing inside the payload, the 24 B directory entry plus 2 B run count, and the rank arithmetic are lab-local models disclosed below, the typepoint lab's stance.
ZSCORE fetches the one block covering the member-hash chunk; ZRANK sums the resident run counts left of the boundary chunk and fetches that one block to settle ties by member bytes, redis order, checked against a fully sorted reference; the counter-arm zrank_scan answers the same query with no score runs by transferring the whole member projection in 16 MiB coalesced ranges.
ZRANGEBYSCORE plans lo to hi through the prefix sums, pays one boundary block GET, and transfers the rest of the covering span coalesced, the doc 08 1 + ceil row, with exact element counts checked.
The churn arm re-scores 10% of the members through an overlay, rebuilds both projections in one pass the way fold writes them, and cross-checks that the two carry the identical (member, score) multiset, the T-I3 shape.

## Envelope disclosure

The counting sim bills requests and bytes with no latency model; latency claims belong to PRED-OBS1-O2B-RANK on the landed plane.
One object per projection stands in for the one-segment envelope the O2a labs documented.
Rank ties inside one chunk resolve locally; a score so duplicated it spans chunks would add boundary blocks, and the corpus here keeps duplicates within chunk width, disclosed rather than tested.

## Prediction (PRED-OBS1-O2B-ZSETDUAL, filed before the scored run)

1. Write amplification is the exact projection doubling: dual bytes over member-only bytes within 1.98 to 2.02 at every decade, since both projections carry the same (member, score) payload and the same framing.
2. ZSCORE bills exactly 1.0000 GETs per op at 100% found at every decade, one block, 120-129 KiB per op.
3. ZRANK bills exactly 1.0000 GETs per op at every decade with every rank answer exact against the sorted reference: flat in cardinality, the row the 2x weight buys.
4. The counter-arm grows on the ceil identity: zrank_scan bills ceil(member projection bytes over 16 MiB) GETs, 1 at 10^5, 2 at 10^6, 17-19 at 10^7, with answers still exact.
5. ZRANGEBYSCORE bills 1 + ceil(covering span bytes over 16 MiB) GETs with exact element counts: 2.0000 for every span that clears the boundary chunk until the 25% span at 10^7 reaches 5-7.
6. The resident rank surcharge is small and flat: directory plus run counts for both projections at 0.08-0.10 B per element every decade.
7. The churned one-pass rebuild leaves the two projections carrying the identical (member, score) multiset at every decade, zero mismatches.

Kill line: any zrank op above 1.0 GETs or below 100% exact, amplification outside the 2x band, or a churned projection mismatch means the dual-projection model does not hold and the zset slices stop until it is understood.

## Calibration disclosure

Quick 5x10^4 passes executed during harness development and shaped the band widths for bytes per op and the amplification tolerance; the full-decade cells below run fresh.

## Run

    ./run.sh

## Results

Pending scored run.

## Verdict

Pending.
