# rank: cold ZRANK latency measured on the landed plane

## Question

The zsetdual lab priced rank flatness on a disclosed model before the zset slices landed: resident prefix sums plus one boundary block, the row the dual projection's 2x byte weight buys.
This lab measures what PRED-OBS1-O2B-RANK claims on the landed plane: cold ZRANK p99 flat from 10^3 to 10^8 members at a fixed cache budget, with the doc 01 S3 Standard latency envelope drawn per GET.

## Method

Per decade, both projections of one zset build with the real chunk codec at the baked 16 KiB target into one real segment via BuildSegment and AppendSegment, the footer read back off the built object the way boot recovery reads it.
Scores are a multiplicative permutation of 0 to n-1, so member i's exact rank is (i times A) mod n by construction and the score runs generate directly in rank order through the modular inverse; no sorted copy of the collection is ever held, the bigcollection trick reshaped for ranks.
A full cold ZRANK holds only the member, so each scored op pays the whole landed path serially: the score off the member projection through Keymap, kind-restricted ResolveField, and the real ColdReader, then ZsetRankFloor over the kind-restricted CollChunksKind refs and one ZsetRunIter boundary block through GetRange plus ParseSegmentBlock, with the answer checked against the arithmetic rank.
1000 serial ops per decade, serial on purpose: concurrent fetches coalesce on the single-flight table, which prices load shapes, not the per-op claim.
The rank floor walk, the one resident term that grows with cardinality, is also timed alone with no sim in the path.
Fixed cache budget means the resident plan only, keymap plus directory, reported per decade; there is no block cache (doc 05 section 4 is owed), so every op pays its GETs.

## Envelope disclosure

One segment is the lab's envelope, the state a doc 06 rewrite pass leaves, the same stance as the O2a flat lab.
Scores are distinct by construction, so every rank settles inside its floor run; a score so duplicated it spans runs would add boundary blocks, disclosed in the zsetdual lab and unchanged here.
The sim draws latency per request and has no bandwidth term; the O5 refit owns the envelope constants.

## Prediction (PRED-OBS1-O2B-RANK, filed before the scored run)

1. ZRANK bills exactly 2.0000 GETs per op at every decade with every rank exact against the arithmetic reference: one member-projection block for the score, one boundary score-run block for the position, flat in cardinality.
2. The flatness claim proper: p99 max over min across all six decades lands at or under 1.6, the flat lab's band, because the bill per op is two draws from the same envelope at every cardinality.
3. p50 lands between 38 and 52 ms at every decade, two serial S3 Standard GETs at the envelope's 19 to 26 ms center.
4. Bytes per op stay at or under 260 KiB everywhere, two 128 KiB blocks at most, with the 10^6 and larger decades between 240 and 260 KiB and the small decades clipping to their object size.
5. The floor walk grows linearly with the run count but stays at or under 1 ms per op even at 10^8, two orders under the GET p50, which is what keeps it out of the latency story.
6. The resident surcharge is flat: directory share between 0.06 and 0.12 B per element at every decade, the dual projection's two 32 B rows per packed chunk.

Kill line: any decade above 2.0 GETs per op, any rank answer wrong, or a p99 ratio above 2 means rank flatness does not hold on the landed plane and the exit gate stops until it is understood.

## Calibration disclosure

The harness smoke (10^3 to 10^5, 50 ops, no latency model) ran during development before this file was committed and confirmed the 2.0000 GETs and 100% exact mechanics; the bands above were derived from the envelope arithmetic and the flat lab's measured centers, and the scored run below is a fresh execution.

## Run

    ./run.sh

## Results

One scored run, six decades, 1000 serial ops each, S3 Standard envelope per GET (rank.csv):

| n | chunks | obj MiB | GETs/op | KiB/op | rank exact | floor us | p50 ms | p99 ms | dir B | dir B/elem |
|---|---|---|---|---|---|---|---|---|---|---|
| 10^3 | 4 | 0.0 | 2.0000 | 82.3 | 100% | 0.0 | 49.4 | 216.1 | 225 | 0.2250 |
| 10^4 | 26 | 0.4 | 2.0000 | 217.3 | 100% | 0.0 | 49.7 | 211.6 | 989 | 0.0989 |
| 10^5 | 258 | 4.0 | 2.0000 | 223.1 | 100% | 0.1 | 48.4 | 233.5 | 9073 | 0.0907 |
| 10^6 | 2562 | 40.2 | 2.0000 | 224.6 | 100% | 0.4 | 51.6 | 233.4 | 89381 | 0.0894 |
| 10^7 | 25610 | 402.4 | 2.0000 | 224.7 | 100% | 4.7 | 50.4 | 246.2 | 892777 | 0.0893 |
| 10^8 | 256082 | 4024.1 | 2.0000 | 224.7 | 100% | 48.2 | 52.6 | 241.6 | 8926361 | 0.0893 |

p99 max over min across the six decades: 1.16.
Cold reader closed clean: 6000 fetches, 6000 block GETs, 0 misses, 0 unresolved, 0 errors.

Band scoring:

1. HIT: exactly 2.0000 GETs per op at every decade and every one of the 6000 ranks exact against the arithmetic reference.
2. HIT: p99 ratio 1.16, well inside 1.6; p99 spans 211.6 to 246.2 ms from 10^3 to 10^8 members, which is the flatness claim measured.
3. MISS, narrow: five decades inside 38 to 52 ms but 10^8 lands at 52.6 ms, 0.6 ms over the filed edge; the miss is quantile noise on 1000 draws, not a mechanism, since 10^6 sits at 51.6 with a fortieth of the data.
4. PARTIAL: the 260 KiB cap holds everywhere and 10^3 clips as called, but the 10^6-plus decades sit at 224.6 to 224.7 KiB, under the filed 240 to 260 center; the two-full-128-KiB assumption was wrong because packed score-run and member blocks close at about 112 KiB at the 16 KiB chunk target.
5. HIT: the floor walk grows one order per decade as predicted and tops out at 48.2 us at 10^8, twenty times under the filed 1 ms bar and three orders under the GET p50.
6. PARTIAL: 0.0893 to 0.0989 B per element from 10^4 up, inside the band and converging on 0.0893, but 10^3 lands at 0.2250 because four chunks cannot amortize the per-chunk rows over a thousand elements; the band should have excluded the degenerate decade.

## Verdict

HIT on the claim, with three envelope numbers set wrong and disclosed above.
Cold ZRANK on the landed plane bills exactly two GETs and answers exactly at every cardinality from 10^3 to 10^8, p99 varies 1.16x across five orders of magnitude at a fixed resident budget, and the one term that grows, the floor walk, is microseconds against a 50 ms op.
The row PRED-OBS1-O2B-RANK claims is measured and holds; the misses are a 0.6 ms quantile graze at 10^8, a block-size center filed 7% high, and a directory band that forgot small collections do not amortize.
