# flat: cold field latency vs cardinality on the landed read plane

## Question

PRED-OBS1-O2A-FLAT claims cold HGET p99 stays flat from 10^3 to 10^8 fields at a fixed cache budget.
The bigcollection lab proved the request bill flat on a modeled directory; this lab re-asks the question on the landed plane with latency in the answer: real ChunkPacker chunks in a real segment built by BuildSegment and AppendSegment, resolved through a real Directory and Keymap, fetched by the real ColdReader against the sim drawing the doc 01 S3 Standard envelope per GET.

## Method

Each decade builds one collection into one segment on a fresh sim: hash decades with 64 B values to 10^7, the 10^8 decade as the valueless set shape, which doc 08 defines as a hash with empty values and identical planning.
Elements generate from a sorted (disc, index) table so the big decades never hold per-element blobs, the bigcollection lab's trick.
The footer is read back off the built object the way boot recovery reads it, the directory and keymap are populated from it, and 1000 scored fetches per decade run serially through ColdReader.FetchField with per-fetch wall time recorded.
Serial on purpose: concurrent fetches into a small collection coalesce on the single-flight table, which prices load shapes, not the per-op claim; every op here pays its own draw.
The resolver walk, the one term that grows with cardinality, is also timed alone against the resident directory with no sim in the path.
Fixed cache budget means the resident plan only, keymap plus directory bytes reported per decade; there is no block cache yet (doc 05 section 4 is owed), so every fetch pays its GET, which is the claim under test.

## Envelope disclosure

One segment per collection is the lab's envelope: ResolveField plans within the segment the keymap locator pins, and BuildSegment has no size ceiling, so the whole collection sits in a single object the way a doc 06 rewrite pass would leave it; cross-segment field shadowing belongs to that rewrite.
The sim's latency model is a per-request lognormal draw with no bandwidth term, so first-byte-to-last-byte transfer is not in these numbers; the #1113 cold-latency lab carried the link assumption and the O5 E-cloud refit replaces both.
Chunks are cut at the baked 16 KiB target (#1313).

## Prediction (PRED-OBS1-O2A-FLAT, filed before the scored run)

1. Every decade costs exactly 1.0000 GETs per fetch at 100% found, 10^3 through 10^8.
2. p50 per decade lands within 25% of the envelope's 20 ms GET p50 at every cardinality, no trend with n.
3. p99 is flat across decades: the max over min p99 ratio across all six decades stays at or under 1.6, which is the sampling spread of a lognormal p99 estimated from 1000 draws, and the 10^8 decade is not systematically the slowest.
4. The resolver walk is the one cardinality-dependent term and it stays harmless: mean ResolveField time grows roughly linearly in chunk count and stays under 2 ms at 10^8, two orders under the GET p50.
5. Bytes per fetch are one block at every decade: 110-132 KiB once the object exceeds one block, with the 10^3 decade clipping to its smaller object.
6. The cold reader finishes clean at every decade: zero errors, zero unresolved, zero misses; the keymap holds exactly one entry per collection, and the directory's bytes per element fall with cardinality to 0.05 B or less at 10^8.

Kill line: cold field p99 rising with cardinality past 1.6x from 10^3 to 10^8, or any decade above 1.0 GETs per fetch, means cold HGET is not flat on the landed plane and the O2a exit gate stops until it is understood.

## Calibration disclosure

Quick 10^3 to 10^5 passes with the latency model off executed during harness development, plus one full six-decade pass while confirming the 10^8 build fits local RAM and the run fits its wall clock; the bands were derived from the envelope arithmetic and the #1113 sampling spread beforehand and stand unmodified, and the scored run below is a fresh execution.

## Run

    ./run.sh

## Results

Scored run (flat.csv), 1000 serial fetches per decade:

| kind | n | chunks | obj MiB | GETs/op | KiB/op | found | resolve us | p50 ms | p99 ms | dir B/elem |
|---|---|---|---|---|---|---|---|---|---|---|
| hash | 10^3 | 5 | 0.1 | 1.0000 | 75.3 | 100% | 0.1 | 22.2 | 141.2 | 0.2570 |
| hash | 10^4 | 47 | 0.7 | 1.0000 | 108.8 | 100% | 0.3 | 20.6 | 129.3 | 0.1721 |
| hash | 10^5 | 470 | 7.4 | 1.0000 | 112.4 | 100% | 0.6 | 22.0 | 133.8 | 0.1646 |
| hash | 10^6 | 4695 | 73.8 | 1.0000 | 112.3 | 100% | 6.4 | 22.4 | 150.5 | 0.1637 |
| hash | 10^7 | 46949 | 737.5 | 1.0000 | 112.3 | 100% | 56.0 | 21.7 | 156.5 | 0.1637 |
| set | 10^8 | 79303 | 1245.2 | 1.0000 | 112.2 | 100% | 94.2 | 21.3 | 137.4 | 0.0276 |

p99 max over min ratio: 1.21.
Cold reader: 6000 fetches, 6000 block GETs, 0 attached, 0 unresolved, 0 misses, 0 errors.

## Verdict

Hit, with one marginal byte-band edge.

Band 1 exact: 1.0000 GETs per fetch at 100% found at every decade.
Bands 2 and 3, the flatness claim itself: p50 sits at 20.6-22.6 ms across five orders of magnitude of cardinality, within 13% of the envelope's 20 ms, and the p99 ratio is 1.21 against the 1.6 line with the 10^8 decade's 137.4 ms nowhere near the slowest.
Band 4: the resolver walk grows linearly in chunk count as predicted (0.1 us at 5 chunks to 94.2 us at 79303) and tops out two and a half orders under the GET p50, far inside the 2 ms line; the linear scan is affordable at 10^8, though it is the number a doc 06 rewrite of ResolveField would watch.
Band 5 edge: the 10^4 decade reads 108.8 KiB per fetch, 1% under the stated 110 KiB floor, because chunk-boundary block closes leave slightly shorter blocks in a 47-chunk object; every larger decade sits at 112.2-112.4 KiB inside the band and the 10^3 decade clips to its 0.1 MiB object as predicted.
Band 6: cold reader clean everywhere, and the directory's share falls to 0.0276 B per element at 10^8, under the 0.05 line.

The kill line did not fire: cold field latency does not rise with cardinality on the landed plane, at any decade, in either quantile.
