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

Pending the scored run.

## Verdict

Pending the scored run.
