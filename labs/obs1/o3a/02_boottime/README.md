# boottime: cold boot to serving priced, the fleet checkpoint cadence baked

## Question

Doc 02 section 2.5 states the checkpoint cadence in records and seconds, but a booting node replays the chain one object GET at a time, serially by construction, so the constant that decides boot time is how many chain objects a checkpoint may trail the tail by.
What does the landed boot path cost per trailing chain object and per manifest, and what does a whole multi-group estate cost to boot to serving, so the lease-manager and warm-restart slices bake real numbers?

## Method

Every term runs on the real primitive against the sim's doc 01 S3 Standard envelope: BootChain (root GET, checkpoint GET), LeaseFold.Prime plus ChainAppender.Follow for the replay term, LoadManifests for the GET-next discovery walk, RebuildResident per group behind the group fan, and the group WAL-tail object GET plus parse.
The estate is real end to end: CreateRoot, a seeded chain of grant records folded through a Checkpointer-wrapped LeaseFold, real WriteCheckpoint calls that write the object, append the 0x06 record, and advance the root, heartbeat batches pushing the tail out to each sweep distance, and per-group segments, manifests, and WAL objects built the folder's way.
Every measured cell carries a hard op-count assertion from the sim's request counters: chain boot must bill exactly distance plus 4 GETs, discovery exactly count plus 1, and the composed boot exactly its estate arithmetic, so a stray or missing request is an error, not a footnote.
Every booted fold's StateSum must equal the builder fold's at the same tail, the C-I2 comparison, which holds because the checkpoint's own 0x06 batch lands after its Through position and therefore always replays.

## Envelope disclosure

The sim draws per-request latency with no bandwidth term; segment and WAL byte weights ride the doc 01 fit arithmetic from the handoff lab and refit at O5.
The rebuild-vs-segment-count and fetch-fan story is 01_handofftime's; this lab holds groups at 4 small segments each and prices the walk shape, not the segment bytes.
Boot quantiles are medians and worsts over small rep counts at the arm sizes wall clock allows; the linearity claims lean on the exact op counts, which are noise-free.

## Prediction (PRED-OBS1-O3A-BOOTTIME, filed before the scored run)

1. Fixed floor: a zero-distance chain boot is exactly 4 GETs (root, checkpoint, the 0x06 batch, the 404) with p50 under 300ms.
2. Replay is linear in checkpoint distance at 20 to 40ms per trailing chain object, billed at exactly distance plus 4 GETs; distance 64 stays under 3s at p50 and distance 256 lands over 5s, past even the takeover bar.
3. The cadence constant this bakes: fleet checkpoints must land at least every 64 chain objects, and the lease manager should target well under that so the replay term stays a fraction of the graceful bars.
4. Manifest discovery is linear at exactly count plus 1 GETs; 16 manifests walk under 1s at p50, and a group that compacts its manifest chain boots its discovery in 2 GETs.
5. The composed 32-group boot to serving at distance 16 with a group fan of 8 bills exactly 244 GETs (20 chain plus 7 per group) and lands under 3s at p50, under the 5s takeover bar with margin.
6. Correctness throughout: every booted fold's StateSum equals the builder's, rebuilt stats exact per group, WAL frame counts exact.

Kill line: a zero-distance boot at or over 300ms p50, any op count off its arithmetic, a composed boot at or over 5s, or any StateSum or stats divergence means the boot path or the cadence plan is wrong and the lease-manager slice does not start until it is understood.

## Calibration disclosure

A quick smoke (distances 0 and 4, 4 groups at fan 2, 2 reps) ran during development before this file was committed and confirmed the mechanics: op counts exact at 4, 8, 2, 5, and 40 GETs, StateSum agreement on every boot, composed quick boot near 1.1s.
The bands above come from the doc 01 envelope arithmetic and the estate op counts, not from tuning to that smoke; the scored run below is a fresh full-size execution.

## Run

    ./run.sh

## Results

One scored run, full estate (boot.csv), every op-count and StateSum assertion green:

| term | arm | p50 ms | worst ms | GETs |
|---|---|---|---|---|
| chain boot | distance 0 | 85 | 217 | 4 |
| chain boot | distance 16 | 532 | 558 | 20 |
| chain boot | distance 64 | 2015 | 2354 | 68 |
| chain boot | distance 256 | 7937 | 7937 | 260 |
| manifests | 1 | 39 | 55 | 2 |
| manifests | 4 | 121 | 197 | 5 |
| manifests | 16 | 469 | 523 | 17 |
| composed boot | 32 groups, fan 8, distance 16 | 1424 | 1868 | 244 |

Band scoring:

1. HIT: zero-distance boot at exactly 4 GETs, p50 85ms against the 300ms bar.
2. HIT: replay linear at 27.9 to 30.7ms per trailing chain object across the three distances, every arm at exactly distance plus 4 GETs; distance 64 at 2015ms under 3s, distance 256 at 7937ms past the 5s takeover bar.
3. HIT: the cadence constant stands as filed, checkpoints at least every 64 chain objects with the lease manager targeting well under.
4. HIT: discovery linear at exactly count plus 1 GETs, 16 manifests at 469ms under 1s, a compacted group at 2 GETs.
5. HIT: composed 32-group boot at exactly 244 GETs and p50 1424ms, under the filed 3s and well under the 5s takeover bar.
6. HIT: StateSum agreement on all 21 boots, rebuilt stats exact on all 160 group rebuilds, WAL frames exact.

## Verdict

HIT on all six bands, the kill line untouched.
Boot cost is the estate you own: a fixed 4-GET floor near 85ms, then one GET and roughly 30ms per trailing chain object, per manifest, per segment, and per WAL-tail object, with groups fanning independently.
The cadence the fleet slices bake: checkpoints at least every 64 chain objects, and since a heartbeating 16-node fleet appends multiple batches per second, the record-and-seconds cadence of doc 02 must be checked against object count, not just records; a 60s lull at 16 nodes heartbeating each second is nearly a thousand objects, so the lease manager checkpoints on object count first.
Manifest chains want the same treatment: discovery is a GET per manifest, so the folder's manifest rewrite keeps the walk short and a compacted group boots its discovery at the 2-GET floor.
