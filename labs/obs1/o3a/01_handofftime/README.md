# handofftime: the graceful handoff window priced on the landed primitives

## Question

Doc 02 section 4.3 hands a group over in seven steps, and the O3a graceful-handoff slice has to hit the milestone bars: p50 under 300ms, p99 under 1.5s, flat in data size from 1 GiB to 1 TiB.
What do those steps actually cost on the landed primitives, and what ordering and fetch plan must the slice bake for the bars to be reachable?

## Method

The handoff prices as four terms, each measured on the real engine pieces against the sim with the doc 01 S3 Standard envelope drawn per request: the holder's release append through a real ChainAppender, the taker's Follow poll that observes it (one batch GET plus the 404 tail probe), the taker's grant append, and the serial GET-plus-parse replay of the D WAL-tail objects past the fold cursor.
Two nodes hand the group back and forth for the rep count, both folding the chain through real LeaseFold instances, and the lab asserts after every handoff that both folds agree on the holder and the monotone epoch.
The flatness claim is proven by op accounting rather than timing: the sim's request counters over the whole warm phase must equal exactly the chain and WAL-tail ops, so a single size-dependent GET leaking into the window is a hard error, which is stronger evidence than two identical code paths timing alike.
The pre-warm term a crash taker pays is the as-built RebuildResident, one serial whole-object GET per manifest segment; the lab sweeps it over segment count serial and behind a lab-side prefetch fan (parallel GETs into memory, then the unchanged RebuildResident feeding from the prefetched bodies through a Store shim), with rebuilt stats asserted identical across arms.
Segments are real BuildSegment objects of plain string run chunks cut the folder's way; WAL-tail objects are real AppendWAL sections whose replayed frame counts are asserted.

## Envelope disclosure

The sim draws per-request latency with no bandwidth term, so a 4 MiB segment GET and a 16 KiB one price the same; the results section carries the doc 01 fit arithmetic (100 MB/s per lane) for the real-size projection, refit at O5 on live infra.
In the seven-step order the holder seals and publishes before releasing, so the graceful window's WAL-tail depth is 0 or 1 by construction; depth 4 is reported as the sensitivity row and deep tails belong to the crash-boot lab.
The cold-takeover row is composed from measured term medians (observe + grant + rebuild + replay at depth 4), not a separately clocked run, and the taker-side full-TTL wait is policy time on top of it.
Warm-window quantiles at a given rep count share their chain terms across the depth rows, since each rep prices all depths against the same release, observe, and grant draws.

## Prediction (PRED-OBS1-O3A-HANDOFF, filed before the scored run)

1. Warm graceful window (depth 0 and 1): p50 under 300ms and p99 under 1.5s, the milestone bars, at 40 reps.
2. Flatness: the warm phase bills exactly its per-rep chain and WAL ops with zero segment GETs, the op-count invariant holds, and no term in the window scales with data size; this is the form the 1 GiB to 1 TiB claim takes at sim scale.
3. Replay is linear in depth: the sweep medians grow proportionally and depth 16 stays under 700ms serial.
4. Serial rebuild is linear in segment count at roughly 20 to 45ms per segment, which puts a 256-segment group past 5s, over even the crash-takeover bar; as built, a cold taker cannot meet any bar at fleet segment counts.
5. The prefetch fan restores it: fan 8 at 256 segments speeds rebuild up at least 4x over serial and lands under 1.5s.
6. Composed cold takeover at 256 segments: under the 5s takeover bar (lease TTL 3000ms plus 2s) with fan 8, over it serial.
7. Correctness throughout: both LeaseFolds agree on holder and epoch after every handoff, rebuilt stats are identical across all arms, and replay walks exactly the built frame counts.

Kill line: a warm graceful window p50 at or over 300ms or p99 at or over 1.5s, any size-dependent op inside the window, fan 8 under 3x at 256 segments, or any stat or fold divergence means the pre-warm-plus-fan plan the handoff slice intends to bake is wrong, and the slice does not start until the design is rethought.

## Calibration disclosure

A quick smoke (4 and 8 segments, 6 reps, 256 records per segment) ran during development before this file was committed and confirmed the mechanics: op invariant green, stats identical across arms, folds agreeing, windows in the hundreds of milliseconds with wide small-n medians.
The bands above come from the doc 01 envelope arithmetic and the milestone bars, not from tuning to that smoke; the scored run below is a fresh full-size execution.

## Run

    ./run.sh
