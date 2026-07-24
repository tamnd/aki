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

## End-to-end re-score (filed with the graceful-handoff slice, before its scored run)

The primitive-level run above priced the window as four hand-rolled terms; the slice landed the real machinery (LeaseManager.Handoff batching the final flush's commit with the release, TailWindow retention, TakeGroup replay from the fold cursor), so the e2e phase re-scores the same prediction on the real sequence: holder Handoff with a real WAL PUT and commit record, taker Follow, Reconcile, Acquire, and TakeGroup, warm taker, back and forth for the rep count.
The taker stays at replay depth 1 by construction (the seal-and-publish-before-release discipline), enforced in the lab by a checkpoint record after each rep, which also exercises the window trim.
Bands: the e2e window (Handoff through TakeGroup return) hits the same bars, p50 under 300ms and p99 under 1.5s; the term arithmetic above (release 30, observe 58, grant 48, replay 22, plus one WAL PUT near 30) predicts a p50 near 200ms.
Op invariant, hard-asserted: exactly 3 GETs (observe pair plus one ranged WAL section) and 3 PUTs (WAL, handoff batch, grant) per rep on the clock, zero segment GETs, nothing size-dependent; folds agree on holder and epoch after every rep, epochs monotone, and every rep replays exactly its flushed frame count.
Kill line: either bar missed or any op-invariant breach means the slice's sequence adds cost the primitives did not price, and the slice does not merge until the term that grew is named.

## Crash-takeover e2e (PRED-OBS1-O3A-TAKEOVER, filed with the crash-takeover slice, before its scored run)

The cold-takeover row above composed the bar from term medians; the slice landed the real case (b) machinery (TakeoverJudge discipline, LeaseManager.Takeover, cold TakeGroup with the fan-8 prewarm), so this phase scores the milestone prediction on the real sequence.
Each rep the holder flushes a depth-4 WAL tail with real commits and goes silent; the taker runs the discipline on a policy clock (chain-observed staleness of TTL plus skew, then the full-TTL watch, 6600ms simulated the way production waits it out); then the mechanics run on the wall clock: the silence probe, the Takeover grant, and the cold TakeGroup rebuilding keymap and directory behind the fan of 8 over the full 256-segment set plus the ranged replay of the retained tail.
The 5000ms bar (lease TTL 3000ms plus 2s) prices the mechanics, the primitive run's convention: the discipline is deliberate policy waiting, reported beside the window, not inside it.
Bands: mechanics p50 under 5000ms and p99 under the same bar with room; the term arithmetic (probe 29, grant 48, fan-8 rebuild 1148, depth-4 replay 120) predicts a p50 near 1350ms.
Op invariant, hard-asserted: on the clock exactly 1 silence-probe GET, 256 segment GETs, 4 ranged WAL section GETs, and 1 grant PUT per rep; off the clock exactly the depth's WAL PUTs and commit appends, the pre-crash follow, the checkpoint, and the wake follow; the retained window trims to zero every rep.
Correctness bands: every rep the grant lands at epoch plus one with both folds agreeing after the wake, the woken holder demotes with nothing held, the rebuild stats equal the full segment set, and the replay walks exactly the depth's frames to the flush's last seq.
Manifest disclosure: the manifest hands TakeGroup the fixed segment set with the fold cursor at the rep's floor; the frames below the floor live in the retained tail rather than the segments, which TakeGroup does not and should not verify, the same trust the boot path places in a published manifest.
Kill line: mechanics p50 or p99 at or over 5000ms, any op-invariant breach, or any fold or stat divergence means the takeover sequence costs what the primitives did not price, and the slice does not merge until the grown term is named.

## Calibration disclosure

A quick smoke (4 and 8 segments, 6 reps, 256 records per segment) ran during development before this file was committed and confirmed the mechanics: op invariant green, stats identical across arms, folds agreeing, windows in the hundreds of milliseconds with wide small-n medians.
The bands above come from the doc 01 envelope arithmetic and the milestone bars, not from tuning to that smoke; the scored run below is a fresh full-size execution.

## Run

    ./run.sh

## Results

One scored run, full size (handoff.csv): 440 objects built, 40 handoff reps, rebuild reps of 3, every correctness assertion green.

Warm window (release + observe + grant + replay), zero segment GETs by the sim's own counters:

| depth | p50 ms | p99 ms |
|---|---|---|
| 0 | 144.7 | 465.3 |
| 1 | 175.1 | 568.2 |
| 4 | 280.5 | 532.5 |

Terms: release p50 30.2ms, observe 57.8, grant 47.8, replay 21.5 at depth 1.
Warm phase billed exactly 9 GETs and 2 PUTs per rep, none of them size-dependent.

Rebuild sweep, ms per segment in parentheses:

| segments | serial p50 | fan 8 | fan 32 |
|---|---|---|---|
| 8 | 271 (33.8) | 54 | 90 |
| 32 | 965 (30.1) | 217 | 146 |
| 128 | 4348 (34.0) | 896 | 620 |
| 256 | 8123 (31.7) | 1148 | 476 |

Replay sweep p50: 29.3ms at depth 1, 120.3 at 4, 488.9 at 16.
Composed cold takeover at 256 segments: serial 8342ms, fan 8 1366ms, fan 32 695ms against the 5000ms bar.

Band scoring:

1. HIT: warm graceful window p50 144.7ms at depth 0 and 175.1ms at depth 1 against the 300ms bar, p99 465.3 and 568.2ms against 1.5s.
2. HIT: the op-count invariant held as a hard assertion, exactly 2 chain PUTs, 4 chain GETs, and the depth's WAL GETs per rep, zero segment GETs, no size-dependent term.
3. HIT: replay medians 29.3, 120.3, and 488.9ms track depth 1, 4, and 16 near-proportionally, depth 16 under the 700ms band.
4. HIT: serial rebuild at 30.1 to 34.0ms per segment at every count, and 256 segments serial takes 8.1s, past even the 5s takeover bar.
5. HIT: fan 8 at 256 segments is 7.1x over serial and lands at 1148ms, under 1.5s.
6. HIT: composed cold takeover at 256 segments is 1366ms with fan 8, under the 5s bar, and 8342ms serial, over it.
7. HIT: both folds agreed on holder and epoch after all 40 handoffs, rebuilt stats identical across all twelve arm-count pairs, replay walked exactly the built frame counts.

Two edges worth recording: the depth-4 sensitivity row's p50 came out at 280.5ms, grazing the graceful bar from below, which is why the slice must keep the holder's seal-and-publish ahead of the release so graceful replay stays at depth 0 or 1; and fan 32 lost to fan 8 at 8 segments (90 vs 54ms), straggler draws over too few objects, so the fan constant should not exceed the segment count.

## End-to-end result (scored with the graceful-handoff slice)

One scored full run after the slice landed (handoff.csv), 40 reps of the real sequence, every assertion green.
The e2e window came out at p50 213.5ms and p99 703.3ms against the 300ms and 1.5s bars, right where the term arithmetic put it, and slightly wider at p99 than the primitive run because the real window carries the WAL PUT and the replay's ranged GET in one tail.
The op invariant held as a hard assertion: exactly 3 GETs and 3 PUTs on the clock per rep, zero segment GETs, one checkpoint PUT plus the three catch-up GETs off the clock, and the checkpoint trimmed the retained window to zero every rep.
Folds agreed on holder and epoch after all 40 reps with epochs monotone, and every rep replayed exactly its 128 flushed frames to the flush's last seq.
PRED-OBS1-O3A-HANDOFF re-scored HIT on the real machinery; the kill line stays untouched.

## Crash-takeover e2e result (scored with the crash-takeover slice)

One scored full run after the slice landed (handoff.csv), 20 reps of the real sequence at 256 segments and depth 4, every assertion green.
The mechanics came out at p50 1412.3ms and p99 2369.0ms against the 5000ms bar, right where the term arithmetic put it, with the fan-8 rebuild carrying almost the whole window and the p99 tail coming from straggler segment draws across the fan lanes.
The bill held as a hard assertion: exactly 261 GETs (the silence probe, 256 segments, 4 ranged WAL sections) and 1 grant PUT on the clock per rep, the off-clock flush, follow, checkpoint, and wake ops exact, and the retained window trimmed to zero every rep.
Every grant landed at epoch plus one with both folds agreeing after the wake, every woken holder demoted holding nothing, every rebuild matched the full segment set, and every replay walked its 512 frames to the flush's last seq.
The policy wait beside the window is 6600ms simulated: chain-observed staleness at TTL plus skew, then the taker's full-TTL watch.
PRED-OBS1-O3A-TAKEOVER scored HIT on the real machinery; the kill line stays untouched.

## Verdict

HIT on all seven bands, the kill line untouched.
The handoff slice bakes what the lab priced: the taker pre-warms manifest, directory, and keymap before the release lands so the graceful window is chain ops plus a shallow WAL tail and nothing that scales with data, and crash takeover fetches segments behind a fan of 8 or the 5s bar is unreachable past roughly 150 segments.
The sim carries no bandwidth term; at the doc 01 fit's 100 MB/s lane, real 4 MiB segments add roughly 40ms per segment serial or 40ms over fan lanes per segment fetched, which moves the serial arm further past the bar and leaves the fan-8 arm at roughly 2.4s for 256 segments, still under it, to be refit on live infra at O5.
