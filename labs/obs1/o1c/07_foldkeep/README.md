# foldkeep: the replay floor under sustained design ingest

## Question

The FOLDKEEP prediction row asks whether fold sustains design ingest inside its pass budget without WAL-replay-floor growth: boot replay applies every frame above the fold cursor, so if the gap between the committed watermark and the published FoldSeq grows with total ingest, replay work at boot grows with runtime and the floor is not a floor.
This lab runs the real pipeline against the sim under paced ingest and samples that gap live.

## Method

The reqgib rig (write log at the 8 MiB size trigger, chain committer, folder, publisher) with ingest paced at 100 MiB/s, the design rate the fold-throughput lab priced, everything-cools shape, 1000 B values, four groups.
Every 128 MiB the lab reads each group's latest published manifest off the bucket (LoadManifests plus SelectManifest, the boot path's own read) and takes lag = committed watermark minus FoldSeq in frames, worst group and mean.
After the run, barrier plus fold flush plus quiesce, then a final settled reading.
An unpaced contrast arm runs the same payload with no pacing, where sim ingest outruns any real network and the fold is expected to fall behind; that arm bounds the margin, it is not the row's claim.

Smoke exposure: a 0.5 GiB paced calibration run measured a sawtooth with max lag 16k to 18k frames, trend ratio 1.12, final lag zero, and 36 segments; the bands below are calibrated on it and disclosed as such.
The calibration also surfaced the mechanism: at design pace the 500 ms fold-age flush governs the cut, so segments land around 14 MiB, well under the 64 MiB size target, and it is the age cadence that bounds the floor.

## PRED-OBS1-O1C-FOLDKEEP (filed before the scored run)

1. No growth at design pace: over 2 GiB the second-half max lag stays within 1.5x of the first-half max (smoke measured 1.12; the allowance is sawtooth phase, not trend).
2. The lag ceiling is cadence-bound: max lag across all paced samples at most 24k frames, about 24 MiB of frames in the worst group, set by the fold-age flush not the segment size target.
3. The settled floor is exact: final lag zero in every group after barrier plus flush plus quiesce.
4. The run completes with zero pipeline errors (build, walk, coverage, row), which run() enforces by failing otherwise.
5. Segment count confirms the cadence mechanism: the paced 2 GiB run cuts 120 to 160 segments (the 0.5 GiB smoke's 36 scaled), against 36 or so if the 64 MiB size target governed.
6. The unpaced contrast arm shows growth: trend ratio above 1.5, the shape the quick smoke already showed at shrunken constants (1.69).

## Run

    ./run.sh            # scored paced arm + unpaced contrast, writes foldkeep-*.csv
    go run . -quick     # smoke at shrunken constants, unpaced

## Results

Scored run, 2 GiB at 1000 B values, four groups, production flush and segment constants; foldkeep-paced.csv and foldkeep-unpaced.csv hold the full sample series.

Paced arm at 100 MiB/s: max lag over 16 samples was 18369 frames, first-half max 18369 against second-half max 12225 for a trend ratio of 0.67, final settled lag zero in every group, 132 segments, zero pipeline errors.
Unpaced contrast arm: lag climbs from 32k to 267k frames across the run, trend ratio 1.63, only 19 of an eventual 40 segments published when ingest ends.

Scoring the six predictions:

1. HIT. Trend ratio 0.67, well inside 1.5; the second half actually ran lower than the first as the publisher warmed up past its opening sawtooth.
2. HIT. Max paced lag 18369 frames, inside the 24k cadence band; the size target's 66k-frame window never came close to governing.
3. HIT. Final lag zero in every group after barrier plus flush plus quiesce.
4. HIT. Both arms completed with zero build, walk, coverage, and row errors.
5. HIT. 132 segments against the 120 to 160 band, confirming the fold-age flush governs the cut at design pace (about 15 MiB per segment, not 64).
6. HIT. Unpaced ratio 1.63, the growth shape, with the publisher visibly saturated (19 of 40 segments at ingest end).

The verdict for the row: at design ingest the replay floor tracks the committed watermark inside a cadence-bounded sawtooth of at most about 18k frames (roughly 18 MiB in the worst group) and settles exact at quiesce, so boot replay work is bounded by the fold-age window, not by runtime.
The unpaced arm records the margin honestly: sim ingest with no network in the way does outrun the fold, so the floor guarantee is a rate-conditional one and the design rate sits comfortably inside it.
One caveat carries over from #1275: FoldSeq over-claims resident records no segment holds, so the floor this lab measures is the manifest's claimed floor; the sound cursor plus WAL trimmer pair stays owed and replay tolerates the over-claim until then.
