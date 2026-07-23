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

Pending the scored run.
