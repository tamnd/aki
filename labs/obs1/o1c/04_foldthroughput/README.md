# foldthroughput: the owner-side fold tax and the I/O-pool build rate

## Question

Doc 06 section 8 asks for MB/s folded per core and hot-path interference vs pass budget, carrying the f3 demote-budget discipline over and gating the owner-tick tax.
Fold is the direct descendant of f3 demotion, so the owner-side cost is not a model: StageColdDrain is the real SIEVE hand walk plus framing that the fold slice will drive, and this lab times it on a pressured store built through the public surface, with the two-phase drain completed the way the shard worker completes it.
The I/O-pool half is the segment build through the real encoder at comp 0; the zstd-worth lab (#1097) already priced comp 1, so the pipeline bound at comp 1 is the harmonic of the two measured rates.

## Method

Stage cells sweep value size {16 B, 200 B, 2 KiB} x pass budget {1, 4, 8, 16 MiB}: fill a store (512 MiB arena, 128 MiB resident cap, vlog and cold region in a temp dir) past the drain trigger plus 48 MiB of margin, then loop staging 160 MiB through StageColdDrain, writing each buffer with ColdWriteAt, flipping with CompleteColdDrain, and refilling with Set whenever pressure drops below the trigger.
Everything runs on one goroutine, which is the owner model; the stall is the StageColdDrain call time, and a command arriving mid-pass waits at most one stall.
Build cells pack 64 MiB segments (512 records per chunk) through BuildSegment plus AppendSegment at record size {200 B, 2 KiB}.
The cold write is measured but not gated: obs1 replaces it with segment PUTs on the I/O pool.

Smoke exposure: the -quick run exposed the (200 B, 8 MiB) stage cell and both build cells before predictions were filed; those bands are calibrated and disclosed as such.

## PRED-OBS1-O1C-FOLDTHRU (filed before the scored run)

1. Stage stall rate: 200 B values 1200 to 2200 MiB/s (smoke-calibrated), 2 KiB 2500 to 7000 (memcpy-bound), 16 B 150 to 600 (per-record hand-and-frame overhead dominates a 27 B record).
2. An 8 MiB pass is a multi-ms owner hole at every size: p50 stall 2.5 to 5 ms at 200 B (smoke-calibrated), 1.1 to 3.2 ms at 2 KiB, 13 to 53 ms at 16 B. The doc 06 budget therefore has to be an aggregate per tick, not one synchronous burst: the fold slice should serialize in sub-slices of at most 1 MiB, which this sweep's 1 MiB column will show as sub-ms stalls at 200 B and up.
3. The budget is a latency knob, not a throughput knob: sustained MiB/s at fixed value size varies under 20% across the 1 to 16 MiB budgets.
4. Owner fold tax at 100 MiB/s ingest (ingest divided by stall rate): at most 8% of owner time at 200 B (smoke-calibrated 6.1%), at most 4% at 2 KiB, 17 to 67% at 16 B, which is the known small-record wall every store has.
5. Build rate: 900 to 1400 MiB/s per core at 200 B records (smoke-calibrated), 3000 to 4500 at 2 KiB. Composed with the #1097 zstd-1 compress rates, one I/O-pool core sustains at least 400 MiB/s of build-plus-compress at 200 B, comfortably above any single-node design ingest.
6. Dry passes stay near zero: the hand's heat-clearing revolution does not starve a pressured store.

## Amendment (filed after the first scored attempt, before the rerun)

The first scored run refuted the 2 KiB stage cells structurally, not numerically: the store separates values over 1 KiB into the vlog by construction, separated records never enter the cold-stage path, and the cells dry-looped 10000 hand revolutions while NeedsColdDrain stayed true.
That is a finding, not a harness bug: fold's stage machinery covers the embedded band only, separated and chunked values reach segments by their own route (their bytes already live off-arena), and the fold slice has to account for both populations; the shard worker survives the same shape today by returning on an empty drain and paying one fruitless hand revolution per tick.
The stage sweep therefore replaces 2 KiB with 1000 B, the embedded ceiling, and prediction 1's 2 KiB band transfers as: 1000 B stall rate 2000 to 5000 MiB/s, 8 MiB stall p50 1.6 to 4 ms, owner tax at 100 MiB/s at most 5%.
The build cells keep 2 KiB records, since the encoder does not care where the store kept the bytes.

## Run

    ./run.sh            # full sweep, writes foldthroughput.csv
    go run . -quick     # smoke

## Results

Pending the scored run.

## Verdict

Pending the scored run.
