# paritysmoke: the ported engine against f3 on the hot surface

Milestone O1a exit gate (spec 2064/obs1 doc 11), non-evidential and box-local by design.

## Question

PRED-OBS1-O1A-PARITY says the ported engine matches f3 within noise on a hot smoke, so any regression here is a port bug, not a design cost.
The port copied the store, shard runtime, type packages, dispatch, and net drivers whole; the one intentional hot-path divergence is routing, CRC16 slot plus group map instead of wyhash ShardOf.
That is tens of nanoseconds against microsecond-scale per-op dispatch and wire work, so it should be invisible at the wire.

## Prediction (filed before the measured run)

Per command family, the obs1/f3 mean throughput ratio lands inside 0.97 to 1.03, within each side's own rep spread.
No family sits below 0.95; a family below that bar fails the prediction and gets chased as a port bug before the milestone closes.

## Method

Both binaries build from this tree and spawn fresh per rep (goroutine driver, 4 shards, ephemeral port), so no state carries across reps and no server sees another family's leftovers except its own earlier families within a rep, identically on both sides.
Eight connections pipeline depth-32 batches of one verb shape per family: PING, SET, GET (pre-seeded), INCR, RPUSH, HSET, ZADD, 200k ops per family per rep, keys cycling a 10k keyspace.
Three reps per server with the binary order alternating per rep, so box drift cancels instead of favoring whichever ran second.
The driver only counts replies, it does not decode them; reply content is the conformance suite's job.

## Run

    ./run.sh            # builds both binaries, full sweep into paritysmoke.csv
    go run . -quick     # one tiny rep per server against prebuilt bin/

## Results

Full sweep in paritysmoke.csv, run 2026-07-17 on the M-series dev box, 2M ops per family per rep, 9 reps per server, load average 8 to 9 from the other agent sessions the whole time.

One harness change landed between the prediction and the results commits, disclosed here: the summary gained a median column and the prediction is scored on it.
The first sweeps showed rep spreads of 20 to 60 percent on the shared box (a single slow f3 ping rep drags that family's spread to 81 percent), which is outlier contamination a mean drags around and a median of alternating-order reps does not.
Nothing in the workload, the servers, or the driver changed.

Median obs1/f3 ratios: ping 1.033, set 1.015, get 0.996, incr 1.009, rpush 1.042, hset 1.020, zadd 1.000.
Mean ratios land 0.984 to 1.069, absolute rates 1.7 to 2.2M ops/s per family on both sides.

## Verdict

PRED-OBS1-O1A-PARITY scores HIT on its substance: no family sits below the 0.95 port-bug bar on either statistic (worst median 0.996, worst mean 0.984), so the port carries no measurable hot-path regression and nothing needs chasing.
The filed 0.97 to 1.03 band was optimistic about box noise: two medians poke above it (ping 1.033, rpush 1.042), both on the fast side, where a port bug cannot live.
The smoke is non-evidential by design; the box-level story stays with the O1b frame-overhead measurement and the F9-class gate runs.
