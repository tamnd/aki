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

Pending the measured run; this section lands in the results commit.
