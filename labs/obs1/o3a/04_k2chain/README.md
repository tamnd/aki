# k2chain: chain contention at 16 nodes vs the flush cadence

## Question

K2 (doc 11 section 4): if chain contention forces flush cadence misses at 16 simulated nodes, log domains pull forward from deferred into O4 rather than tuning backoffs past their dignity.
This lab produces the numbers that decision hangs on, dated in #861 either way.
The O0b chain-append lab (#899) proved the raw CAS loop holds 16-node design load through probe-first and coalescing; this lab asks the same question of the as-built write path, 16 real Flusher plus Committer plus ChainAppender stacks sharing one chain, because the flush cadence lives in the flusher and the coalescing lives in the committer, not in the raw loop.

## Method

16 nodes over one simulated bucket on the doc 01 S3Standard latency model, real goroutines and wall clock, seeded.
Each node is the as-built pipeline: a real Flusher at defaults (8 MiB size trigger, 4-deep WAL PUT pipeline, swap-and-continue), a real Committer (16-deep queue, single drainer coalescing everything queued into one chain batch per append), and a real ChainAppender with probe-first catch-up feeding its own LeaseFold, all sixteen appenders on one shared chain.
Each node first grants itself one group at epoch 1 on the chain, then a load goroutine appends 100 ops per second of roughly 100-byte frames, the flush-cadence lab's trickle shape, which realizes the age trigger's worst case at every age setting.
Arms sweep flush-age 50ms (the shipped default, 320 offered commit records per second against a chain the O0b lab measured at about 25 appends per second), 250ms (thrift, 64 offered), and 1000ms (16 offered, under chain capacity), 60 seconds measured each after a 5 second warmup.

Measured per arm: per-node realized flush rate against the age trigger's rate, chain appends per second and records per batch (the coalescing), append latency per Committer.Append call (p50, p99, max across nodes), and commit lag from WAL delivery to the commit record landing (p50, p99, max).
Append latency is the lease-safety proxy: a successful append of the node's own is what renews its leases (the OnAppended seam), so an append stuck past the 2500ms gate belief window parks the node's writes and one stuck past the 3500ms staleness horizon starts the peer takeover watch.
Disclosed: the LeaseManager is not in the loop, so nothing here actually suspends or seizes; the lab measures the gap against the horizons the fleet slices proved, which is the direct mechanism either way.

## Prediction (PRED-OBS1-O3A-K2CHAIN, filed before the scored run)

Derived from the O0b chain-append lab's probe-arm numbers (25 appends per second at design load, commit p50 1.5s, worst record 13.2s) and the flush-cadence lab's trickle arithmetic.

1. The chain sustains every arm: total appends per second in [18, 32] at age 50ms with records per batch in [8, 16], near 24 with 2 to 4 records per batch at 250ms, near 16 with 1 to 2 at 1000ms; the committer never fails and every chain is dense.
2. WAL flush cadence holds on every arm: per-node realized flush rate at or above 90 percent of the age-trigger rate (18/s at 50ms, 3.6/s at 250ms, 0.9/s at 1000ms), because WAL PUTs are per-node keys with no contention and the delivery path only blocks when the committer queue is full.
3. Commit lag at age 50ms: pooled p50 in [0.8s, 3s], p99 under 12s; at 1000ms, p50 under 300ms.
4. The decision band: at age 50ms at least one node's worst append latency crosses the 3500ms staleness horizon, on the O0b lab's 13.2s worst-record precedent, which means the single chain cannot carry 16-node default-cadence load inside the lease discipline and K2 fires: log domains pull forward into O4.
   At 1000ms every append stays under the 2500ms belief window, so a 16-node fleet on one chain is safe below chain capacity.
   The 250ms arm dates where the cliff sits.

Kill line: this prediction's bands score the model, but the K2 decision follows the measurement, not the prediction; whichever way band 4 lands, the checkpoint is dated in #861 with the measured gaps and the deferral status of log domains.

## Run

    ./run.sh    # writes k2chain.csv

## Results (scored run, 2026-07-24)

| age | flush rate min/max (target) | appends/s | rec/batch | append p50/p99/max ms | lag p50/p99/max ms |
|-----|------------------------------|-----------|-----------|------------------------|---------------------|
| 50ms | 10.2 / 14.1 (20) | 24.9 | 8.0 | 47 / 5238 / 7446 | 928 / 7199 / 10376 |
| 250ms | 3.6 / 3.9 (4) | 21.8 | 2.8 | 266 / 4488 / 11838 | 936 / 6238 / 16582 |
| 1000ms | 1.0 / 1.0 (1) | 15.6 | 1.0 | 589 / 2272 / 3067 | 655 / 2360 / 3067 |

## Verdict

K2 fires: log domains pull forward from deferred into O4.

Band 1 HIT: the chain sustains every arm at the O0b lab's capacity, 24.9 appends per second at defaults with 8 records per batch, dense everywhere, no failures.
Band 2 MISS on the defaults arm, and that miss is the K2 trigger itself: per-node realized flush rate is 10.2 to 14.1 against the 20 per second age-trigger rate, because the committer's queue fills behind slow contended appends, delivery blocks, and the flusher's swap loop stalls behind its own pipeline, the block-not-drop chain doing exactly what it promises at a load the chain cannot drain.
Band 3 split: defaults lag p50 928ms and p99 7.2s inside their bands, the 1000ms arm's p50 655ms above the predicted 300ms because even 16 offered appends per second against 16 racing nodes prices every append at a contended race, not a solo round trip.
Band 4, the decision: worst appends cross the 3500ms staleness horizon at defaults (7.4s) and at thrift (11.8s), and even the under-capacity 1000ms arm's worst append (3.07s) exceeds the 2500ms gate belief window.
A 16-node fleet on one chain misses its flush cadence at the shipped defaults and cannot keep its lease renewals reliably inside the discipline at any swept cadence, which is doc 11's K2 condition met in both halves; per the criterion the answer is log domains, not backoff tuning.
The prediction called the decision and the mechanism; the band 2 arithmetic was wrong about where backpressure first bites, and the honest reading is that the cadence miss arrives through the delivery path before the append tail even matters.
