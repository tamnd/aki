# obs1 predictions

Filed before the matching measured run, per the milestone ground rules
(spec 2064/obs1 doc 10 section 8, doc 11 section 5). A prediction is
scored when its gate row runs: HIT, MISS-LOW, or MISS-HIGH, dated, with
the run's provenance line. Misses get a sentence about what the model
missed, and a K4-relevant miss freezes gating until the simulator refit.

## Seed table (doc 11 section 5, written before any code)

| ID | Claim | Prediction | Scored |
|----|-------|------------|--------|
| PRED-OBS1-HOT-TAX | hot gate tax from WAL frames + tier branch | < 5% vs f3 baseline | pending |
| PRED-OBS1-CHAIN-APPEND | chain append p50 / 412 rate at design load | one PUT RTT / < 5% | pending; sim rung 2026-07-16 (see PRED-OBS1-O0B-APPEND): the p50 half holds at every contention level, the 412 half will MISS-HIGH under any real contention (50% at just 2 contenders, 93% blind at 16, 64% probe-first) since every node races every link by design; final score at the O0b exit gate |
| PRED-OBS1-HANDOFF-WARM | graceful handoff, warm taker | p50 < 300ms, p99 < 1.5s, flat in data size | pending |
| PRED-OBS1-TAKEOVER | crash takeover to serving | < TTL + 2s | pending |
| PRED-OBS1-LOSS-WINDOW | relaxed loss window, defaults | p99 < 250ms of acked writes | pending |
| PRED-OBS1-STRICT-ACK | strict ack, Standard / Express | 90-140ms / 15-30ms | pending |
| PRED-OBS1-COLD-READ | cold point read, Standard | p50 25-45ms, hedged p99 <= 2.5x p50 | pending |
| PRED-OBS1-REQ-PER-GIB | requests per ingested GiB | ~300 | pending |
| PRED-OBS1-BILL-B | example-B monthly bill, diskless | ~$3.2k, 30-40x under RAM rivals | pending |
| PRED-OBS1-IDLE-FLOOR | idle floor, 3 nodes | ~$42/month | pending |
| PRED-OBS1-FLEET-SCALE | fleet scaling 3 to 12 nodes | >= 0.8 linear | pending |

## Milestone predictions

| ID | Claim | Prediction | Scored |
|----|-------|------------|--------|
| PRED-OBS1-O0A-POOL | warm stdlib pool, one process, doc 01 latency model | sustains 5,000 GET/s per node without client-side queuing; local lab shows reuse fraction > 0.99 once pool >= concurrency, and no throughput cliff through 512-way fan-out | HIT 2026-07-16, sim rung (labs/obs1/o0a/02_poolrate, S3Standard seed 1, M-series dev box): 5,000/s at pool 256 peaks 172 in flight with max slot wait 12us over 50k requests, measured ceiling 8,629/s (Little's law says 8,797), queuing signature confirmed reachable past the ceiling and at pool 128; local rung was #875; O5 E-cloud re-scores live after the model refit |
| PRED-OBS1-O0B-APPEND | doc 02 section 2.4 append loop, 16 nodes at 20 records/s each, doc 01 latency model | append-loop p50 within 2x one PUT round trip (70ms on the model's 35ms PUT p50, since roughly half of contended attempts pay one catch-up GET plus a second PUT); 412 rate under 5% of PUTs only while offered appends stay under ~40% of the chain ceiling (~20-28 appends/s on the model), so at 16 nodes the fleet holds the record rate through coalescing (batches grow toward rate x RTT records) and the 412 rate lands ABOVE 5%, more likely 20-40%, without hurting record commit latency more than one extra round trip; backoff verdict: spec jitter beats none on 412 rate at 16 nodes but buys little at 4 or fewer | MISS-HIGH 2026-07-16, sim rung (labs/obs1/o0b/01_chainappend, S3Standard seed 1, M-series dev box): the p50 half HIT (34-45ms, ~1.2x one PUT round trip, every arm), but the 412 rate is 93% not 20-40%, the blind loop does NOT hold design load (233 of 320 records/s over 60s, commit p50 climbs to 46s), and backoff is a dead knob (all three policies tie on sim; spec jitter is harmful on a fast store, 60 vs 313 appends/s on local MinIO). What the model missed: blind PUTs serialize winner selection at ~1 append per 2.5 round trips because a losing PUT wastes a full round trip before catch-up starts, and unfair win rotation keeps batches below the fair-share size coalescing needs. The lab's probe-first arm (GET the target after a first 412, PUT only on 404) restores the claim: 2.1x appends/s, stable at design load (312/320), commit p50 1.5s, 3.6x fewer PUTs; slice 7 bakes probe-first with no sleep, amending the doc 02 literal loop |
