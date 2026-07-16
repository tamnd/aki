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
| PRED-OBS1-CHAIN-APPEND | chain append p50 / 412 rate at design load | one PUT RTT / < 5% | pending |
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
| PRED-OBS1-O0A-POOL | warm stdlib pool, one process, doc 01 latency model | sustains 5,000 GET/s per node without client-side queuing; local lab shows reuse fraction > 0.99 once pool >= concurrency, and no throughput cliff through 512-way fan-out | pending |
