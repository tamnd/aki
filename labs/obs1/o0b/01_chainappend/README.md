# chainappend: the CAS append loop under contention

## Question

What do append latency and the 412 rate look like when 1 to 16 nodes drive one chain through the doc 02 section 2.4 loop, and which retry backoff should the append-loop slice bake: none, the spec's jittered 5 to 25ms after the first free retry, or a fixed 15ms?
A fourth arm, probe, is not a backoff: once a 412 proves a race it GETs the target seq before each further PUT, to price the burned PUTs the blind loop pays (a GET is 12.5x cheaper than a PUT on S3).

The doc 11 prediction (PRED-OBS1-CHAIN-APPEND) says append p50 stays near one PUT round trip and the 412 rate stays under 5% at design load.
The analytic worry is that a single chain admits roughly one append per PUT round trip in total, about 20 to 30 per second on the doc 01 latency model, so 16 nodes at 20 records per second each can only survive through coalescing: batches must grow instead of the queue.
The lab exists to see whether that holds and what it does to the 412 rate.

## Method

Each node generates records on a fixed schedule (the doc 04 flush shape), keeps at most one append in flight, and folds everything pending into the next batch, so records per batch is the coalescing measurement, not an input.
Batches are real doc 03 bytes through AppendChainBatch: one commit record plus one heartbeat per coalesced record.
The loop is the real protocol: PUT If-None-Match at tail+1, on 412 read the winner and retry one up, on 409 or ambiguity re-read and recognize our own object by writer id and batch id in the parsed bytes.
The harness test walks the finished chain and proves it dense: every seq holds a parseable batch and the record count on the chain equals what the nodes generated.

Arms: the three backoff policies at 16 nodes design rate, the contender ladder 1/2/4/8/16 on the spec policy, the rate ladder 20/80/320 records per second per node at 16 nodes, all against the simulator's doc 01 latency model, plus a live MinIO cross-check into a fresh bucket.

## Run

    ./run.sh            # sim arms always; minio arms when AKI_OBS1_S3 is set
    go run . -header    # CSV column names
    go run . -quick     # one short sim row

## Results

2026-07-16, M-series dev box, sim seed 1 (S3Standard: GET 20/150ms, PUT 35/260ms), MinIO local at sub-2ms RTT; full data in chainappend.csv.

Design load is 16 nodes at 20 records per second each, 320 records per second offered to one chain.

| arm | appends/s | 412/PUT | recs/batch | commit p50 | commit p99 |
|-----|-----------|---------|------------|------------|------------|
| sim none 16x20 60s | 11.9 | 93.7% | 19.5 | 45.6s | 78.8s |
| sim probe 16x20 60s | 24.9 | 63.8% | 12.5 | 1.5s | 8.4s |
| sim spec 16x20 | 9.3 | 93.2% | 16.6 | 11.5s | 19.3s |
| sim fixed 16x20 | 9.5 | 93.3% | 15.6 | 12.8s | 20.5s |
| minio none 16x20 | 313.0 | 91.7% | 1.02 | 9.2ms | 125ms |
| minio probe 16x20 | 292.7 | 50.4% | 1.06 | 4.4ms | 306ms |
| minio spec 16x20 | 59.9 | 90.0% | 4.95 | 2.6s | 10.3s |

The single chain is latency-bound: one node alone sustains 15.4 appends per second on the model (one append per mean PUT round trip), and the blind loop under 16-way contention drops to 10 to 12 because every link pays a loser catch-up cycle before the next winner can move.
The winner path stays at one PUT round trip at every contention level (append p50 34 to 45ms everywhere on sim); contention lives entirely in the tail.
At design load the blind loop never reaches steady state: the 60s arm commits 233 of the offered 320 records per second, the backlog grows for the whole run, and commit latency climbs to 46s p50 by the end.
Coalescing does grow batches (19.5 average) but unfair win rotation keeps them below the roughly 27 the fair-share balance needs.

Backoff is a dead knob at S3 latencies: none, spec, and fixed tie within noise on every sim metric, because the mandatory catch-up GET already paces retries at about one round trip.
On a fast store the sleep is actively harmful: MinIO at 2ms RTT does 313 appends per second with no backoff and 60 with the spec jitter, since a 5 to 25ms nap lets the chain run ahead and the catch-up is one GET per link (commit p50 2.6s versus 9ms).

Probe-first dominates the blind loop on the latency model on every axis at once: 2.1x the appends per second, stable at design load (312 of 320 records per second, 1.6s drain, commit p50 1.5s, worst record 13.2s), 3.6x fewer PUTs, and the request bill per committed record drops 2.6x once PUTs are priced at 12.5 GETs.
The mechanism is winner selection speed: a blind PUT that will 412 wastes a full PUT round trip before the node even starts catching up, while a probe GET both tracks the chain and finds the 404 that says go.
On the fast store probe trades 6% of throughput and a fatter tail (306ms versus 125ms p99) for 8x fewer PUTs; nothing breaks.

The 412 rate never comes near the seed prediction's 5% under real contention: 93% blind, 64% probe, and already 50% at just 2 contenders, because every node races every link by design and the rate only measures how the losses are priced.

## Verdict

Bake into the append loop: no sleep between CAS retries at any attempt (the catch-up GET is the backoff), and probe-first catch-up after the first 412 on a key (blind PUT only on the uncontended fast path).
This deviates from the doc 02 section 2.4 literal loop, which re-PUTs blind after each catch-up; the lab says that shape cannot hold design load on S3 latencies and probe-first can, so slice 7 should implement probe-first and doc 02 gets the amendment.
Revisit the no-sleep half only if live S3 answers 503 SlowDown, which the loop's 5xx path already re-reads through.
One open flag for the lease slice: even with probe, the worst record waited 13.2s at saturated design load, so heartbeats must not queue behind data commits or a saturated chain will expire live leases.
