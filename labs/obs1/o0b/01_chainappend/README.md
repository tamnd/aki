# chainappend: the CAS append loop under contention

## Question

What do append latency and the 412 rate look like when 1 to 16 nodes drive one chain through the doc 02 section 2.4 loop, and which retry backoff should the append-loop slice bake: none, the spec's jittered 5 to 25ms after the first free retry, or a fixed 15ms?

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

(filled after the sweep)

## Verdict

(filled after the sweep)
