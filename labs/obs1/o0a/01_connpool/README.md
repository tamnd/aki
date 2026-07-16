# connpool: stdlib connection reuse and TLS session cost under GET fan-out

Milestone O0a lab 01 (spec 2064/obs1 doc 11 sections 1 and 7, doc 01 section 2.2).

## Question

Which MaxIdleConnsPerHost does the O0a client core bake, and does stdlib net/http connection reuse hold up at the fan-out a hot node needs, or does the doc 11 risk-register escalation (documented tuning, never an SDK) trigger already?
The seed prediction this lab feeds is PRED-OBS1-O0A-POOL: a warm pool sustains 5,000 GET/s per node from one process without client-side queuing at the doc 01 latency model.

## Method

One binary, one configuration per run, CSV rows to stdout; run.sh sweeps them into connpool.csv.
Every request is a full GET of a -size byte object with the body read to the end, latencies sampled per request, and httptrace counting how many requests rode a reused connection versus a fresh dial.

Arms:

- inproc-http: an in-process HTTP server, so the numbers isolate the client transport with no server variance; sweeps concurrency x pool limit.
- inproc-tls: the same server with TLS, isolating handshake and session cost; the fresh sub-arm disables keep-alive so every request pays a dial plus handshake.
- fresh: keep-alive off over plain HTTP, the per-connection setup floor without TLS.
- minio: signed GETs against a real local MinIO (AKI_OBS1_S3, default http://127.0.0.1:19000), the real-server cross-check for the reuse fractions; the lab creates its own bucket and object, and carries a trimmed copy of the SigV4 signer since the engine client lands after this lab by design.

Local numbers answer the pool-mechanics question only.
Cloud latency (doc 01 section 2.2) multiplies the cost of every non-reused connection by orders of magnitude; the simulator carries that model, and O5 confirms it live, so the verdict here is about reuse fractions and client-side ceilings, not about predicting cloud GET/s.

## Run

    ./run.sh            # full sweep into connpool.csv; starts nothing, expects MinIO up for the minio arm
    go run . -quick     # smoke: one small configuration per arm that needs no MinIO
    go test .           # harness test, tiny counts

## Results

Pending.

## Verdict

Pending.
