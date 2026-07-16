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

Full sweep in connpool.csv, run 2026-07-16 on the M-series dev box (macOS, 4 KiB objects, MinIO RELEASE.2025-09-06 local on 127.0.0.1:19000), zero dial stalls after the TIME_WAIT pauses went in.

Reuse: the stdlib transport keeps reused_frac above 0.99 in every keep-alive configuration, including pool 32 under 512-way fan-out, because under sustained load in-flight connections never become idle and the idle cap almost never triggers a close.
The pool size matters at the margins: at conc 512 against MinIO, pool 64 re-dialed 745 times where pool 256 re-dialed 284, and in-process throughput at conc 512 was 114.7k ops/s with pool 256 against 102.7k with pool 64 (about 12%).

Throughput: no cliff anywhere.
In-process HTTP holds 94k to 128k GET/s from conc 16 through 512; real MinIO plateaus at 22k to 29k GET/s from conc 64 up, server-bound, with client p50 growing linearly in concurrency exactly as queuing predicts.
One process against a real S3 wire protocol clears 5,000 GET/s at conc 1 already (5.2k to 6.3k), and by conc 16 it is 18k to 24k.

TLS: session reuse works (reused_frac 0.999 at every level), but the few fresh handshakes under CPU contention are brutal: inproc-tls p99 goes from 2.9ms at conc 16 to 151ms at conc 512 while plain HTTP stays under 18ms, and throughput halves.
The handshake, not the session, is the cost; anything that forces fresh TLS connections in the hot path (pool too small for the fan-out, keep-alive hiccups) pays tens of milliseconds per event.

Fresh (keep-alive off): the per-request dial floor is about 400us plain and local; at conc 256 the arm needed its own cool-down because it burns an ephemeral port per request and macOS runs out within two configurations.
That is the risk-register failure shape, reproduced on demand.

## Verdict

Bake MaxIdleConnsPerHost 256 into the client core (engine/obs1 s3client.go).
Reuse holds regardless, but 256 cuts re-dials by 2 to 3x at high fan-out, buys about 12% throughput at conc 512, avoids fresh TLS handshakes (the one measured catastrophe), and idle sockets cost nothing we can measure.
Stdlib net/http is comfortably inside the bar: no SDK, no custom pool, the doc 11 escalation stays untriggered.
PRED-OBS1-O0A-POOL local rung: supported, reuse above 0.99 and no cliff through 512-way; the 5,000 GET/s claim sits 4x under the single-connection-count local ceiling against a real server, and final scoring waits for the simulator's doc 01 latency model plus the O5 cloud run.
