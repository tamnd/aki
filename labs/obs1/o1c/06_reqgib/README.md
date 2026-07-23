# reqgib: requests per ingested GiB end to end

## Question

Doc 09 section 2 prices the size-triggered regime at about 300 requests per ingested GiB: roughly 128 WAL PUTs from 8 MiB flushes, 128 chain appends, 16 segment PUTs from 64 MiB segments, a rewrite term that does not exist yet at O1c, and noise.
Gate CG1 caps the measurement at 400, with the margin absorbing manifests and retries.
This lab runs the real pipeline against the counting sim bucket and scores PRED-OBS1-O1C-REQGIB.

## Method

One rig, all production components: a WriteLog flushing at the 8 MiB size trigger with the age trigger pushed out of the way, its committer chaining every flush, a Folder cutting 64 MiB segments fed the same record frames the sets emit (the everything-cools shape, so the run pays the full WAL plus segment ledger on the same bytes), and a ManifestPublisher CASing manifests as folds publish.
Four slot groups, so per-group ingest reaches the segment size cut inside a 1 GiB run; the ledger arithmetic itself is group-count-insensitive at the size-triggered regime.
Values are 1000 B, the embedded-band ceiling; the payload is key plus value bytes and the per-GiB rate normalizes by it.
Requests are counted by the sim bucket (PUT, GET, and free classes) with the setup requests subtracted, and the component stats cross-check the breakdown: wal_flushes and chain_commit_batches from the INFO rows, SegmentsPut from the folder, Published from the publisher.

Smoke exposure: the -quick run at shrunken constants (1 MiB flush, 4 MiB segment, 32 MiB payload) measured 90 requests, which scales to about 308 per GiB at production constants; the bands below are calibrated on that and disclosed as such.

## PRED-OBS1-O1C-REQGIB (filed before the scored run)

1. Total requests per ingested GiB: 280 to 340 (smoke-calibrated), comfortably inside gate CG1's 400.
2. Breakdown at 1 GiB: about 137 WAL PUTs (1 GiB payload plus 4 to 5% frame overhead at 1000 B values over 8 MiB flushes), chain appends at most equal to WAL flushes with committer batching only helping, 17 segment PUTs, manifest CASes within 2 of the segment count.
3. Zero GETs at steady state: the write path never reads the bucket, and the only GETs in the run are the chain Follow at setup, which the baseline subtraction removes.
4. Zero PUT retries on the clean sim; the CG1 margin exists for the real world's retries, not the sim's.
5. The doc 09 ledger's shape holds: WAL PUTs and chain appends each within 10% of payload divided by flush size, segments within one of payload divided by segment size plus groups.

## Run

    ./run.sh            # scored run, 1 GiB, writes reqgib.csv
    go run . -quick     # smoke at shrunken constants

## Results

Scored run, 1 GiB at production constants (8 MiB flush, 64 MiB segment, 4 groups, 1000 B values), reqgib.csv:

    payload_bytes,puts,gets,free,total,req_per_gib,wal_flushes,chain_batches,seg_puts,man_published,put_retries
    1073741838,302,0,0,302,302.0,131,131,20,20,0

Scoring the five predictions:

1. HIT. 302.0 requests per GiB, inside the 280 to 340 band and well inside gate CG1's 400.
2. PARTIAL. WAL PUTs came in at 131 against the predicted 137, chain appends landed exactly equal to WAL flushes as the at-most bound allowed, and manifests matched the segment count exactly, but segment PUTs were 20 against the predicted 17: the fold flush cuts one tail segment per group, four of them, where the prediction priced one tail total.
3. HIT. Zero GETs; the write path never touched the bucket for reads.
4. HIT. Zero PUT retries on the clean sim.
5. HIT. WAL PUTs 131 sit 2.3% over payload divided by flush size (128) and chain appends match them, both inside the 10% band; segments hit payload divided by segment size plus groups (16 plus 4 equals 20) exactly.

The one miss is instructive rather than alarming: prediction 2 and prediction 5 disagreed on the tail-segment term and prediction 5's per-group arithmetic was the correct one.
The doc 09 ledger's 300 estimate holds as written, and the group-count term only matters in the last partial segment per group, a constant that vanishes as ingest grows.
