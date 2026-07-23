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

Pending the scored run.
