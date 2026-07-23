# queueend

## Question

Doc 08 section 6 claims queue workloads never touch the bucket: chunks are contiguous position runs, the fold selector prefers interior runs, and the ends stay hot by construction.
The lab spec asks for sustained push and pop through fold pressure and gates the ends-stay-hot claim at cold reads on the working end ~0.
Does the landed engine deliver that end to end: a deep list backlog demoted below the resident cap, sustained end ops over RESP taking zero bucket GETs, and an exact FIFO drain back through every demoted interior chunk?

## Method

A durability-booted server (4 shards, 16 MiB arena, 2 MiB resident cap, 5 ms flush, 20 ms fold) on a counting sim bucket, exercised over real RESP.
Four phases, each wrapped in a sim GET delta:

- backlog_build: LPUSH a 200k-element backlog (62 B values) into one queue key. The interior must demote under the cap during the build itself; with nothing else written yet, every cold-region byte after this phase is the queue's own interior, read from the new `cold_region_bytes` INFO row.
- priming: 32 rounds of 4k string ballast SETs plus fold settle, so the fold pipeline is provably live before the claim is scored (the pipeline needs roughly 100k keys before the first segment cuts).
- steady: 20 rounds of 100 LPUSH then 100 RPOP at the working ends, ballast plus settle every fourth round keeping demote and fold pressure on throughout.
- drain: RPOP the entire backlog, verifying every value byte-exact in strict FIFO order, then EXISTS must answer 0.

The cold reader's own fetch and error counters close the loop on the bucket side.

## Envelope disclosure

Fold emission for lists does not exist yet; it lands with the O2b list slice.
The list emission PR in O1b was write-log durability op frames, so a list key cannot appear in the fold ledger today and the interior cold tier is the local cold region (pread on demoted chunk slabs), not the bucket.
This lab therefore pins the engine contract the fold selector must preserve: interior-only demotion with a hot margin at each end, pop-promote when a cold chunk is exposed, and exact FIFO through demoted chunks, all at zero bucket GETs.
PRED-OBS1-O2B-QUEUE proper re-gates on the landed fold plane later in the milestone.
Two smaller disclosures: the workload uses RPOP rather than BRPOP (the queue is never empty when popped, so blocking adds nothing and the conformance conn is synchronous), and this PR adds the `cold_records`/`cold_region_bytes` INFO rows the lab reads (the ring slabs sit outside the arena, so no existing counter sees collection demotes).

## Prediction (PRED-OBS1-O2B-QUEUEEND)

Filed before the scored run.

1. backlog_build takes exactly 0 bucket GETs, and cold_region_bytes lands in 9.0 to 13.6 MB (payload is 200k x 62 B = 12.4 MB; the resident cap plus the per-end hot margin keeps roughly 2 MB warm, so the cold share is about 83 percent and up).
2. steady takes exactly 0 bucket GETs across all 4000 end ops, with the fold pipeline live (segments_folded at least 1000 over the whole run).
3. drain takes exactly 0 bucket GETs across roughly 200k pops, every value exact in FIFO order, EXISTS 0 at the end.
4. The cold reader finishes with 0 fetches and 0 errors or unresolved reads.

Kill line: any bucket GET in any phase, a single FIFO mismatch, or backlog cold bytes under half the payload kills the ends-stay-hot claim as landed.

## Calibration disclosure

A quick 60k-backlog configuration shaped the harness before this prediction was filed: it found the fold-pipeline knee (no segment cuts until about 100k ballast SETs, hence the fixed 32-round priming stage), confirmed the cold-bytes attribution (1,815,488 cold bytes against a 3.72 MB payload minus the roughly 1.9 MB kept resident), and confirmed all phases at zero bucket GETs at that scale.
The 200k scored run had not been executed when the bands above were set.

## Run

```
./run.sh
```

## Results

(scored run pending)

## Verdict

(pending)
