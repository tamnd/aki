# queue: the ends-stay-hot claim re-gated on the landed fold plane

## Question

The queueend lab gated ends-stay-hot before list fold emission existed and disclosed that PRED-OBS1-O2B-QUEUE proper re-gates once the interior actually reaches the bucket.
The list slice landed that emission: every demoted interior chunk folds a position-run projection through the fold tap, so the queue's cold interior now publishes into segments and the ledger.
Does the claim survive the re-gate: with the fold plane provably carrying the queue's own chunks, do the working ends still take exactly zero bucket GETs through steady state and the full drain?

## Method

The queueend rig unchanged where it can be: a durability-booted server (4 shards, 16 MiB arena, 2 MiB resident cap, 5 ms flush, 20 ms fold) on a counting sim bucket over real RESP, a 200k-element backlog of 62 B values, 20 steady rounds of 100 LPUSH then 100 RPOP with ballast every fourth round, then a strict-FIFO drain of the whole backlog, each phase wrapped in a sim GET delta.
One stage changes: priming no longer counts fixed ballast rounds but polls the folder ledger until the queue key's own chunk placements appear in a published segment, the new proof obligation this re-gate exists for.
The cold reader's fetch and error counters close the loop on the bucket side.

## Envelope disclosure

The queue never reads what it folded: pops promote from the local cold region (the pread tier), and the bucket copy exists for takeover and diskless shapes, so zero GETs is the honest steady-state bill on a node that holds its own cold region.
BRPOP stands aside for RPOP as before: the queue is never empty when popped and the conformance conn is synchronous.

## Prediction (PRED-OBS1-O2B-QUEUE, filed before the scored run)

1. The queue key's chunk placements appear in the published fold ledger during priming, at least one non-tombstone place, which is the re-gate's premise: fold emission for the demoted interior is live end to end.
2. backlog_build takes exactly 0 bucket GETs with cold_region_bytes in the queueend band, 9.0 to 13.6 MB of the 12.4 MB payload.
3. steady takes exactly 0 bucket GETs across all 4000 end ops with the fold pipeline live (segments_folded at least 1000 over the whole run).
4. drain takes exactly 0 bucket GETs across the roughly 200k pops, every value byte-exact in FIFO order, EXISTS 0 at the end.
5. The cold reader finishes with 0 fetches and 0 errors or unresolved reads: the fold plane carried the interior without the serve path ever needing it.

Kill line: any bucket GET in any phase, a FIFO mismatch, or the queue key never reaching the ledger kills the ends-stay-hot claim as landed and the exit gate stops until it is understood.

## Calibration disclosure

The bands were derived from the queueend lab's scored run before any configuration of this rig executed; after this file was written the small smoke ran once inside the repo race gate and confirmed harness mechanics (zero GETs, ledger reached), and the scored run below is a fresh full-size execution.

## Run

    ./run.sh

## Results

Pending the scored run.

## Verdict

Pending the scored run.
