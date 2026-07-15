# batchdrain-b: drain window and batch cap over the B-format store

Milestone B3 lab 01 (spec 2064/sqlo1 doc 04 section 7, doc 13 lab table).
Re-verdict of the A2 batchdrain lab: the constants do not carry silently.

## Question

The shared queue's two constants were set from the doc table: drain when dirty bytes cross 8 MiB, at most 1024 ops per cycle.
A Track B cycle is a WAL append run plus a RAM apply into group buffers and index chunks, not a SQL transaction, so both the cost of a cycle and the lock hold point reads pay for are different animals than at A2.
How do window and cap trade drain lag against writer and reader throughput on sqlo1b, and where do checkpoint stalls land?

## Method

Same harness shape as labs/sqlo1/a2/03_batchdrain, so rows compare directly: a mirrored coalescing first-dirtied-first queue (pinned by a unit test), the store exactly as shipped, the writer loop running its own drain cycles so drain time is stolen from write time, four uniform readers sampling latency throughout.

Two B-side additions:

- The checkpoint cadence runs inline in the writer loop through the shipped CheckpointPolicy, bytes rung only (the 60 s interval rung never fires in a lab-length run). Default trigger 256 MiB of WAL growth, -ckpt to sweep it.
- A ckpt row reports the stalls: count, p50/p99/max checkpoint duration, WAL MiB at the trigger peak (mb_a), data file MiB at the end (mb_b).

One configuration per run; run.sh sweeps threshold {1, 4, 8, 16, 32 MiB} x cap {256, 1024, 4096} x write distribution {uniform, zipf}.
The keyspace (500k keys, 128 B) is preloaded and checkpointed before the measured phase, so readers start against a cold-indexed base.

Rows per run (mb_a and mb_b are per-workload):

- write: accepted writes/s; mb_a is peak dirty MiB (window overshoot).
- drain: drained rows/s; mb_a is mean batch fill in rows, mb_b is peak WAL MiB.
- lag: per-batch oldest-entry drain lag, p50/p99/max; ops is the cycle count.
- ckpt: checkpoint count and duration percentiles; mb_a is peak WAL MiB at trigger, mb_b is final data file MiB.
- pool-read: reader ops and p50/p99/max, with VmHWM on Linux.

## Run

    ./run.sh            # full sweep, 30 configurations, gate box
    go run . -quick     # smoke
    go test ./...       # tiny-count harness test plus the policy pin

## Results

Pending: runs on the gate box.

## Verdict

Pending.
Predictions in results/sqlo1/b3-predictions.md, filed before the verdict run.
