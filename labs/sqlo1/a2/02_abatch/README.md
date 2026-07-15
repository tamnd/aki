# abatch: ApplyBatch sizing and the stack-tax predictions

Milestone A2 lab 02 (spec 2064/sqlo1 docs 02 and 13).
Carries the measured arms for PRED-SQLO1-A2-POINT and PRED-SQLO1-A2-DRAIN (results/sqlo1/a2-predictions.md, filed at #759 before any run).

## Question

Three, one run.
What does a point Get cost through the full sqlo1a stack against the raw prepared-read floor (the stack tax)?
How much does batching buy over one row per transaction (the commit-path check)?
And where is the rows-per-transaction knee once reader p99 is on the other side of the scale?

## Method

The stack under test is engine/sqlo1a exactly as shipped; the lab sets no pragmas on it and reports what the stack costs today.
The cell is the drivershoot reference shape: 200k keys, 128 B values, uniform.

Arms, in run order:

- point-get: uniform Gets through the full stack on a warmed store; the PRED-SQLO1-A2-POINT number, bound at 4416 ns against the recorded 2208 ns drivershoot floor.
- floor-get: the store is closed and its own file is reopened with a raw ncruces connection running the catalog's get statement; the same-run point/floor ratio isolates the stack tax from the pragma posture of the day, in case the absolute floor moved with it.
- drain-single: the same upsert committed one row per ApplyBatch, so every row pays the whole commit path; the PRED-SQLO1-A2-DRAIN denominator.
- drain-solo at each batch size in {256, 1k, 4k, 8k, 16k, 32k}: drained rows/s with no readers.
- pool at each batch size: 4 goroutines hammer Get while the writer drains; the store serializes on one connection behind one mutex, so a bigger batch holds the lock longer and reader p50/p99/max show what the batch size costs the read side.

The knee is the smallest batch size whose solo rate sits near the plateau while pool reader p99 has not yet blown out; that number becomes the ApplyBatch sizing constant.
Both prediction ratios print to stderr at the end of the run; the CSV carries the raw rows.
VmHWM rides along on Linux per the memory bar.

## Run

    ./run.sh            # full run, gate box, before slice 5 bakes pragma constants
    go run . -quick     # smoke
    go test ./...       # tiny-count harness test

## Results

Pending: runs on the gate box after the S0 self-proof frees it, before slice 5.

## Verdict

Pending.
