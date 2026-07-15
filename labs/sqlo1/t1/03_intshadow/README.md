# intshadow: the INCR int64 shadow, on and off

Milestone T1 lab 03 (spec 2064/sqlo1 doc 05 section 2).

## Question

Is the header-cached int64 shadow worth its invalidation rules?
Doc 05 keeps int-shaped values as their canonical decimal string and lets INCR add to a cached int64 instead of parse, add, format on every op; T1 slice 4 bakes that design, and if the measured delta is small, slice 4 ships without the shadow.
Since B3 the suite runs on both backends: -store a is the SQLite path below, -store b the same counters over sqlo1b records, and the verdict reads from the arm that will actually carry the shadow.

## Method

Both arms keep the store identical: counters live as decimal strings in kv and drain as decimal strings in drain-shaped transactions, so the arms differ only in what a resident INCR costs.
The shadow arm adds to an int64 and pays one format per dirty key per flush; the noshadow arm reparses and reformats the resident decimal bytes on every op.

Hot phase: every touched key resident (64k keys warmed first), 2M INCRs timed in 1024-op blocks because a hot INCR is tens of nanoseconds and per-op clock reads would drown it; flush stalls land in the block tails and in the flush row.
Cold phase: a 4096-entry resident cap over a 1M keyspace with random clean eviction and dirty entries pinned (the sqlocache shape), so most INCRs pay the store read and parse before the add; per-op timing at microsecond scale.
An oracle test drives both shadow arms on both backends over a small capped model against a reference map and requires the drained store to hold the exact reference counters.
On the b arm a counter is a plain record under its user key holding the decimal bytes, a flush is one DrainBatch with the model's sequence as the high-water mark, and the checkpoint cadence calls the store's checkpoint.

Read the sweep as: the hot-incr ns/op delta between arms is the shadow's entire value; the cold-incr rows are the control where the read should drown the parse.
A hot delta under a few nanoseconds per op says the parse is cheap enough that the shadow and its invalidation-by-byte-writer rules are not worth carrying; a double-digit delta says slice 4 keeps them.
Zipf is the favorable case (few keys, resident, format amortized over coalesced INCRs); uniform spreads the format cost at drain.

## Run

    ./run.sh            # both backends x {shadow, noshadow} x {zipf, uniform}, gate box
    go run . -quick     # smoke (add -store b for the Track B arm)
    go test ./...       # both-arm smoke plus the counters oracle, both backends

## Results

Pending: runs on the gate box after the A2 queue frees it.

## Verdict

Pending.
