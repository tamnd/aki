# batchdrain: drain window and batch cap over a real store

Milestone A2 lab 03 (spec 2064/sqlo1 doc 04 section 7, doc 13 lab table).
Re-verdicted on Track B at B3.

## Question

The shared queue's two constants were set from the doc table against the placeholder store: drain when dirty bytes cross 8 MiB, at most 1024 ops per cycle.
What do they cost against sqlo1a, where a cycle is a real SQL transaction holding the same lock point reads need, and how do window and cap trade drain lag against writer and reader throughput?

## Method

The drainer is package-internal to engine/sqlo1 on purpose, so the harness mirrors its policy instead of importing it: a coalescing first-dirtied-first queue, one entry per dirty key however often it is rewritten, drained oldest-first up to the cap when dirty bytes cross the threshold, one DrainBatch per cycle with the sequence advancing.
A pinned unit test keeps the mirrored policy honest.
The store is engine/sqlo1a exactly as shipped.

One configuration per run; run.sh sweeps threshold {1, 4, 8, 16, 32 MiB} x cap {256, 1024, 4096} x write distribution {uniform, zipf} (zipf is where coalescing pays; uniform is the worst case where every write is a new dirty record).
The keyspace (500k keys, 128 B) is preloaded so reader misses are store reads, not not-founds.

The writer loop runs its own drain cycles, matching the owner-loop shape where drain time is stolen from write time, so the write row's rate already pays for draining.
Four readers run uniform point gets against the store throughout, sampling latency.

Rows per run (mb_a and mb_b are per-workload):

- write: accepted writes/s; mb_a is peak dirty MiB (how far past the threshold the window overshoots).
- drain: drained rows/s; mb_a is mean batch fill in rows (how full the cap runs), mb_b is peak WAL MiB.
- lag: per-batch oldest-entry drain lag, p50/p99/max in the latency columns; ops is the cycle count.
- pool-read: reader ops and p50/p99/max, with VmHWM on Linux.

## Run

    ./run.sh            # full sweep, 30 configurations, gate box
    go run . -quick     # smoke
    go test ./...       # tiny-count harness test plus the policy pin

## Results

Pending: runs on the gate box after the S0 self-proof frees it.

## Verdict

Pending.
