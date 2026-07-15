# hseg: the hash segment size

Milestone T2 lab 01 (spec 2064/sqlo1 doc 06 section 2).

## Question

What does seg_max, the encoded-segment split threshold, cost at 2016, 4032, and 8064 bytes?
T2 slice 2 bakes the constant, and rule W4 makes it a bandwidth knob: every HSET's WAL frame is a full segment post-image, so doubling the segment doubles the steady-state WAL bytes per mutating command, while halving it doubles the segment count, the fence length, and the rows a drain writes for the same churn.
PRED-SQLO1-T2-WAL is priced here too: under zipfian field reuse the WAL bytes per HSET against the post-image size say whether the doc 14 wal-delta lever gets a ticket.

## Method

The model is the doc 06 shape resident: fields partitioned by a 64-bit field hash, the fence found by binary search, entries sorted by fh inside each segment, splits at the entry-median fh when the encoded size crosses seg_max, count and fence kept exact in the root per rule W1.
Segments drain as encoded blobs into a (k, segid) row table in drain-shaped transactions on the engine's 8 MiB dirty threshold; Track A proper maps hashes to helem rows instead, but seg_max belongs to the shared segment machinery, so the lab prices segment records on the same SQLite substrate the other sqlo1 labs use.
The WAL column is modeled arithmetic under rules W2 and W4 (segment post-image per HSET, root post-image only on a fence change) because the aki WAL is not SQLite's; write amplification is drained bytes over logical field-plus-value bytes.
Preload builds every hash through the same HSET path so splits happen the way slice 2 will do them, then the measured mix overwrites preloaded fields at the given HSET percentage, per-op cost timed into its class.
An oracle test at seg_max 512 pins the model through the SQL readback path: fence partition, sorted in-range entries, under-threshold rows, honest fill classes, exact root count, and the exact reference map.

Read the sweep as: the hset and hget rows are resident-op costs (fence depth shows up here), wal_b_per_op is the bandwidth bill, the flush row is the drain IO, and WA closes the loop on disk traffic.
The crossover the verdict needs is where growing wal_b_per_op stops buying fewer, larger drain rows.

## Run

    ./run.sh            # {2016, 4032, 8064} x {small, med, large} x setpct {10, 50, 90}, gate box
    go run . -quick     # smoke
    go test ./...       # smoke plus the segment oracle

## Results

Pending: runs on the gate box after the A2 queue frees it.

## Verdict

Pending.
