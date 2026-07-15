# apragma: pragma sweep on the frozen driver

Milestone A2 lab 01 (spec 2064/sqlo1 doc 02 sections 5-6).

## Question

Which page_size, cache_size, and checkpoint cadence does slice 5 bake into sqlo1a.Open?
The drivershoot verdict froze ncruces at page_size 8192 on datasets the page cache swallowed whole, and left a note that 16384 has to earn its way back on datasets that beat the cache.
This lab is that rematch, on the production workload shape, with the two knobs drivershoot never swept.

## Method

One binary, one configuration per run, CSV rows to stdout; run.sh sweeps page {4096, 8192, 16384} x cache_size {8 MiB, 32 MiB, 128 MiB} x checkpoint cadence {1, 8, 64 drain batches} into apragma.csv.
The default dataset is 2M keys at 128 B values, roughly 430 MB of file, so every swept cache budget loses the whole-file game and the split has to matter.

The store shape is the doc 02 kv table plus the meta row, and the drain transaction mirrors ApplyBatch: batched upserts with the high-water write committed inside.
Pragmas follow the doc 02 section 5 posture: WAL, synchronous=OFF, wal_autocheckpoint=0 with explicit wal_checkpoint(TRUNCATE) on the swept cadence, temp_store=MEMORY, mmap_size=0, auto_vacuum=INCREMENTAL.

Arms per configuration:

- load: drain-shaped fill of the whole keyspace, checkpointing on the swept cadence; reports rows/s plus file and WAL size after the final checkpoint.
- get-zipf: zipf(1.1) point reads on the writer connection after a short warmup; the hot set fits the page cache even though the file does not, so this is where cache_size earns its RAM.
- get-cold: reopen (drops the SQLite cache, not the OS file cache; read the cross-cell ratios, not the absolute numbers) then uniform point reads across the whole keyspace.
- pool: 4 readonly reader connections hammering uniform point gets while the writer drains batches and checkpoints on the cadence; readers sample every 8th latency for p50/p99/max, the writer records the peak WAL size between checkpoints and times every checkpoint call (pool-ckpt row: mean and worst stall).

VmHWM rides along on the pool-read row (Linux only) because the memory bar is part of every sqlo1 verdict.

## Run

    ./run.sh            # full sweep, 27 configurations, gate box
    go run . -quick     # smoke
    go test ./...       # tiny-count harness test

## Results

Pending: runs on the gate box after the S0 self-proof frees it.

## Verdict

Pending.
