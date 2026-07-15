# Lab 01: SQLite driver shootout

Part of milestone A1 (tamnd/aki#712, spec 2064/sqlo1 doc 02 section 2).
This is the lab the driver freeze depends on: sqlo1a links exactly one of modernc, zombiezen, or ncruces, and the published 2026 benchmarks disagree about which wins because they measure shapes that are not ours.

## Question

Which pure-Go SQLite driver is fastest on the aki-shaped workload, at which page size, and how much SQL tax remains after preparing statements (the rkv bound says most of SQLite-as-KV cost sits above the B-tree)?

## Method

One binary per driver behind build tags (drv_modernc, drv_zombiezen, drv_ncruces); a no-tag stub keeps plain builds green.
Each binary runs the same suite against the doc 02 section 4 schema (kv and helem, WITHOUT ROWID) under the doc 02 section 5 pragmas: load (drain-shaped batch transactions into a cold file), step (prepared bind-step-reset with no table behind it, the SQL tax floor), get-hot and get-cold point reads, single-statement autocommit sets, batch drains, the helem composite-PK shape, and a read pool of separate connections beside a draining writer.
The adapters speak each driver's native API; modernc goes through database/sql because that is its documented posture and the compatibility floor the doc names.
get reports value length so each adapter may use its cheapest column access; where an API forces a copy, that cost is the driver's to pay.
The cold arm reopens the file, which drops the SQLite page cache but not the OS file cache; that treats all drivers equally and flatters absolute numbers, so cross-driver ratios are the read, not the ops/s.
run.sh sweeps page size 4/8/16 KiB, value size 16/128/512 B, and uniform vs zipfian keys into one CSV.

Two harness findings that are results in their own right, both found when the modernc read pool collapsed to single reads per second:

1. auto_vacuum and page_size are creation-time pragmas and must run only on the connection that creates the file.
Riding them on the modernc DSN replays them on every pooled connection, and each new read connection then queues behind the writer for a lock: 1 read per second beside a 670k rows/s writer, against 540k reads per second once split out.
2. wal_autocheckpoint = 0 (the doc 02 posture) is only safe with the drain-cadence checkpointing the doc pairs it with; the harness checkpoints after load and drain phases and every 8 pool batches, and readers on all three drivers stay healthy.

`go run -tags drv_zombiezen . -quick` smokes one driver; `./run.sh out.csv` is the full sweep.
The tests run the whole suite at tiny counts per tag and assert the created file header carries the swept page size, so a pragma-plumbing regression cannot silently turn every page-size arm into the default.

## Results

Numbers land with the verdict note (results/sqlo1/, milestone A1 slice 2) from the gate box; the local quick run only proves the harness.
Apple M4 smoke ratios for orientation, 4 KiB pages, 128 B values, uniform: zombiezen step 88 ns vs ncruces 110 ns vs modernc 586 ns; get-hot zombiezen 1.7 us vs ncruces 1.8 us vs modernc 2.4 us; pool reads zombiezen 986k/s vs ncruces 642k/s vs modernc 442k/s beside their writers.

## Verdict

Frozen by the A1 slice 2 note, not here.
