# Drivershoot verdict: ncruces at 8 KiB pages

Milestone A1 (tamnd/aki#712), slice 2; spec 2064/sqlo1 doc 02 section 2.
Lab: labs/sqlo1/a1/01_drivershoot at 671ed9f; raw sweep beside this note under results/sqlo1/drivershoot/sweep.csv.
Box: the gate box (GamingPC, WSL2, 32 cpus, go 1.26.0), 2026-07-15, database files on the ext4 root disk, TMPDIR pinned off /tmp.
Sweep: three drivers x page 4/8/16 KiB x value 16/128/512 B x uniform/zipf, 200k keys, 500k point ops per arm, 4096-row drain transactions, 4-reader pool for 2 s beside the drain writer.

## Verdict

sqlo1a links ncruces (github.com/ncruces/go-sqlite3) at page_size 8192: with four readers beside the drain writer it holds 96 percent of its solo drain rate and serves 1.14M point reads/s, while zombiezen and modernc, which share the modernc SQLite core, both collapse to about one connection's worth of total throughput.

## The decisive table

Reference cell, page 8192, 128 B values, uniform keys:

| arm | zombiezen | ncruces | modernc |
|---|---|---|---|
| step (prepared bind-step-reset, no table) | 104 ns | 143 ns | 826 ns |
| get-hot (single conn) | 2050 ns, 488k/s | 2208 ns, 453k/s | 3138 ns, 319k/s |
| drain solo (rows/s at 4096-row txns) | 658k/s | 449k/s | 479k/s |
| pool: reads/s, 4 readers beside writer | 356k/s | 1141k/s | 129k/s |
| pool: drain rows/s beside 4 readers | 74k/s | 429k/s | 45k/s |

zombiezen wins every single-connection arm by 7 to 32 percent, and it does not matter.
The production shape of sqlo1 is exactly the pool arm: one drain writer committing batch transactions while a read pool serves point GETs.
Under that shape zombiezen keeps 11 percent of its solo drain rate and its four readers together deliver less than one connection's read rate.
ncruces keeps 96 percent of its drain rate and scales reads 2.5x past a single connection.
The same collapse hits modernc harder, and both sit on the same transpiled core and Go libc runtime; ncruces isolates each connection in its own wazero instance, which is evidently what survives concurrency.
The ratios hold at every value size and both distributions: at 512 B values the pool spread is 868k vs 208k vs 83k reads/s.

## Page size

8192 over 4096 buys 5 percent on load and 3 percent on drain and gives up nothing measurable elsewhere.
16384 adds at most another 3 percent on bulk arms, loses the 512 B get-hot arm, and doubles the worst-case bytes a truly cold point read or a WAL frame must carry; this lab's cold arm cannot see that cost (the OS file cache stays warm across the reopen), so the exposure is priced conservatively.
8192 is the freeze; A2's apragma lab re-sweeps it on the real store with datasets that beat the page cache, which is where 16384 would have to earn its way back.

## A2 floor numbers (exit-gate record)

From the reference cell, ncruces at 8192, the numbers the A2 stack is measured against:

- Prepared point read, cache hot, single connection: 2208 ns, 453k/s (PRED-SQLO1-A2-POINT allows at most 2x on top of this).
- Drain transaction rate, 4096-row upsert batches: 2229 ns/row, 449k rows/s solo; 429k rows/s with the read pool live.
- Pool reads beside the drain writer: 1.14M/s at 128 B, 1.31M/s at 16 B, 868k/s at 512 B.
- SQL tax floor (step): 143 ns per prepared statement round trip.

## Losing numbers

- zombiezen: fastest single connection everywhere (step 104 ns, get-hot 2050 ns, solo drain 658k rows/s), but pool reads 356k/s and pool drain 74k rows/s at the reference cell. If sqlo1a ever runs a strictly single-connection store, this verdict flips.
- modernc via database/sql: step 826 ns (5.8x the ncruces tax), get-hot 3138 ns, pool reads 129k/s, pool drain 45k rows/s, degrading further with page size (26k rows/s pool drain at 16 KiB). The compatibility floor, as doc 02 expected.

## Which published bench our shape agreed with

Doc 02 section 2 cites two disagreeing 2026 results.
Our pool arm agrees with the ncruces suite (concurrent prepared reads about 3x over the modernc-core drivers) and our single-connection arms agree with cvilsmeier's go-sqlite-bench Many table (zombiezen fastest on repeated small queries, ncruces second, modernc last).
cvilsmeier's Concurrent table, where zombiezen wins, is N goroutines each scanning a full table on a read-only database: no writer, no point reads, so its statement mix never touches the cross-connection serialization our pool arm exposes.
Both published results are reproduced by our data once the shape is controlled for, which is the outcome that lets us trust the lab.
