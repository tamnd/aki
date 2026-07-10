# Lab 13: hot-set residency policy for the larger-than-memory regime

The question: with the resident cap acting as a one-way valve, the resident set is the fill-order prefix, not the working set, and a zipfian workload pays a synchronous log read per GET for the same hot keys forever (issue #542, the LTM gate cell).
The residency slice adds read-path promotion and SIEVE-style clock-hand demotion at the owner boundaries.
Which promotion policy earns its keep: promote on first touch, the plain two-touch doorkeeper, or the sampled doorkeeper where only 1-in-den first touches set the mark?

## Method

In-process, no server, no wire, one cell per invocation so maxrss is honest.
2M keys of 1032B values, about 2.1GiB of value bytes, the separated-band shape of the f3-ltm-strings scenario.
Resident cap swept at 512MiB (a quarter of the dataset) and 1GiB (half).
Access swept between zipfian s=0.99 (YCSB constant, ranks scrambled over the keyspace) and uniform, the adversary where no working set exists and the right policy promotes almost nothing.
The loop emulates the shard worker: batches of 1024 GETs, then the between-drains demote-or-compact check, verbatim.
Warmup 2M GETs, measured window 6M GETs.

Run one cell:

    go run ./labs/f3/m0/13_ltm_residency -dist zipfian -cap 512 -mode two -dkden 8

## Results (darwin arm64, 2026-07-10)

dk is the doorkeeper sampling denominator (`-dkden`); the shipped default is 8.

| dist    | cap     | mode  | dk | gets/s | log reads/get | hit    | promotes | fill    | maxrss  |
|---------|---------|-------|----|--------|---------------|--------|----------|---------|---------|
| zipfian | 512MiB  | off   | -  | 0.16M  | 0.750         | 25.0%  | 0        | 585MiB  | 703MiB  |
| zipfian | 512MiB  | first | -  | 0.07M  | 0.173         | 82.7%  | 1091067  | 511MiB  | 716MiB  |
| zipfian | 512MiB  | two   | 1  | 0.13M  | 0.158         | 84.2%  | 223826   | 510MiB  | 739MiB  |
| zipfian | 512MiB  | two   | 8  | 0.62M  | 0.216         | 78.4%  | 96821    | 502MiB  | 739MiB  |
| zipfian | 1024MiB | off   | -  | 0.75M  | 0.506         | 49.4%  | 0        | 1074MiB | 1192MiB |
| zipfian | 1024MiB | first | -  | 0.16M  | 0.100         | 90.0%  | 627839   | 1011MiB | 1315MiB |
| zipfian | 1024MiB | two   | 1  | 0.90M  | 0.112         | 88.8%  | 241484   | 1020MiB | 1278MiB |
| zipfian | 1024MiB | two   | 8  | 0.52M  | 0.159         | 84.1%  | 75320    | 1019MiB | 1315MiB |
| uniform | 512MiB  | off   | -  | 1.03M  | 0.763         | 23.7%  | 0        | 585MiB  | 703MiB  |
| uniform | 512MiB  | first | -  | 0.02M  | 0.830         | 17.1%  | 5218465  | 506MiB  | 739MiB  |
| uniform | 512MiB  | two   | 1  | 0.13M  | 0.830         | 17.0%  | 1011388  | 505MiB  | 739MiB  |
| uniform | 512MiB  | two   | 8  | 0.59M  | 0.830         | 17.0%  | 266090   | 500MiB  | 740MiB  |
| uniform | 1024MiB | off   | -  | 1.32M  | 0.526         | 47.4%  | 0        | 1074MiB | 1192MiB |
| uniform | 1024MiB | first | -  | 0.03M  | 0.612         | 38.8%  | 3852423  | 1010MiB | 1315MiB |
| uniform | 1024MiB | two   | 1  | 0.20M  | 0.612         | 38.8%  | 1023962  | 1015MiB | 1315MiB |
| uniform | 1024MiB | two   | 8  | 0.67M  | 0.613         | 38.7%  | 266174   | 1027MiB | 1315MiB |

Demotions track promotions within a fraction of a percent in every cell (the demote pass reclaims exactly what promotion admitted), so the column is omitted.

Two caveats on reading this box's numbers.
First, the whole 2.1GiB vlog fits in the macOS page cache, so a "log read" here is a syscall and a copy, not a disk IO; the off rows post high gets/s because most of their misses are served from RAM pretending to be disk, and the gets/s column across cells mixes churn cost with cache luck.
On the Linux gate box the vlog reads are fadvised away (DONTNEED per read) and the dataset is sized past RAM, so a miss costs a real read and the reads/get column is the one that predicts throughput.
Second, the fill exceeds the cap by the record headers and keys (about 73MiB for 2M keys, always resident by the doc 09 rule) plus dead bytes awaiting the boundary compaction; maxrss tracks the fill plus the Go runtime, and stays the same order as the cap in every cell, against a 4GiB arena reservation.

## What the sweep says

Zipfian is the case the slice exists for, and residency wins it outright: at a quarter of the dataset resident, the sampled doorkeeper cuts log reads per GET from 0.750 to 0.216, a 3.5x cut in disk traffic, and holds a 78% hit ratio the fill-order prefix cannot approach (25%).
The unsampled doorkeeper reaches 84% but pays 2.3x the churn for the last six points; sampling keeps most of the hit and sheds most of the movement.

First-touch is rejected: it buys roughly the unsampled hit ratio and pays 4-5x its churn, collapsing under uniform (5.2M promotions in a 6.3M-GET window, 0.02M gets/s).

Uniform is why the doorkeeper is sampled.
The mark decay (one hand revolution over 2M keys) is far slower than the uniform re-touch interval, so marks saturate and plain two-touch degenerates to first-touch: 0.16 promotions per GET, all churn, no hit-ratio gain over off (17% against 23.7%; the gap is the live charge held at the low-water mark).
The wire scenario showed the bill: f3-ltm-strings runs uniform by protocol, and the unsampled branch ran its GET row 4.7x slower than no-residency main.
Sampling at 1-in-8 cuts uniform promotions 3.8x with an identical hit ratio, and the wire row recovers past main.
A real ghost window (epoch-stamped marks) is the doc 16 shape of this and stays deferred.

The admission gate found a deadlock during this sweep, worth recording: with spillNow charging the budget against the arena fill, the cap=1GiB cells parked the fill just over the cap on dead bytes too thin for any segment to be a compaction victim, and promotion was blocked forever (zero promotes, hit frozen at the fill prefix).
Admission now charges the live bytes; dead bytes are the compactor's debt, scheduled by the fill-based ResidentOver trigger at the same boundaries.

## Verdict (frozen)

- Promotion policy: two-touch doorkeeper sampled at `residDoorkeeperDen = 8`, the shipped default. First-touch rejected (4-5x the churn, throughput collapse under uniform); unsampled two-touch rejected (mark saturation makes it first-touch under uniform, and the uniform wire row runs 4.7x slower than main).
- Demotion: SIEVE second chance off the clock hand, `residSlackDen = 8` (demote to cap minus an eighth), `demotePassBudget = 8MiB`, `demoteScanBudget = 64Ki` slots, `demoteFlushBytes = 1MiB`.
- Admission (`spillNow`) charges live bytes, not fill; `ResidentOver` stays fill-based and schedules the reclaim compaction.
