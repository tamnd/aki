# Lab: arena backing vs GC pacing vs RSS

Spec 2064/f3, M0 gate follow-up, lab 20, issue #542.

## The question

The M10 reactor campaign read 3.2-4.2x rival RSS on every 64B gate cell, on every driver arm.
The per-rep meta files rule out the obvious suspect: the arena's MADV_DONTNEED verifiably returns about 93MB at each flush, and the 64B record layout is efficient (about 96B per key, under redis's per-key cost).
What climbs is the post-flush residual, 163MB then 310MB then 396MB across the three reps of one cell, while redis returns to 14MB after every flush.
The hypothesis: a make([]byte) arena counts its full reservation as live Go heap, the gate config (4 shards x 512MiB) makes that 2GiB, the GC pacing goal lands past 4GiB, and the scavenger's retention target inflates with it.
Transient garbage, per-connection buffers, index tables rebuilt after Reset, per-op allocs, is then never collected and never returned, and reads as permanent RSS.
Does mapping the arena anonymously (heap holds only the substrate, doc 04 section 6.2's ledger posture) restore default GC pacing and flatten the residual?

## Method

`go run .` emulates the gate cell in process: 4 stores with 512MiB arenas (1024MiB under -val 1024), 1M keys of 16B key + 64B value spread across them, three reps of fill plus conn-buffer churn (8 cycles of 512 connections' 64KiB read + 64KiB reply buffers, touched and dropped), then Reset on every store.
Each phase boundary prints VmRSS and VmHWM from /proc/self/status, HeapAlloc, HeapSys, HeapReleased, NumGC, and the stores' ledger.
A/B is by commit: build at the parent of the arena-map change (heap-backed arena) and at the change (mapped arena), same box, same run protocol.
The server-side confirmation is the gate matrix itself, results/f3/m0-rss.md.

## Predictions (filed before the box run)

Heap-backed arm: HeapSys within a segment of 2GiB from launch, NumGC in single digits for the whole run, post-flush VmRSS climbing by hundreds of MB per rep with no recovery, VmHWM past 1.5GiB.
Mapped arm: HeapSys under 300MB, NumGC in the dozens, post-flush VmRSS back within tens of MB of the previous rep's floor, VmHWM under 400MB.
On the server gate cells: 64B P16/512 VmRSS lands near 230-280MB against redis's ~138MB, so the ratio drops from 3.2-4.2x to about 1.7-2.0x, and the reactor's P1/512 excess (+193MB owned buffers plus retained garbage) drops to near the goroutine driver's footprint with the leasing change stacked on top.

## Results

Gate box (GamingPC WSL2, 32-thread, 56GiB), binaries cross-built at 49ab548 (A, heap-backed arena) and at the arena-map commit (B, mapped arena), taskset 0-7, one run per arm per shape, 2026-07-11.

64B (4 x 512MiB arenas), figures in MB:

| phase | A rss | A heapAlloc | A gc | B rss | B heapAlloc | B gc |
|---|---|---|---|---|---|---|
| launch | 5.4 | 2048.2 | 2 | 2.6 | 0.2 | 0 |
| rep0 post-fill | 609.1 | 2588.6 | 3 | 206.2 | 139.1 | 19 |
| rep0 post-flush | 548.5 | 2588.7 | 3 | 145.5 | 139.1 | 19 |
| rep1 post-fill | 1152.0 | 3129.0 | 3 | 175.0 | 75.1 | 37 |
| rep1 post-flush | 1091.3 | 3129.0 | 3 | 114.4 | 75.1 | 37 |
| rep2 post-fill | 1695.1 | 3669.4 | 3 | 192.7 | 75.1 | 50 |
| rep2 post-flush | 1634.3 | 3669.5 | 3 | 131.9 | 75.1 | 50 |

The A arm's heap starts at the 2GiB reservation, the collector runs 3 times in the whole run because the pacing goal sits past 4GiB, and every rep's garbage (the ~512MB of conn-buffer churn plus the rebuilt index) lands on the heap and stays: the post-flush residual climbs 548 to 1091 to 1634MB, the exact shape of the campaign's per-cell climb, scaled by this lab's heavier churn.
The B arm's heap holds only the substrate (75-139MB of index at fill), the collector runs 19 then 37 then 50 times under default pacing, and the post-flush residual is flat at 114-146MB.
The 1KiB shape (-val 1024, 4 x 1024MiB arenas) tells the same story wider: A launches with a 4GiB heap, collects 3 times, and the residual climbs 551 to 1094 to 1637MB; B fills to 786-813MB (the values really are resident, VmHWM 820MB), collects 17 then 35 then 54 times, and flushes back flat to 114-146MB.
One prediction missed: the A arm's launch RSS is 5.4MB, not eagerly resident gigabytes, because the runtime backs a giant make with demand-zero pages too; the reservation costs pacing, not launch pages, which is the whole point.

## Verdict (frozen)

The 64B RSS breach was GC pacing, not arena pages: the heap-accounted reservation multiplied the pacing goal and the scavenger retention target, so per-run garbage became permanent RSS while the arena's own MADV_DONTNEED kept working.
The fix maps the arena backing anonymously on unix builds (engine/f3/store/arena_map_unix.go), heap fallback elsewhere and on mmap failure, unmapped by a finalizer when the store goes away.
This lab isolates the mechanism, not the standing bar: it proves the mapped arm flattens the post-flush residual and returns to substrate, which is a large step down in RSS but not the whole distance.
The standing memory bar was tightened after this lab to aki resident and peak RSS at or below one rival's for the same dataset (not the old under-2x ceiling), and the server-side matrix judges the after-state against it honestly: the fix clears the old ceiling everywhere and passes the new bar only where the values dominate (1KiB SET), still exceeding rivals on 64B where the arena's dead-record slack and per-shard reservation outweigh a value that small.
The server-side gate numbers, the same-data bytes-per-key decomposition, and the named follow-up levers are in results/f3/m0-rss.md.
