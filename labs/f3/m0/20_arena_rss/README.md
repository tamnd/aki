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

Pending the box run.

## Verdict

Pending the box run.
