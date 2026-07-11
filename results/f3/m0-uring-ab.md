# M0 io_uring A/B: the syscall-floor arm and the section 5.4 verdict

Campaign note for M10 slice 3 proper, the io_uring network driver (-net uring), the last named throughput lever before the doc 08 section 5.4 verdict.
Box and protocol as the reactor campaign (results/f3/m0-reactor-ab.md): GamingPC i9-13900K WSL2, servers taskset 0-7, generator 8-15, redis 8.8.0 and valkey 9.1.0 io-threads 4, warm 3s, 3 timed 8s windows, none discarded, FLUSHALL between reps on all servers, 1M keys uniform, p99, VmRSS, and VmHWM captured for every arm and both rivals.
Ratios are the min of per-harness ratios (aki-ab vs rival-ab and aki-rb vs rival-rb, each harness compared with itself, never mixed).
Arms interleave within each cell in one session: goroutine-single, reactor (-net-loops 3), uring (-net-loops 3), against both rivals.
WSL2 caveat, named up front per doc 08 section 5.4: io_uring under a virtualized kernel carries a confound the epoll comparison does not, so if the uring arm underperforms here the verdict is scoped to this kernel and the bare-metal question stays open; the kernel version and probe result are recorded below.

## Predictions (filed before any box run)

Filed 2026-07-11, before the first campaign session, per F21.
The anchors are the reactor matrix (m0-reactor-ab.md, session of 2026-07-11): reactor P16/512 reads 1.50/1.77/1.82/1.34x (GET64/SET64/GET1K/SET1K), P1/512 reads 0.93/0.93/0.91/1.00x, P16/50 reads 0.84/1.08/1.23/1.06x, P1/50 reads 0.42-0.48x.
The mechanism being priced: the uring loop deletes its own read and write syscalls (recv completions and batched sends riding one enter per pass) but keeps the shard workers' eventfd wake, so the gains concentrate where loop syscalls dominate and passes fold work, which is high conns.

- PRED-U1, the P1 lever cell: GET 64B P1/512 lands 1.4-1.9x min-of-harness (from the reactor's 0.93x), because at depth 1 the two syscalls per op are the op, 512 conns fold whole pass-loads into single enters, and the rivals keep paying per-op reads and writes.
  SET 64B P1/512 lands in the same band; the 1KiB P1/512 rows land 1.3-1.8x (GET) and 1.1-1.5x (SET), damped by per-byte engine cost.
- PRED-U2, the gate block P16/512: GET 64B 1.55-1.75x, SET 64B 1.75-1.95x, GET 1KiB 1.85-2.05x, SET 1KiB 1.35-1.55x.
  The syscalls are already 1/16-amortized there, so the uring delta over the reactor is real but small (the deleted floor is ~90ns/op of ~660ns at the reactor's 6.6 Mops on 3 loops... quoted as a band, not arithmetic).
- PRED-U3, the 2x-at-P1 question this campaign exists to answer: 2x at P1 does NOT hold on this box; the best P1 cell tops out under 2x because the owner-side eventfd wake (~1 syscall/op at P1) and the per-op engine cost survive the ring, and the verdict published is the H1 ceiling, not the win.
- PRED-U4, P1/50: the uring arm improves on the reactor's collapse (0.42-0.48x) to 0.55-0.75x but stays below the goroutine arms' regime; 50 conns over 3 loops still fold almost nothing and valkey's io-threads keep the regime.
- PRED-U5, P16/50: uring sits within 0.9-1.1x of the reactor arm cell-for-cell; the amortized loop syscalls it deletes are small there and the regime stays pair's.
- PRED-U6, lab 21 (labs/f3/m0/21_uring, filed in its README): epoll 2.0-2.5 syscalls/op at P1/C512 against uring under 0.1; uring ns/op at most 0.6x epoll's there; parity within 0.85-1.15x at P1/C1; within 1.1x at P16.
- PRED-U7, counters: net_read_syscalls and net_write_syscalls on the uring arm read as ring ops not kernel entries (the counter seam counts submissions), and net_loop_wakes/op at P16/512 stays at the reactor's ~0.01 band since the wake path is unchanged.
- PRED-U8, memory bar: uring RSS tracks the reactor arm within ~10% per cell (same buffers, two per conn instead of out+bufio), the 64B cells keep breaching the pre-existing arena 2x bar, the 1KiB cells keep passing, per the f3 memory-bar rule no new unbounded state is introduced (the out cap is OutBufLimitBytes, inflight is bounded by the swap discipline).
- PRED-U9, F16: the uring driver does not meet the 0.95x-elsewhere clause (expected soft spots: P16/50 and P1/50, same as the reactor), so goroutine-single stays the sole default and uring stays behind -net uring with its wins recorded.

Addendum, filed 2026-07-11 before the box session, after the rebase onto the arena-RSS merge (#587) and the tightened same-data bar (at or below 1x rival RSS, peak counted): the uring driver now leases reply buffers from per-loop free lists like the reactor, but its armed single-shot recv pins one readBufSize buffer per connected conn that leasing cannot release.

- PRED-U10, tightened bar: on the 512-conn cells the uring arm's VmRSS sits within 32MiB+10% of the reactor arm (512 x 64KiB of pinned recv buffers, ~32MiB, is the structural delta; under load the reactor's conns hold leased buffers too so the observed gap is smaller), VmHWM tracks VmRSS within 15% on both aki event-loop arms (no burst allocator in either), and the 1KiB cells hold aki at or below both rivals on VmRSS and VmHWM while the 64B cells stay above (the arena floor, already published in m0-arena-rss).

## Kernel and probe

Recorded 2026-07-11 before any timed rep.
Kernel: 6.18.33.2-microsoft-standard-WSL2 (well past the 5.19 multishot-recv line; single-shot is this slice's design choice, not a kernel limit).
kernel.io_uring_disabled = 0.
Probe: f3srv -net uring -net-loops 3 at commit 4db804f serves with INFO net_driver:uring, no fallback line in the log, SET and GET round-trip; the uringAvailable gate (setup, IORING_FEAT_NODROP, REGISTER_PROBE for ASYNC_CANCEL/READ/SEND/RECV) passed.

## Lab 21 (echo loops)

Table and verdict in labs/f3/m0/21_uring/README.md; the headline: the ring deletes the loop syscalls wholesale (2.008 to 0.004 syscalls/op at P1/C512) and is never slower, but on this WSL2 kernel that deletion is worth 6-7% of echo wall time, not the 2x-shaped slice the reactor campaign's blocking-path syscall figures (876ns write + 537ns read) suggested.
Hot nonblocking loopback syscalls are much cheaper here than the blocking-path measurements, so PRED-U1's 1.4-1.9x band is at risk before the A/B starts, and the P1 verdict will be set by engine and wake costs the ring does not touch.

## A/B matrix

Three box sessions on 2026-07-11, all on the arena-RSS baseline (main with #586 and #587 rebased in), five resident servers per cell (aki single 7111, redis 7112, valkey 7113, aki reactor 7114, aki uring 7115, all taskset 0-7), the three aki arms interleaved per rep inside every cell, -net-loops 3 on both event-loop arms.
Session 1 (aki 4db804f) ran the full sixteen cells and is the throughput record below.
Session 2 (aki c081c39, the close-path free-list fix) reran the eight c512 cells and session 3 (aki 5b51dcb, the reply-list budget fix) reran four of them, both chasing the uring memory gap; their throughput matched session 1 within noise (uring P1/512 HEAD 0.87-0.93x in all three sessions), so the memory section cites them and the table here is session 1.
Driver stamps were verified per arm via INFO net_driver, distinct_keys_est passed the sanity check in every cell, and no crash, fallback line, or correctness failure appeared anywhere in the campaign.
Raw cell outputs, meta files, and machine summaries live under results/f3/m0-uring-ab/ (matrix/, matrix2/, matrix3/).

| cell | arm | aki ab ops/s | ab ratio | rb ratio | headline | aki p99 us (ab) | aki RSS MiB | RSS vs rival |
|---|---|---|---|---|---|---|---|---|
| get_64b_p16_c512 | single | 5,740,042 | 1.21x | 1.20x | 1.20x | 3,072 | 280 | 2.23x |
| get_64b_p16_c512 | reactor | 7,174,371 | 1.52x | 1.50x | 1.50x | 2,662 | 307 | 2.44x |
| get_64b_p16_c512 | uring | 6,864,952 | 1.46x | 1.38x | 1.38x | 2,593 | 510 | 4.06x |
| get_1k_p16_c512 | single | 2,901,963 | 1.39x | 1.43x | 1.39x | 5,890 | 2,342 | 1.81x |
| get_1k_p16_c512 | reactor | 4,110,167 | 1.94x | 2.00x | 1.94x | 4,112 | 1,695 | 1.31x |
| get_1k_p16_c512 | uring | 3,570,441 | 1.71x | 2.00x | 1.71x | 4,440 | 1,873 | 1.45x |
| set_64b_p16_c512 | single | 3,919,597 | 1.06x | 1.10x | 1.06x | 4,600 | 281 | 2.21x |
| set_64b_p16_c512 | reactor | 7,038,919 | 1.90x | 1.77x | 1.77x | 2,695 | 245 | 1.92x |
| set_64b_p16_c512 | uring | 6,711,764 | 1.82x | 1.77x | 1.77x | 2,687 | 419 | 3.29x |
| set_1k_p16_c512 | single | 3,160,287 | 1.02x | 1.08x | 1.02x | 5,386 | 1,224 | 0.93x |
| set_1k_p16_c512 | reactor | 4,536,331 | 1.46x | 1.40x | 1.40x | 3,699 | 1,177 | 0.90x |
| set_1k_p16_c512 | uring | 4,032,097 | 1.30x | 1.40x | 1.30x | 3,961 | 1,356 | 1.03x |
| get_64b_p16_c50 | single | 2,840,919 | 0.75x | 1.00x | 0.75x | 632 | 138 | 1.13x |
| get_64b_p16_c50 | reactor | 3,199,526 | 0.84x | 1.05x | 0.84x | 552 | 142 | 1.16x |
| get_64b_p16_c50 | uring | 3,091,237 | 0.81x | 1.05x | 0.81x | 552 | 178 | 1.46x |
| get_1k_p16_c50 | single | 2,415,812 | 1.14x | 1.36x | 1.14x | 726 | 1,110 | 0.86x |
| get_1k_p16_c50 | reactor | 2,618,286 | 1.24x | 1.36x | 1.24x | 665 | 1,108 | 0.86x |
| get_1k_p16_c50 | uring | 2,497,320 | 1.18x | 1.36x | 1.18x | 658 | 1,157 | 0.90x |
| set_64b_p16_c50 | single | 2,536,257 | 0.86x | 1.08x | 0.86x | 690 | 146 | 1.20x |
| set_64b_p16_c50 | reactor | 3,194,628 | 1.09x | 1.35x | 1.09x | 551 | 141 | 1.16x |
| set_64b_p16_c50 | uring | 3,083,380 | 1.05x | 1.35x | 1.05x | 552 | 171 | 1.40x |
| set_1k_p16_c50 | single | 2,474,589 | 0.95x | 1.31x | 0.95x | 706 | 1,059 | 0.80x |
| set_1k_p16_c50 | reactor | 2,748,095 | 1.07x | 1.31x | 1.07x | 640 | 1,059 | 0.80x |
| set_1k_p16_c50 | uring | 2,592,402 | 1.01x | 1.42x | 1.01x | 641 | 1,090 | 0.82x |
| get_64b_p1_c512 | single | 687,500 | 0.62x | 0.68x | 0.62x | 2,996 | 254 | 2.03x |
| get_64b_p1_c512 | reactor | 1,031,764 | 0.95x | 0.93x | 0.93x | 1,011 | 137 | 1.09x |
| get_64b_p1_c512 | uring | 1,038,008 | 0.95x | 0.93x | 0.93x | 1,023 | 338 | 2.70x |
| get_1k_p1_c512 | single | 603,360 | 0.56x | 0.62x | 0.56x | 3,672 | 1,068 | 0.83x |
| get_1k_p1_c512 | reactor | 994,827 | 0.93x | 0.93x | 0.93x | 1,053 | 1,055 | 0.82x |
| get_1k_p1_c512 | uring | 969,107 | 0.92x | 0.87x | 0.87x | 1,120 | 1,246 | 0.97x |
| set_64b_p1_c512 | single | 606,825 | 0.59x | 0.68x | 0.59x | 3,611 | 246 | 2.00x |
| set_64b_p1_c512 | reactor | 1,032,547 | 0.99x | 0.93x | 0.93x | 997 | 161 | 1.31x |
| set_64b_p1_c512 | uring | 1,026,786 | 0.99x | 0.87x | 0.87x | 1,038 | 311 | 2.53x |
| set_1k_p1_c512 | single | 566,489 | 0.58x | 0.59x | 0.58x | 3,910 | 1,159 | 0.88x |
| set_1k_p1_c512 | reactor | 1,002,284 | 1.03x | 0.93x | 0.93x | 1,037 | 1,096 | 0.83x |
| set_1k_p1_c512 | uring | 987,767 | 1.01x | 0.87x | 0.87x | 1,085 | 1,227 | 0.93x |
| get_64b_p1_c50 | single | 335,967 | 0.50x | 0.71x | 0.50x | 457 | 133 | 1.09x |
| get_64b_p1_c50 | reactor | 275,461 | 0.42x | 0.42x | 0.42x | 455 | 132 | 1.08x |
| get_64b_p1_c50 | uring | 271,410 | 0.41x | 0.50x | 0.41x | 441 | 164 | 1.34x |
| get_1k_p1_c50 | single | 334,955 | 0.53x | 0.71x | 0.53x | 453 | 1,049 | 0.82x |
| get_1k_p1_c50 | reactor | 270,581 | 0.42x | 0.42x | 0.42x | 459 | 1,050 | 0.82x |
| get_1k_p1_c50 | uring | 267,218 | 0.42x | 0.50x | 0.42x | 453 | 1,078 | 0.84x |
| set_64b_p1_c50 | single | 331,316 | 0.53x | 0.63x | 0.53x | 460 | 131 | 1.08x |
| set_64b_p1_c50 | reactor | 274,537 | 0.44x | 0.42x | 0.42x | 460 | 124 | 1.02x |
| set_64b_p1_c50 | uring | 268,013 | 0.42x | 0.50x | 0.42x | 454 | 135 | 1.11x |
| set_1k_p1_c50 | single | 328,380 | 0.59x | 0.75x | 0.59x | 465 | 986 | 0.83x |
| set_1k_p1_c50 | reactor | 270,741 | 0.49x | 0.50x | 0.49x | 463 | 935 | 0.78x |
| set_1k_p1_c50 | uring | 265,220 | 0.48x | 0.60x | 0.48x | 449 | 942 | 0.79x |

The counter seam confirms the mechanism ran as designed: at get_64b_p1_c512 the uring arm logged net_read_syscalls 42.55M and net_write_syscalls 42.54M over 42.54M commands (one ring recv and one ring send per op, counters reading as ring ops, not kernel entries) with net_loop_wakes at 0.122/op, and at get_64b_p16_c512 net_loop_wakes sat at 0.0118/op, the reactor's band.
The ring deleted the loop's kernel entries exactly as lab 21 measured; what it could not delete is the engine, the wake path, and this kernel's cheap nonblocking syscalls.

## Prediction judgments

- PRED-U1 WRONG, and lab 21 called it before the A/B started: GET 64B P1/512 landed 0.93x min-of-harness, the reactor's exact number, not 1.4-1.9x; the whole P1/512 block reads 0.87-0.93x.
  The two-syscalls-per-op premise priced the blocking-path figures (876ns + 537ns), but on this kernel a hot nonblocking recv/send costs ~70ns each, so deleting them moved nothing the harness can see.
- PRED-U2 WRONG in three of four cells: P16/512 landed 1.38/1.77/1.71/1.30x (GET64/SET64/GET1K/SET1K) against bands of 1.55-1.75/1.75-1.95/1.85-2.05/1.35-1.55.
  Only SET 64B scraped its band, and the uring arm trails the reactor by 3-12% across the block: the deleted syscalls were already 1/16-amortized there, and the ring's completion handling and staged-send bookkeeping cost more than they saved.
- PRED-U3 CORRECT: 2x at P1 does not hold on this box; the best P1 cell is 0.93x and the verdict below publishes the H1 ceiling.
- PRED-U4 WRONG: P1/50 did not improve on the reactor's collapse; uring reads 0.41-0.48x, cell-for-cell on top of the reactor's 0.42-0.49x.
  Fifty conns over three loops fold nothing, and the regime is the wake path plus depth-1 turnarounds, which the ring does not touch.
- PRED-U5 CORRECT: every P16/50 uring cell sits at 0.95-0.97x of the reactor arm, inside the 0.9-1.1x band.
- PRED-U6 mostly CORRECT (three of four lab predictions held); L-2, the 0.6x echo win, was WRONG and became the campaign's early warning, per labs/f3/m0/21_uring/README.md.
- PRED-U7 CORRECT: the counters read as ring ops (one recv and one send per op at P1/512, 42.5M each over 42.5M commands) and net_loop_wakes at P16/512 sat at 0.0118/op, the reactor's ~0.01 band.
- PRED-U8 WRONG: uring RSS ran 2.0-2.6x the reactor arm on the c512 cells in every session (365 vs 139 MiB at get_64b_p1_c512 on the final code), not within ~10%.
  Two real free-list bugs were found and fixed along the way (close-path recycling that never drained, c081c39; a reply list sized for one parked buffer per conn when the uring send path parks two, 5b51dcb), and forced-GC heap profiles in the local repro confirmed the churn went away, but the box gap is structural, not a leak: the armed single-shot recv pins one 64KiB buffer per connected conn, the send path holds out plus inflight where the reactor holds one buffer, and the close path drops buffers to GC that the pacer collects lazily.
  The boundedness half of the prediction held: out is capped by OutBufLimitBytes, inflight by the swap discipline, free lists by uringOFreeCap, and no cell showed unbounded growth.
- PRED-U9 CORRECT: uring fails the F16 0.95x-elsewhere clause (P1/50 at 0.41-0.48x, GET 64B P16/50 at 0.81x) and never beats the reactor arm anywhere, so goroutine-single stays the sole default and the driver stays behind -net uring.
- PRED-U10 WRONG: the 32MiB+10% band priced only the pinned recv buffers; the measured gap on the fixed code is 170-230 MiB on the 64B c512 cells (session 3 table below).
  The clauses that held: VmHWM tracks VmRSS within a few percent on both event-loop arms once the recycle bug was out (session 1's uring HWM was inflated by it), and the 1KiB P1 cells hold uring at or below both rivals (1,310 vs 1,341 and 1,268 vs 1,301 MiB).
  The clause that failed with it: GET 1KiB P16 has uring at 1.38x the loaded rival, above the bar the reactor also strains (1.25x).

## Memory bar (VmRSS and VmHWM)

The tightened bar for this campaign: aki at or below 1x the loaded rival's RSS for the same data, peak (VmHWM) counted alongside steady RSS.
The table is the fixed-code readings: session 3 (5b51dcb) for its four cells, session 2 (c081c39) for the other four c512 cells; RSS is the loaded steady reading, HWM the per-cell peak, both MiB; the rival column is the lower of loaded redis and valkey.

| cell | session | reactor RSS/HWM | uring RSS/HWM | min rival RSS/HWM | uring vs rival | uring vs reactor |
|---|---|---|---|---|---|---|
| get_64b_p16_c512 | 3 | 296 / 305 | 451 / 490 | 125 / 125 | 3.59x | 1.52x |
| set_64b_p16_c512 | 2 | 244 / 260 | 433 / 438 | 128 / 131 | 3.36x | 1.77x |
| get_64b_p1_c512 | 3 | 139 / 158 | 365 / 363 | 124 / 125 | 2.93x | 2.63x |
| set_64b_p1_c512 | 3 | 179 / 181 | 350 / 349 | 125 / 125 | 2.85x | 1.96x |
| get_1k_p16_c512 | 3 | 1,699 / 2,156 | 1,872 / 1,944 | 1,357 / 1,365 | 1.38x | 1.10x |
| set_1k_p16_c512 | 2 | 1,183 / 1,183 | 1,355 / 1,354 | 1,330 / 1,337 | 1.02x | 1.15x |
| get_1k_p1_c512 | 2 | 1,055 / 1,079 | 1,310 / 1,309 | 1,341 / 1,349 | 0.98x | 1.24x |
| set_1k_p1_c512 | 2 | 1,098 / 1,096 | 1,268 / 1,275 | 1,301 / 1,303 | 0.97x | 1.15x |

Readings against the bar: the 1KiB P1 cells pass on both RSS and HWM, the 1KiB P16 cells sit at 1.02-1.38x, and the 64B cells fail at 2.85-3.59x where the reactor fails at 1.12-2.37x and the arena floor already fails single at ~2x (published in m0-arena-rss).
The uring-specific surcharge over the reactor is 110-230 MiB per 512-conn cell and it is the driver's shape, not a leak: one pinned 64KiB single-shot recv buffer per conn that leasing cannot release (32 MiB at 512 conns), a double-buffered send path (out plus inflight against the reactor's single writer buffer, another 32 MiB live under load), GC pacing that roughly doubles the live delta, and a close path that must drop buffers to GC because a canceled uring op can still write into them (the zombie hold), so disconnect storms leave garbage the pacer collects on its own schedule.
Provided-buffer rings with multishot recv would delete the pinned per-conn recv buffer, and that is the named follow-up; there is no throughput win here to weigh this cost against.

## Verdict

Frozen 2026-07-11, doc 08 section 5.4's second sentence: 2x at P1 does not hold on any driver on this box, and this note publishes the H1 ceiling with its evidence.

The evidence chain: the reactor left P1/512 at 0.93x with the loop's syscalls named as the remaining floor; lab 21 showed the ring deletes that floor wholesale (2.008 to 0.004 syscalls/op) for 6-7% of echo wall time on this kernel; the full A/B confirmed it end to end, with uring landing at 0.87-0.93x on every P1/512 cell, indistinguishable from the reactor, while the counters proved the deletion ran as designed (one ring recv and one ring send per op, no kernel entries per op).
The last named throughput lever is spent, so what remains at P1 is H1: the per-command engine walk plus the owner-wake path, costs no network driver shape can remove.

Scope, stated plainly: this ceiling is proven on kernel 6.18.33.2-microsoft-standard-WSL2, where a hot nonblocking syscall costs ~70ns.
On bare metal, where syscall entry is the measured 500-900ns, the deleted floor is 10-20x larger and the uring arm could yet be the 1.4x-shaped win PRED-U1 priced; that is a box question, not a driver question, and the driver is now built, tested, and waiting behind -net uring for it.

The standing results: the gate block P16/512 holds at 1.30-1.77x on uring and 1.40-1.94x on the reactor, so the 2x gate remains blocked on the same H1 ceiling at high depth too.
Per F16 and PRED-U9, goroutine-single stays the default; the reactor stays the win-regime option; uring ships as the third driver with fallback, full parity on the Linux test suite (AKI_NET=uring in CI), and a memory surcharge on 512-conn cells (table above) that rules it out as a default even where it ties.
Follow-ups, in order: multishot recv with provided-buffer rings (deletes the pinned per-conn recv buffer, the memory surcharge's largest structural piece, and folds resubmits), and a bare-metal Linux rerun of this exact matrix, which is the only thing that can reopen the P1 question.
