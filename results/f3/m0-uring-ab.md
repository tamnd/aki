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

Pending the box session: uname -r, the uringAvailable probe result, and the multishot-recv feature line (5.19+) recorded here before any timed rep.

## Lab 21 (echo loops)

Pending; table and verdict land in labs/f3/m0/21_uring/README.md and the headline is copied here.

## A/B matrix

Pending the box session.

## Prediction judgments

Pending.

## Verdict

Pending; one of doc 08 section 5.4's two sentences, written after the matrix.
