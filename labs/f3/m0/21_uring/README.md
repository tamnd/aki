# Lab 21: io_uring vs epoll echo loops (M10 slice 3)

The reactor campaign (results/f3/m0-reactor-ab.md) ended on a named floor: at P16/512 the loop still pays epoll_wait plus one read and one write per batch, and at P1 the two syscalls per op are the op (876ns write + 537ns read measured on the gate box).
Doc 08 section 4.3 says the ring-native driver deletes that floor; this lab measures the deletion in isolation before the full server A/B, on two echo loops that differ only in mechanism.
Both arms: one loop thread, dup'd nonblocking fds on loopback, NODELAY re-set on the dup, fixed 64B messages, in-process pipelined clients on the runtime's own netpoll.
The epoll arm is the reactor's syscall shape (epoll_wait, read, write, all counted); the uring arm is the driver's (single-shot recv resubmit, sends staged, one GETEVENTS enter per pass, enters counted).

## Method

`taskset -c 0-7 sh run.sh` on the gate box (GamingPC WSL2, the campaign's server mask), 8s per cell, modes {epoll, uring} x conns {1, 64, 512} x pipeline {1, 16}.
An op is one 64B message echoed; ns/op is wall time over ops with all clients concurrent, so it is a throughput reading, not a latency one.
syscalls/op counts the loop side only, because the loop's syscall budget is what the driver decision is about.

## Predictions (filed before running)

- L-1: epoll syscalls/op at P1/C512 reads 2.0-2.5 (read + write per op plus the epoll_wait share); uring reads under 0.1 (enters fold across 512 ready conns).
- L-2: uring ns/op at P1/C512 is at most 0.6x epoll's, because the syscall floor is most of an echo op at depth 1 and the fold deletes it.
- L-3: at P1/C1 the arms sit within 0.85-1.15x of each other: with one connection every transition is one sleeping syscall in either mechanism (epoll_wait vs enter), so there is nothing to fold.
- L-4: at P16 both arms are already amortized (epoll ~3/16 syscalls/op, uring under 0.02) and ns/op sits within 1.1x either way.

## Results

Gate box, 2026-07-11, kernel 6.18.33.2-microsoft-standard-WSL2, `taskset -c 0-7 sh run.sh`, commit 4db804f.

| conns | pipe | epoll ns/op | epoll sys/op | uring ns/op | uring sys/op | uring/epoll ns |
|------:|-----:|------------:|-------------:|------------:|-------------:|---------------:|
| 1     | 1    | 26307       | 3.000        | 26345       | 2.000        | 1.00x          |
| 1     | 16   | 1674        | 0.188        | 1651        | 0.125        | 0.99x          |
| 64    | 1    | 2055        | 2.023        | 1973        | 0.043        | 0.96x          |
| 64    | 16   | 136         | 0.126        | 128         | 0.003        | 0.94x          |
| 512   | 1    | 2133        | 2.008        | 1989        | 0.004        | 0.93x          |
| 512   | 16   | 139         | 0.125        | 130         | 0.000        | 0.94x          |

Prediction judgments:

- L-1 CORRECT: epoll P1/C512 reads 2.008 syscalls/op, uring reads 0.004, a 500x fold; the deletion mechanism works exactly as designed.
- L-2 WRONG, and it is the finding of the lab: uring ns/op at P1/C512 is 0.93x epoll's, not the predicted at-most-0.6x. Deleting two syscalls per op bought 144ns/op of wall time, not the ~1.4us the bare-metal per-syscall figures (876ns write + 537ns read) priced in. On this WSL2 kernel a hot non-blocking read/write on loopback is far cheaper than those blocking-path figures, so the syscall floor the ring deletes is a small slice of the echo op here.
- L-3 CORRECT: P1/C1 reads 1.00x, one sleeping transition per op either way.
- L-4 CORRECT: every P16 cell sits at 0.94x, inside the 1.1x band.

## Verdict

Frozen 2026-07-11.
The ring deletes the loop's syscalls wholesale (2.0 to 0.004 syscalls/op at P1/C512) and is never slower: 0.93-0.94x ns/op at every folded cell, parity at C1.
But on this virtualized kernel the deleted syscalls are worth single-digit percent of wall time, not the 2x-shaped slice the reactor campaign's blocking-path measurements suggested, so the server A/B should expect uring to land near the reactor arm plus a few percent, and the section 5.4 question at P1 will be answered by the engine and wake costs the ring does not touch.
The 0.6x-shaped win, if it exists, lives on bare metal where syscall entry is the measured 500-900ns; that scoping goes in the campaign note's verdict.
