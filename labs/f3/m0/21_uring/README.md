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

Pending the box session; the sweep runs under the campaign's BOX LOCK alongside the A/B.

## Verdict

Pending the results table.
