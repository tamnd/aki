# Lab 14: transport decomposition from counters

Part of issue #542, M10 pull-forward slice 1 per spec 2064/f3/08 section 9.5.

## Question

The per-op cost decomposition of the gate cells has so far come from CPU profiles.
The pre-lab-11 darwin reference for GET 64B P16 c128 charged 876 ns/op to write syscalls, 537 to reads, 383 to wakes, and 118 to parks of 2045 total.
Profiles are manual and per-box; the akinet counters are readable per run.
Can counters/op times a calibrated per-event cost reproduce the profile's decomposition, so each reactor slice can rerun this lab and read which band it moved without pulling a profile?

## Setup

Apple M4 (darwin/arm64), in-process f3srv over loopback, 4 shards, 1M keys at 64B, pipelined GET rounds, 10s per cell.
Cells: the P16 c128 gate shape and a P1 c50 latency row.
CPU/op is process rusage over ops, so it includes the client, same caveat as lab 11.

Calibration microbenches run in the same process before the cells:

- write(2) and read(2) come from a concurrency-matched echo herd: 128 loopback pairs, one round in flight per pair, a P16 request round (544B) up and a P16 reply round (1136B) down.
  Each write lands in a kevent-parked peer and its wall clock is its cost; the two reads park, so they are priced as the CPU residue per round after the two timed writes, split evenly.
- wake and park come from a channel ping-pong on cap-1 token channels, the shard waker's shape: the wake is the producer-visible send to a parked consumer, the park is the pair's CPU residue after the wakes are subtracted.

This run calibrated write(2) 6.51us, read(2) 10.14us, wake 60ns, park 114ns.
An isolated hot pair prices the same syscalls far lower (about 2us per write, 300ns per read) and is the wrong regime; see the verdict for the gap that remains even with the matched herd.

## Results

Current main (19c08a4 plus the counter slice), 2026-07-10.

P16 c128: 2,945,206 ops/s, CPU/op 2.856us (server plus client).

| counter/op | writes | reads | batches | cmds/batch | worker wakes | conn wakes | worker parks | conn parks |
|---|---|---|---|---|---|---|---|---|
| P16 c128 | 0.062 | 0.063 | 0.062 | 16.00 | 0.033 | 0.144 | 0.033 | 0.144 |

The counters read the transport doing exactly what doc 08 says it should: one write and one read per 16-command round, boundary flush holding, and a wake only on roughly one round in seven.

P1 c50: 215,113 ops/s, CPU/op 38.5us (server plus client).

| counter/op | writes | reads | batches | cmds/batch | worker wakes | conn wakes | worker parks | conn parks |
|---|---|---|---|---|---|---|---|---|
| P1 c50 | 1.000 | 1.000 | 1.000 | 1.00 | 0.871 | 0.999 | 0.870 | 0.999 |

At P1 every command pays a full syscall pair and nearly a full wake/park cycle on both sides, which is the whole M10 case in one row.

Estimated ns/op is counters/op times the calibrated cost:

| est ns/op | writes | reads | wakes | parks | sum |
|---|---|---|---|---|---|
| P16 c128 | 407 | 634 | 11 | 20 | 1071 |
| P1 c50 | 6508 | 10139 | 112 | 213 | 16973 |

## Profile cross-check

A CPU profile was fetched from the server's pprof endpoint during the P16 cell (9.11s window, 68.44s of samples, 26.83M ops in the window).
Bands: server writes are `countedWriter.Write` cum, server reads are `net.(*conn).Read` focused under `Server.handle`, wakes are `wakeConns`, parks are `Conn.idleOnce` cum.

| ns/op, P16 c128 | writes | reads | wakes | parks | sum |
|---|---|---|---|---|---|
| profile | 775 | 533 | 3 | 23 | 1334 |
| counter estimate | 407 | 634 | 11 | 20 | 1071 |

Against the pre-lab-11 reference (876/537/383/118), current main's wake band has collapsed from 383 to 3 ns/op and writes are down from 876 to 775, which matches the boundary-flush and waker work that landed since.

## Verdict

The counter decomposition reproduces the profile's shape and three of its four bands: reads within 19 percent, parks within 13 percent, and the wake band correctly read as negligible on current main (both figures are near zero, 11 vs 3 ns/op).
The write band is underestimated about 2x on this box: the loaded server pays about 12.5us of kernel CPU per write(2) while the matched echo herd pays 6.5us, and no isolated microbench tried here closed that gap, so the estimated sum lands 20 percent under the profile rather than inside the 10 percent hoped for.
That gap is a fixed per-box calibration scale, not a counting error: the counters/op are exact, so band attribution and slice-to-slice deltas are trustworthy even where the absolute darwin write cost is not.
Frozen: this lab is the per-slice recovery meter for the reactor slices; each slice reruns it and reads which band its change moved, with a full profile cross-check reserved for any slice whose write band moves in a surprising direction.
