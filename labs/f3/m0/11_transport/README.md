# Lab: transport cheap wins

Spec 2064/f3, M0 gate follow-up, lab 11 (issue #542).

## The question

The ns/op decomposition of the GET 64B P16 gate cell (issue #542) charged the transport, not the engine: 876 ns/op in write syscalls at 1.81 write() per 16-command round where 1.0 suffices, 383 ns/op of wake tax in pthread_cond_signal dominated by the locked-M handoff when a pinned worker parks the instant its queue drains, and 118 ns/op in the parks themselves, against 33 ns/op of engine execute.
Three knobs claim those cycles.
The boundary flush defers the writer's socket flush while the connection still owes replies for commands already published to shards, flushing at the pipeline boundary or when the buffer fills.
The worker spin-before-park window keeps the hot consumer polling through the inter-wave gap instead of parking into a cross-thread wake; the workers inherited the 4us window lab 3 sized for connection writers, so it gets its own sweep here.
And the thread lock itself is up for review: the single-owner invariant is goroutine affinity, not thread affinity, so LockOSThread only ever bought cache residency while charging every park and unpark the handoffp/startlockedm double cond_signal.

## Method

`go run .` runs the full matrix in one process: worker spin window {0, 5us, 20us, 80us} x workers {unpinned, pinned} x flush {boundary, every-drain}.
One f3srv per cell on loopback, 4 shards, 1M keys at 64B, then 128 client connections run pipelined GET rounds of depth 16 for 2 seconds, the gate cell shape.
The spin window rides shard.SetWorkerSpinWindow, the pin rides Options.PinWorkers, and the old discipline rides Options.FlushEveryDrain, which restores a flush after every writer drain pass.
Per cell: GETs per second, process CPU per op from rusage (the clients live in the same process, so this is server plus client and only same-column comparisons mean anything), and server flushes per 16-command round from the drivers flush counter.
The reply buffer is the default 64KiB and a 64B P16 round is about 1.2KiB, so every counted flush is exactly one write() syscall and flush/round is writes per round.

## Results

Apple M4 (4P + 6E), 24GiB, macOS, Go 1.26, in process, quiet box.
Two full runs; absolute ops/s moved 10 to 25 percent between runs (the co-located client load makes every cell scheduler-sensitive), the ordering held.

Run 1:

| spin window | workers | boundary ops/s | boundary CPU/op | boundary flush/round | every-drain ops/s | every-drain CPU/op | every-drain flush/round |
|---|---|---|---|---|---|---|---|
| 0s | unpinned | 1996091 | 3.047µs | 1.00 | 1886763 | 3.416µs | 1.58 |
| 0s | pinned | 2052390 | 3.058µs | 1.00 | 1923491 | 3.29µs | 1.42 |
| 5µs | unpinned | 1831674 | 3.202µs | 1.00 | 1883531 | 3.54µs | 1.51 |
| 5µs | pinned | 1549818 | 3.101µs | 1.00 | 1644097 | 3.394µs | 1.42 |
| 20µs | unpinned | 1742169 | 3.435µs | 1.00 | 1646716 | 3.628µs | 1.48 |
| 20µs | pinned | 1832126 | 3.364µs | 1.00 | 1729864 | 3.594µs | 1.42 |
| 80µs | unpinned | 1442370 | 3.883µs | 1.00 | 1412902 | 4.195µs | 1.36 |
| 80µs | pinned | 1635972 | 3.732µs | 1.00 | 1453144 | 4.073µs | 1.48 |

Run 2:

| spin window | workers | boundary ops/s | boundary CPU/op | boundary flush/round | every-drain ops/s | every-drain CPU/op | every-drain flush/round |
|---|---|---|---|---|---|---|---|
| 0s | unpinned | 2522442 | 3.054µs | 1.00 | 2164712 | 3.699µs | 1.53 |
| 0s | pinned | 2246663 | 3.405µs | 1.00 | 2198914 | 3.658µs | 1.38 |
| 5µs | unpinned | 2222602 | 3.495µs | 1.00 | 2083602 | 3.871µs | 1.48 |
| 5µs | pinned | 2304786 | 3.469µs | 1.00 | 2028752 | 3.774µs | 1.38 |
| 20µs | unpinned | 1342100 | 4.144µs | 1.00 | 970006 | 4.98µs | 1.49 |
| 20µs | pinned | 1113146 | 4.162µs | 1.00 | 1185651 | 4.6µs | 1.46 |
| 80µs | unpinned | 1337854 | 4.624µs | 1.00 | 1368496 | 4.889µs | 1.39 |
| 80µs | pinned | 1466029 | 4.577µs | 1.00 | 1071173 | 5.291µs | 1.57 |

The shape, consistent across both runs:

The boundary flush takes writes per round from 1.4 to 1.6 down to exactly 1.00 in every cell and cuts CPU/op by 250 to 650 ns against the same cell with the old discipline; at the best cells it is also worth 5 to 15 percent throughput.
The estimate from the decomposition was about 390 ns/op and the measurement brackets it.

The spin window loses monotonically from 0 through 80us on both throughput and CPU/op.
The prediction that the hot consumers wanted a wider window than lab 3's 4us was wrong on this box, and the reason is the box, not the protocol: server and clients share ten cores, so every microsecond a worker burns polling an empty queue is stolen from the net goroutines that would refill it.
Lab 3 measured the same protocol on dedicated pinned threads with one producer and got its latency win exactly because nothing else wanted the core.
Under real multiplexed load the park is the cheaper idle, and it got much cheaper still once the workers stopped being locked threads, because an unpinned park is a plain gopark with no handoffp.

Pinning is a wash.
At the frozen cell (spin 0, boundary flush) the two runs split, pinned +2.8 percent then unpinned +12.3 percent, with CPU/op a dead heat in run 1 (3.047 vs 3.058) and clearly unpinned in run 2 (3.054 vs 3.405).
Across the other cells the wins scatter both ways inside the run-to-run spread.
A tie does not pay for the lock: the lock is what puts handoffp and startlockedm on the park path that the wake-tax 383 ns/op profile named, and with the window frozen at 0 the workers park constantly.

## Verdict

Boundary flush on, structurally, in f3srv/drivers (the every-drain discipline stays as the Options.FlushEveryDrain lab knob).
Freeze workerSpinWindow = 0 in engine/f3/shard/tuning.go: the worker parks as soon as its queue drains, and SetWorkerSpinWindow stays as the lab hook.
Drop LockOSThread from the shard workers by default; the pin stays available as Config.PinWorkers and the f3srv -pin-workers flag for a box with cores to dedicate, but it must earn its way back with numbers there.

On the wire (aki-bench, closed loop, interleaved A/B, this box), the three together moved GET 64B P16 c128 from 1.74M to 1.96M ops/s and GET 1KiB P16 c128 from 1.33M to 1.49M ops/s at the medians, won every interleaved pair, and improved the P1 c50 latency cell (p50 293us to 242us, p99 1027us to 745us over six reps each).
