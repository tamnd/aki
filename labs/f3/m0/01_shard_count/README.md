# Lab: shard count per box

Spec 2064/f3/03 section 2.2, M0 lab 1.

## The question

f3 fixes the shard count S at startup and doc 03's starting rule gives the data plane about 60 percent of the box's cores. Before the shard runtime bakes a default in, this lab asks: with the network entirely out of the picture, how does the ported single-owner store scale when the keyspace is partitioned N ways across N pinned workers, and is there anything to gain from running more shards than cores?

## Method

`go run .` builds N `engine/f3/store` instances, deals 1M keys (16B keys, 64B values) round-robin across them, and runs 16M total ops (90/10 GET/SET, pre-shuffled uniform order) split evenly over N worker goroutines. Each worker calls `runtime.LockOSThread` and touches only its own store, which is the single-owner contract the shard runtime enforces. All workers start on a barrier and the run is timed to the slowest finisher, so the figure is aggregate throughput with every shard hot. N sweeps 1, 2, 4, 8, cores, 2x cores.

macOS has no `sched_setaffinity`, so LockOSThread ties each worker to a thread but the kernel still places threads on P or E cores as it likes. That is a known limit of laptop runs and one of the reasons the verdict below stays provisional.

## Results

Apple M4 (4P + 6E, 10 cores), macOS, Go 1.26. Two runs, second shown; run-to-run spread was under 5 percent except the 4-shard row (18.5 to 22.5 Mops/s, cache-residency sensitive).

| shards | Mops/s | speedup vs 1 | per-shard Mops/s |
|---|---|---|---|
| 1 | 4.3 | 1.00x | 4.28 |
| 2 | 9.9 | 2.32x | 4.97 |
| 4 | 18.5 | 4.33x | 4.64 |
| 8 | 37.9 | 8.85x | 4.73 |
| 10 | 41.0 | 9.59x | 4.10 |
| 20 | 45.5 | 10.63x | 2.27 |

Notes on the shape:

- Scaling is superlinear at 2 to 8 shards because partitioning also shrinks each worker's index and arena working set, so per-shard throughput rises even as workers are added. That effect is real in production too (it is one of the arguments for sharding at all) but it inflates the low-N speedup column.
- 8 shards already reaches 8.9x. The last two cores (E cores on this box) add about 8 percent.
- 2x oversubscription does not collapse: 20 pinned threads on 10 cores gained another ~10 percent here, almost certainly because time-slicing lets the P cores absorb work that a 10-way static split strands on E cores. On a box with homogeneous pinned cores that mechanism does not exist.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. One shard per data-plane core is the rule: the engine scales essentially linearly to the core count and there is no cliff to avoid. The 2x-cores row's small gain is a P/E-core artifact of macOS scheduling, not evidence for oversubscription; on the gate box, where owners pin to homogeneous cores and the other ~40 percent of cores must stay free for parse and syscalls, oversubscribing owners would steal exactly those cores. The doc 03 default (S = 8 owners on the 14-core gate box, sweep {6, 8, 10}) stands. The gate-box sweep must rerun this with real `sched_setaffinity` pinning before the shard-count flag default is frozen.
