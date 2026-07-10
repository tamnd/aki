# Lab 19: reactor loop count (M10 pull-forward slice 6)

The question: doc 08 section 4.2 gives two loop-count rules in one section, M = shard count (the f1 default it describes) and M = cores minus shards (the pinned-split starting point it argues for), and the plan says the lab decides.
On the gate box's server mask (8 cpus, taskset 0-7) f3srv defaults to 4 shards, so the two formulas coincide at 4 and a plain sweep cannot tell them apart.
This lab therefore runs two sweeps: the default-shards sweep over -net-loops {1, 2, 3, 4, 6, 8} to find the knee, and a disambiguation arm at -shards 5 with -net-loops {3, 5} where the formulas split (cores minus shards = 3, shards = 5).

## Method

Full f3srv binary under the gate protocol split, not an in-process harness, because core accounting between loops and unpinned shard workers is the question itself.
Server taskset 0-7, generator taskset 8-15, -net reactor, arena 512 MiB per shard.
Cells: GET 64B and SET 64B, 1M keys uniform, P16, 512 conns, redis-benchmark --threads 4, preload before GET reps, 3 reps of n=20M per arm, none discarded.
The sweep is aki-internal (one knob, one harness, same box session), so no rival ratio is quoted here; rivals meet the reactor in the slice 7 matrix.
VmRSS is snapshotted after the last rep of each arm, which is the 512-conn RSS column doc 08 section 6.2's buffer-leasing question reads.
run.sh is the exact driver; raw rep CSVs land in /root/f3gate/reactor-ab/lab19/ and are copied under results/f3/m0-reactor-ab/.

## Prediction (filed before measuring)

Throughput rises to 4 loops and flattens or dips at 8; the winner is 4, which on this box is both formulas at once.
At shards=5 the knee follows cores minus shards (loops=3 >= loops=5), because loops compete with the unpinned owners for the same 8 cpus and an oversubscribed reactor manufactures scheduler churn the goroutine driver does not pay.
RSS at 512 conns stays flat across loop counts (buffers are per-conn, not per-loop) and under the bar, so buffer leasing is not forced by this lab.

## Results

GamingPC, WSL2, aki c76d6c0, 2026-07-11, redis-benchmark --threads 4, median of 3 reps of n=20M, P16/512 conns, 1M keys uniform.
rps is the rb csv figure; p99 is rb's per-run p99 in ms; RSS is f3srv VmRSS after the last rep.

Default shards (4 on the 8-cpu mask):

| loops | GET Mops | GET p99 ms | SET Mops | SET p99 ms | RSS (GET) |
|---|---|---|---|---|---|
| 1 | 2.05 | 5.75 | 2.00 | 5.94 | 516 MiB |
| 2 | 6.14 | 1.64 | 6.14 | 1.73 | 516 MiB |
| 3 | 6.64 | 1.38 | 6.65 | 1.45 | 568 MiB |
| 4 | 6.65 | 1.56 | 6.14 | 1.61 | 593 MiB |
| 6 | 4.70 | 2.46 | 4.44 | 2.66 | 687 MiB |
| 8 | 3.47 | 3.36 | 3.33 | 3.49 | 701 MiB |

An interleaved second GET pass over {3, 4, 6} reproduced 6.64 / 6.65 / 4.70 Mops within 0.1%, so the knee is not session drift.

Disambiguation arms, GET only:

| shards | loops | Mops | p99 ms | note |
|---|---|---|---|---|
| 5 | 2 | 6.14 | 1.74 | cores-shards-1 |
| 5 | 3 | 6.65 | 1.44 | cores minus shards |
| 5 | 5 | 5.70 | 2.18 | shards |
| 3 | 3 | 6.65 | 1.39 | |
| 3 | 4 | 6.65 | 1.51 | |
| 3 | 5 | 5.70 | 2.11 | cores minus shards |
| 3 | 6 | 4.70 | 2.43 | |

A false-saturation check reran the shards=4 GET arms {2, 3, 4, 6} with an 8-thread generator: 3.99 / 6.65-7.25 / 6.65 / 4.99 Mops, knee unchanged at 3, so the 4-thread plateau is not a generator ceiling artifact (the loops=3 arm even cleared it once at 7.25).

## Verdict

The section 4.2 contradiction resolves as neither formula: the knee sits at 3 loops for shard counts 3, 4, and 5 alike, so the loop count follows the core budget alone and has no tie to the shard count.
M = shards loses outright at shards=5 (5 loops 5.70 vs 3 loops 6.65 Mops) and M = cores minus shards loses at shards=3 (5 loops 5.70 vs 3 loops 6.65) and on the SET and p99 columns at shards=4 (4 loops 6.14 vs 3 loops 6.64).
3 of 8 server cpus is the 2/5 network share of the doc 03 section 2.2 core split, the exact complement of shard.DefaultShards' 3/5, so that is the frozen default: Options.NetLoops <= 0 now takes max(1, GOMAXPROCS*2/5) (f3srv/drivers/reactor_linux.go defaultNetLoops).
Oversubscription is confirmed as the failure mode the section predicted: past the knee every added loop costs throughput (6 loops -29%, 8 loops -48%) and p99 (2.4x at 8 loops), because loops compete with the unpinned owners for the same mask.
The 512-conn RSS column moves 516 to 701 MiB across the sweep, driven by loop count, not connection buffers; at the frozen default it reads 568 MiB against rivals in the several-hundred-MiB band, so doc 08 section 6.2 buffer leasing is not forced at this connection count and stays a follow-up slice for the 10k-conn shape.

