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

Pending the box run.

## Verdict

Pending.
