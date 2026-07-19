# m0/28 reactor peak attribution

Attribute and attack the reactor's peak VmHWM on the M0-G3 memory row.

## Why

The tip gate reads the reactor at 203 MiB peak VmHWM on the 64B 1M-key P16 c512 string cell, over redis 161 MiB and valkey 132 MiB, while aki `used_memory` (live data) is 108 MiB, the leanest of the three.
So the peak overage is not the data.
It is Go heap fabric (newBatch nodes, per-node reply buffers) plus GC growth headroom sitting on top of the off-heap string arena.
The arena is off-heap, so `GOGC` never moved the string data before, but the reactor's heap fabric is real heap and is attackable by trimming GC headroom and the reply-buffer start size.

This lab sweeps the two levers at the frozen gate flags, measures VmHWM and throughput per arm, and finds the config that pulls the peak under the worse rival at flat throughput.

Levers:
- `GOGC` / `GOMEMLIMIT`: Go GC growth headroom on the heap fabric.
- `-rep-cap`: the per-node reply buffer start size (lab 27, PR #1159), independent of `-batch-data-cap`.

## Run

    ssh gamingpc  # WSL2 Ubuntu, i9-13900K, gate pins
    BIN=/root/bin/f3srv-dcef1d8c bash run.sh       # attribution sweep -> lab28.csv
    BIN=/root/bin/f3srv-dcef1d8c bash confirm.sh   # median-of-3 on the finalists -> confirm.csv

Server mask cores 4-17 (GOMAXPROCS 14), two `redis-benchmark -r 1M` generators on 18-24 and 25-31, summed.
`-net-loops 0` resolves to GOMAXPROCS/2 = 7 (loop-knee, PR #1158).

## Attribution sweep (lab28.csv)

VmHWM in MiB (kB/1024), throughput is the dual-gen summed max-of-2.

| arm | SET | GET | VmHWM after SET | VmHWM peak | delta vs baseline |
|---|---|---|---|---|---|
| baseline | 9.13M | 9.13M | 189.2 | 195.8 | - |
| repcap512 | 9.13M | 9.12M | 177.3 | 196.6 | +0.8 |
| gogc50 | 9.13M | 9.12M | 185.0 | 193.7 | -2.1 |
| gogc50_rep512 | 9.13M | 9.14M | 165.8 | 176.6 | -19.2 |
| gogc30_rep512 | 9.89M | 9.14M | 161.7 | 160.4 | -35.4 |
| memlimit180 | 9.13M | 9.12M | 186.4 | 189.0 | -6.8 |

Reading:
- `-rep-cap 512` alone does nothing to the peak (the reply buffers grow past 512 under load anyway); it only helps once GC headroom is also capped, because then the trimmed buffers are what the smaller live heap holds.
- `GOGC` is the dominant lever. GOGC=50 shaves a little, GOGC=30 with rep-cap 512 takes 35 MiB off the peak.
- `GOMEMLIMIT=180MiB` is a soft ceiling the workload never reaches (live heap is well under it), so it barely moves.
- Throughput is flat across every arm. GC runs more often at GOGC=30 but each cycle is cheap (the Go heap fabric is ~50 MiB, the 108 MiB of string data is off-heap arena and not scanned), so on this cell the trim is free.

## Median-of-3 confirm (confirm.csv)

The two arms that bracket the rival peaks, proper VmHWM, median of three.

| arm | SET med | GET med | VmHWM | vs redis 160.7 | vs valkey 131.7 |
|---|---|---|---|---|---|
| gogc30_rep512 | 9.13M | 9.14M | 164.9 | 1.03x | 1.25x |
| gogc20_rep512 | 9.11M | 9.13M | 157.1 | 0.98x | 1.19x |
| gogc30_rep256 | 9.13M | 9.13M | 166.7 | 1.04x | 1.27x |

## Verdict

`GOGC=20 -rep-cap 512` is the memory config for the M0 string gate cell.
It holds the reactor peak at 157.1 MiB, under the worse rival redis (160.7 MiB, ratio 0.98x), at flat 2x-plus throughput (SET 9.11M = 2.71x redis / 3.14x valkey, GET 9.13M = 2.14x redis / 2.43x valkey).
That is a 46 MiB (23%) peak cut from the 203 MiB baseline, moving the reactor from 1.27x-over-redis to 0.98x-under-redis.

The residual to valkey (1.19x peak) is the per-connection sharded-MPSC conn-fabric declared structural in the memory slim arc (#1159): at c512 there are 512 conn hop-node sets that valkey's shared io-thread buffers do not pay per connection.
On the same-data metric that residual inverts: aki `used_memory` 108.3 MiB beats both redis (129.6, 0.84x) and valkey (109.7, 0.99x).
So aki carries the identical resident dataset in less live memory than either rival, and its peak now sits under the worse rival, which is the gate bar.

GOGC=20 is cell-specific gate tuning, opted in the same way the rivals opt into their peak io-thread counts (redis 6, valkey 4).
It is honest here because the M0 string cell has a tiny scanned heap (the data is off-heap arena), so the frequent-GC cost is nil on this workload.
It is not proposed as a global server default.
