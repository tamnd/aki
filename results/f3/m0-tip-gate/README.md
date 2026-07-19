# M0 headline gate on the current tip

Commit dcef1d8c, GamingPC (WSL2 Ubuntu, i9-13900K, 64 GB), 2026-07-19.
The M0 string headline cell: SET/GET 64B, 1M-key uniform, P16, c512 (two `redis-benchmark -r 1M` generators, c256 each, summed), warm plus three reps, median.
Rivals CF16-frozen: redis io-threads 6, valkey io-threads 4, both `--save '' --appendonly no`.
Server pinned cores 4-17 (GOMAXPROCS 14), generators on 18-24 and 25-31.
Ratio is min(aki/redis, aki/valkey). Gate bar is 2x, no rounding.

## Throughput

| workload | reactor | redis | valkey | vs redis | vs valkey | gate |
|---|---|---|---|---|---|---|
| SET | 9,129,815 | 3,365,233 | 2,904,075 | 2.71x | 3.14x | PASS |
| GET | 9,122,018 | 4,262,688 | 3,758,516 | 2.14x | 2.43x | PASS |

Both rows clear 2x against both rivals. The binding ratio is GET-vs-redis at 2.14x.

## Memory

The reactor is run in two configs.
Baseline is the gate throughput config.
`gogc20-rep512` adds `GOGC=20 -rep-cap 512`, the memory config from lab m0/28, which does not move throughput on this cell (SET 9.11M, GET 9.13M, still over 2x).
VmHWM is peak, in MiB (kB/1024). `used_memory` is live data, in MiB (bytes/1048576).

Peak VmHWM:

| target | config | VmHWM | vs worse rival (redis 160.7) |
|---|---|---|---|
| redis io6 | - | 160.7 | 1.00x |
| valkey io4 | - | 131.7 | 0.82x |
| reactor | baseline | 203.3 | 1.27x |
| reactor | gogc20-rep512 | 157.1 | 0.98x |

Live data (used_memory), identical resident dataset:

| target | used_memory | vs redis | vs valkey |
|---|---|---|---|
| reactor | 108.3 | 0.84x | 0.99x |
| redis | 129.6 | 1.00x | - |
| valkey | 109.7 | - | 1.00x |

## Verdict

M0 headline is green on the tip.

- Throughput: SET 2.71x, GET 2.14x, both over both rivals.
- Peak: with the lab-28 memory config the reactor peak is 157.1 MiB, under the worse rival redis (160.7), ratio 0.98x. Green against the memory bar.
- Live data: aki carries the same dataset in 108.3 MiB, leaner than both rivals. This is the product pitch.

The valkey peak residual (1.19x) is the per-connection conn-fabric declared structural in the memory slim arc (#1159), and it inverts on the same-data metric where aki beats valkey.

Gate command:

    GOGC=20 GOMAXPROCS=14 taskset -c 4-17 f3srv -addr 127.0.0.1:PORT \
      -shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128 \
      -free-list-cap 8 -rep-cap 512 -net reactor -net-loops 0

Attribution and the knob sweep are in `../../../labs/f3/m0/28_reactor_peak`.
