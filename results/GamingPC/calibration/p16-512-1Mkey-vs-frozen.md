# M0 headline gate: P16/512, 64B, 1M-key uniform, vs CF16-frozen rivals

GamingPC, 2026-07-12. aki = the #653 adaptive conn-writer-spin build (f3srv-cs,
commit 6a82a29). Rivals CF16-frozen (io-threads.txt): redis 8.8.0 io-threads=6,
valkey 9.1.0 io-threads=4. Server cores 4-17 (8 shards, GOMAXPROCS 14). This is
the cell M0.md line 49 gates on: SET/GET at 64B, 1M keys uniform, P16/c512.

## The client-cap confound that hid this result

A single `redis-benchmark ... -r 1000000` process hard-caps at ~6.64M ops/sec
regardless of `--threads` (14/16/20/24 all read 6.64M) because generating a
fresh random key per command is a per-client-process ceiling, not a server
limit. aki's server can do far more, so one `-r` client under-measures aki by
~42% while the slower rivals (3.3M / 2.7M, below the cap) stay honest. Every
prior M0 "point-op ~1.4-1.8x" number that used a single `-r` client or the
single-hot-key regime was measuring the wrong thing:

- single hot key (no `-r`): every write lands on ONE key -> ONE shard, so aki
  serializes and reads ~9.95M SET; the 1M-key spread lets all 8 shards run.
- single `-r` client: caps the harness at 6.64M, below aki's real throughput.

Fix: drive every server with TWO concurrent `-r 1000000` clients (cores 18-24
and 25-31, each c256/threads7/n10M) and SUM their rps. Confirmed three ways that
aki's true throughput is ~11.4M: no-`-r` control 11.34M, two-client no-`-r`
11.39M, two-client 1M-key 11.40M all agree.

## Throughput (median-of-3, 2-client summed, n=10M/client)

| | aki-go | redis io=6 | valkey io=4 | min ratio |
|---|---|---|---|---|
| SET | 11,398,260 | 3,329,172 | 2,661,875 | **3.42x** |
| GET | 11,400,884 | 4,438,034 | 3,802,281 | **2.57x** |

Rivals are honestly server-bound: their two-client sums equal their single-client
`-r` numbers (redis 3.33M both ways), so a second client adds no throughput to
the single-threaded executors. Both cleared 2x decisively. The win is
architectural: aki spreads 1M keys across 8 parallel shard workers while the
single-threaded rivals (io-threads parallelize socket I/O only, never command
execution) eat cache misses on the 1M-entry hash table. Under 1M-key uniform aki
RISES vs the hot-key case (SET 9.95M -> 11.40M) while both rivals FALL vs the
hot-key case (redis SET 4.99M -> 3.33M).

## Why P16 passes where P1 does not

P1 (no pipelining) is bound by the cross-goroutine dispatch floor: every op
crosses reader -> shard-worker -> back = two futex wakes a single-thread C event
loop never pays, so aki's P1/512 edge is only ~1.4x (p1-512-vs-frozen.md). P16
amortizes that handoff across 16 commands per syscall, so aki's parallel
execution dominates and the ratio jumps to 2.57-3.42x. M0.md gates P16 (PRED-F3-
M0-SPREAD, line 41), so the P16 cell is the exit gate; P1 is its own harder
regime (doc 08).

## Memory bar: FAILS at this cell (the now-binding blocker)

M0.md line 49 requires ">= 2x with the memory column green". Throughput is green;
memory is RED. Peak VmHWM after a 5M-SET load of the 1M keyspace:

| | aki-go | redis io=6 | valkey io=4 |
|---|---|---|---|
| VmHWM (load only) | 190,840 kB | 151,024 kB | 125,920 kB |

aki = 1.26x redis, 1.52x valkey. The HARD bar wants aki BELOW the rivals (ideal
0.5x). GOGC is NOT the lever: sweeping GOGC 100 -> 20 leaves throughput flat at
11.4M and barely moves VmHWM, proving the overhead is the manually-managed
per-shard arena + index (8 copies of the fixed structures), not Go GC headroom.
Lowering it is a structural labs task (shared arena / tighter index / slab
sizing / shard count), owed a labs/f3 microbench per the memory-bar rule. aki has
2.5-3.4x throughput headroom to spend on memory and still clear 2x.

## Verdict

The M0 headline THROUGHPUT gate PASSES at the mandated cell (SET 3.42x, GET
2.57x vs CF16-tuned rivals) - a real result, not the prior "1.4-1.8x", which was
a wrong-cell / client-cap artifact. The cell is NOT fully green: the memory bar
fails (aki 1.26-1.52x rival RAM). The open M0 lever flips from throughput to
memory: shrink aki's per-shard arena/index footprint below the rivals while
holding >2x.
