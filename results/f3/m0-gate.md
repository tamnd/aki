# f3 M0 exit gate, first run: full miss

Run date 2026-07-10, gate box GamingPC.
Every gated cell missed the 2.0x bar.
The in-memory matrix sits between 0.39x and 1.57x against the better rival, reads are behind Redis 8.8 outright, and the LTM pair is two orders of magnitude off as framed.
Two robustness failures surfaced: the arena never reclaims replaced values, so sustained SET churn at 4KiB and above ends in `ERR store: arena full`, and the value log grows dead bytes without compaction.
No cell result was tuned mid-run; the constants frozen before the run were used everywhere.
This note records the miss and the profile evidence, and hands M0 to a perf round.

Part of the M0 exit gate, tracking issue #542.

## Provenance

- Box: GamingPC, i9-13900K, 56 GiB RAM, 48 GiB swap, WSL2 kernel 6.18.33.2-microsoft-standard-WSL2, NVMe-backed ext4.
- aki commit 2e4c23fc151aa241bafc6daf2cccbf3a219aca43, aki-bench commit 7aa38c8396c56e173646811d37c536526ab2fa6d, go1.26.0.
- Rivals: Redis 8.8.0 (jemalloc 5.3.0, build 5f155628c849f81c), Valkey 9.1.0 (jemalloc 5.3.0, build 62d4a6f3cac454c6), redis-benchmark 8.8.0.
- Binary sha256: f3srv cdc9ceeb6704f57bf46fba8e823ac797fb0ef15801976bab6518aab1b2b1be28, aki-bench 2e305602a9105595a62b194ead9c17309a5a0db97d97dd0e0b6897f111a48f61, redis-server 3bea75409d8182add0459b2765cd62cd5962ffbcf8adb8ccf637205e70cbadb0, valkey-server 3ee5b35da0bd9916c7058c83221972d06fe69182e0790029a183ca8ffd0cf4af, redis-benchmark 6c5be4074733eaf19ceb212633dd9b0c699f5f17315fb61fa7ea50f74491babb.
- CPU split: servers pinned to CPUs 0-7, load generators to CPUs 8-15, same split for aki and both rivals, verified per process via taskset readback (CF2).
- WSL2 reports a synthetic topology (16 cores, 2 threads each, siblings n/n+16), not the physical 8P+16E layout, so the 0-7 half is what WSL2 presents as cores 0-7 with HT siblings excluded from both halves.
An empirical spin-rate probe across CPUs 0, 4, 8, 12, 20, 28 showed uniform rates, consistent with WSL2 virtualizing the topology; the physical P/E asymmetry is not observable from inside the guest.
- f3srv: 4 shards (the DefaultShards formula on the 8-CPU mask, one shard per data-plane core of the half per the frozen lab verdict), RESP2 readback confirmed on every cell (CF19, resp_ver 2 both directions).
- Rival io-threads frozen before the run by a sweep at SET/GET 64B P16 c512: redis io-threads 4 (3.32M SET), valkey io-threads 4 with io-threads-do-reads (2.21M SET).
Rivals launched fresh per cell in clean mktemp workdirs, write probe and role:master checked per cell (CF1).
- Bandwidth calibration (CF20): single-stream loopback 10.98 GiB/s, memcpy class 81 GB/s.
- Harnesses: aki-bench (closed loop, burst batch) and redis-benchmark with --threads 4 or 8 (open, adversarial), 1 discarded warm rep plus 3 measured reps per harness per cell, ratio is the min over rivals, then harnesses, then reps.
- Arena capacity was provisioned per value-size class before the run (512 MiB per shard for values up to 256B, 1 GiB at 1KiB, 2 GiB at 4KiB, 3 GiB at 64KiB and 1MiB, 256 MiB for LTM).
This is capacity provisioning, not tuning: the arena is a fixed-size allocation and the shipped 256 MiB default cannot hold the big-value datasets at all.
- Raw outputs: /root/f3gate/m0/ on the box (cells, meta with per-rep RSS, swap, io and CPU-jiffy readbacks, run log, profiles).
The raw tree is about 4 MB so it stays on the box; the reduced per-cell table is committed next to this note as m0-gate/cells.csv.

## Gate table

Bar: 2.0x over the min rival on the min harness with p99 inside 1.25x the best rival.
Columns: throughput as aki-bench/redis-benchmark medians of measured reps, ratio is min over rivals, harnesses and reps, p99 factor is aki p99 over the best rival p99 (worst harness).
aki has no used_memory surface yet, so its memory column is RSS; that gap is itself recorded.

| cell | aki ab/rb | redis ab/rb | valkey ab/rb | ratio | p99 x best rival | aki RSS | verdict |
|---|---|---|---|---|---|---|---|
| set_16b_1m | 4.82M/3.63M | 4.23M/3.20M | 3.00M/2.76M | 1.09x | 1.37 | 540 MB | MISS |
| set_64b_1m | 4.53M/3.63M | 4.79M/3.33M | 3.01M/2.76M | 0.93x | 1.66 | 590 MB | MISS |
| set_256b_1m | 4.45M/3.33M | 4.97M/3.33M | 2.92M/2.42M | 0.89x | 1.64 | 787 MB | MISS |
| set_1k_1m | 3.45M/2.50M | 4.84M/2.66M | 2.85M/2.00M | 0.71x | 1.86 | 1606 MB | MISS |
| set_4k_1m | 1.77M/- | 1.58M/- | 1.51M/- | 1.10x | 1.42 | 9108 MB | MISS |
| set_64k_100k | 0.26M/- | 0.16M/- | 0.16M/- | 1.57x | 1.38 | 15765 MB | MISS |
| set_1m_4k | 14k/- | 13k/- | 12k/- | 1.15x | 1.72 | 26455 MB | MISS |
| get_16b_1m | 4.80M/4.00M | 5.73M/4.70M | 4.31M/3.81M | 0.77x | 1.73 | 649 MB | MISS |
| get_64b_1m | 4.75M/3.63M | 5.70M/3.99M | 4.33M/3.80M | 0.83x | 1.73 | 850 MB | MISS |
| get_256b_1m | 4.36M/3.33M | 5.46M/3.07M | 4.20M/2.76M | 0.78x | 1.80 | 1621 MB | MISS |
| get_1k_1m | 2.80M/2.10M | 4.21M/2.10M | 3.35M/1.90M | 0.67x | 1.94 | 4922 MB | MISS |
| get_4k_1m | 1.31M/3.99M | 1.95M/1.23M | 0.82M/1.07M | 0.67x | 1.41 | 16495 MB | MISS |
| get_64k_100k | 0.19M/2.39M | 0.26M/0.17M | 0.30M/0.34M | 0.62x | 1.77 | 25553 MB | MISS |
| get_1m_4k | 13k/100k | 24k/14k | 21k/20k | 0.52x | 2.31 | 24976 MB | MISS |
| set_64b_1k | 4.50M/4.21M | 4.60M/4.44M | 3.06M/2.85M | 0.95x | 1.59 | 445 MB | MISS |
| set_64b_100k | 4.43M/3.81M | 5.00M/3.99M | 2.99M/2.85M | 0.86x | 1.68 | 466 MB | MISS |
| get_64b_1k | 4.64M/4.44M | 5.79M/5.32M | 4.29M/3.81M | 0.77x | 1.80 | 597 MB | MISS |
| get_64b_100k | 4.48M/3.81M | 5.87M/4.99M | 4.29M/3.63M | 0.74x | 1.88 | 622 MB | MISS |
| incr_1k | 4.67M/4.44M | 4.97M/4.70M | 4.30M/3.81M | 0.93x | 1.63 | 454 MB | MISS |
| incr_100k | 4.65M/4.00M | 5.53M/4.44M | 4.19M/4.00M | 0.81x | 1.74 | 457 MB | MISS |
| incr_1m | 4.77M/3.64M | 5.47M/4.20M | 4.15M/3.81M | 0.85x | 1.68 | 539 MB | MISS |
| set_64b_1m_zipf | 4.51M/- | 4.54M/- | 2.97M/- | 0.96x | 1.58 | 246 MB | MISS |
| get_64b_1m_zipf | 4.67M/- | 5.40M/- | 4.18M/- | 0.86x | 1.67 | 529 MB | MISS |
| set_1k_1m_zipf | 3.30M/- | 4.45M/- | 2.84M/- | 0.73x | 1.84 | 366 MB | MISS |
| get_1k_1m_zipf | 2.69M/- | 4.03M/- | 3.34M/- | 0.66x | 1.86 | 3704 MB | MISS |
| mset_64b_1m | 2.76M/1.14M | 2.37M/0.41M | 1.67M/0.50M | 1.11x | 1.50 | 2532 MB | MISS |
| mget_64b_1m | -/0.79M | -/0.47M | -/0.53M | 1.49x | 1.21 | 2570 MB | MISS |
| getrange_1k_1m | 4.40M/- | 5.22M/- | 3.96M/- | 0.84x | 1.70 | 1519 MB | MISS |
| getrange_64k_100k | 2.00M/- | 5.09M/- | 3.73M/- | 0.39x | 2.92 | 13001 MB | MISS |
| getrange_rb_1k | -/3.33M | -/2.66M | -/2.35M | 1.25x | 1.20 | 1497 MB | MISS |
| getrange_rb_64k | -/0.92M | -/2.39M | -/2.00M | 0.39x | 2.50 | 12968 MB | MISS |
| setrange_rb_1k | -/3.32M | -/2.85M | -/2.85M | 1.16x | 1.28 | 1306 MB | MISS |
| setrange_rb_64k | -/- | -/1.97M | -/1.99M | - | - | 12596 MB | FAIL (arena full) |
| append_rb_1k | -/2.98M | -/2.97M | -/2.99M | 1.00x | 1.21 | 3790 MB | MISS |
| append_rb_64k | -/- | -/1.58M | -/1.59M | - | - | 12586 MB | FAIL (arena full) |
| append_grow_1m | 4.49M/- | 5.35M/- | 3.52M/- | 0.82x | 1.73 | 4330 MB | MISS |
| hot_set | 8.91M/7.99M | 5.38M/5.00M | 3.12M/3.07M | 1.50x | 1.06 | 300 MB | MISS |
| hot_incr | 9.21M/8.88M | 5.92M/5.33M | 4.35M/4.20M | 1.50x | 1.07 | 300 MB | MISS |
| ltm_get | 40k/46k | 4.85M/2.98M | 4.15M/2.39M | 0.01x | 151.61 | 2272 MB | MISS |
| ltm_set | 64k/- | 4.10M/- | 2.72M/- | 0.02x | 73.38 | 1383 MB | MISS |

0 PASS, 39 MISS, 2 FAIL with no aki number.

Row notes:

- set_4k_1m, set_64k_100k, set_1m_4k: the redis-benchmark side of the aki runs failed with `ERR store: arena full`, so these ratios are aki-bench only.
- get_4k_1m, get_64k_100k, get_1m_4k: the redis-benchmark aki numbers are invalid because the rb preload had already hit arena full, so most GETs returned nil at wire speed; the recorded ratio comes from the aki-bench side, which preloads inside the run and verified the band.
The 64KiB and 1MiB GET rows are also bandwidth-influenced, see CF20 below.
- set_1m_4k and get_1m_4k ran at 64 connections; everything else at 512.
- zipfian rows and append_grow are aki-bench only (redis-benchmark has no zipfian mode); mget, getrange_rb, setrange_rb and append_rb rows are redis-benchmark arbitrary-command only (aki-bench has no such workloads).
Single-harness rows are marked by the dashes and their ratios carry that caveat.
- setrange_rb_64k and append_rb_64k have no aki number at all: every rep against aki failed with arena full while both rivals completed (redis 1.97M and 1.58M ops/s respectively).
Recorded as failures, not misses.
- hot_set and hot_incr drive one single hot key (aki-bench -keys 1; redis-benchmark without -r).
- p1_set_64b is the recorded-only P1 row, see the P1 section.

## Memory column

aki's memory column is red on its own, independent of throughput.
On get_64b_1m (full 1M preload) the RSS-derived cost is about 269 bytes per key against used_memory 128 (redis) and 115 (valkey) bytes per key; the F14 bar of within 25 percent of Valkey is missed by about 2.3x.
At 1KiB values aki is about 1943 bytes per key against 1328/1331, roughly 1.5x over the at-or-below bar.
Big-value cells are worse because the arena is provisioned up front and touched pages stay resident: 16.5 GB RSS on get_4k_1m and about 25 GB on the 64KiB and 1MiB rows, against sub-2 GB rivals at 4KiB.
f3srv exposes no used_memory in INFO yet, so all aki numbers here are RSS deltas over launch RSS; the missing surface is itself a gap this note records.

## Robustness failures

Arena exhaustion: full-replace SET is supposed to re-run band selection and reuse space, but replaced values are not reclaimed in this build, so any sustained random-key write load at 4KiB and above eventually gets `ERR store: arena full` (first seen on set_4k_1m under redis-benchmark, then on every bigger write row, then on ltm_set even with the vlog spill path active).
The server stays up and keeps answering; only writes fail.

Value log growth: each aki-bench rep re-preloads the keyspace, and the vlog grew from 519 MB to 1.55 GB across two reps of ltm_get with vlog_dead_bytes climbing and zero compaction runs.
Dead-byte reclamation exists in the design but did not engage.

## LTM pair

The LTM scenario as specified pits aki (1M keys at 1032 bytes, 512 MiB resident cap split 128 MiB across 4 shards, values in the vlog on NVMe) against rivals at maxmemory 512mb with allkeys-lfu.
The row uses 1032-byte values because 1024 bytes is exactly the embedded-band ceiling and never spills; a literal 1KiB row would be an in-memory run mislabeled as LTM.
The rivals never touched disk: they evicted roughly half the keyspace and answered from RAM (used_memory pinned at 525 MB, zero read_bytes), while aki honored retention and read 6.7 GB from the vlog during the GET reps.
Measured that way aki is 0.01x on GET (40k vs 4.85M) and 0.02x on SET, with p99 at 328 ms.
The comparison is honest about the numbers and lopsided in semantics: rivals answer nil for evicted keys, aki returns the data.
Two real problems remain regardless of framing: the vlog read path is about 100x too slow to gate (40k ops/s over 4 shards on NVMe implies synchronous single-read behavior with no batching or readahead), and aki RSS reached 2.27 GB against a 512 MiB resident-cap configuration, so the cap does not bound total process memory (index and per-connection buffers live outside it).
PRED-F3-M0-LTMSTR is judged on this row below.

## CF20 bandwidth flags

Calibrated single-stream loopback is 10.98 GiB/s.
On get_64k_100k the rivals moved 17-20 GB/s across 512 connections (multi-stream loopback scales past the single-stream figure), aki 12.5 GB/s.
These rows and get_1m_4k are flagged bandwidth-influenced: the rivals' numbers are near or past the calibrated ceiling, so the true compute gap at 64KiB and above is not resolvable on loopback and the ratios there are lower bounds on the confound, not clean compute ratios.
No row below 4KiB comes within 15 percent of the ceiling; the headline misses are compute, not bandwidth.

## P1 record (no gate)

SET 64B, 1M keys, pipeline 1, aki-bench closed loop, 3 reps each:

- 50 connections: aki 258-263k, redis 312-332k, valkey 579-600k; min ratio 0.43-0.45x.
- 512 connections: aki 321-322k, redis 1040-1044k, valkey 959-984k; min ratio 0.31x.
- redis-benchmark agrees: c512 aki 235k vs redis 666k.

The P1 recorded ratios on every other cell (each cell carried one P1 c512 rep) sit in the same 0.30-0.32x band.
P1 is where the hop architecture pays its full per-op latency with no batch to amortize it, and 512 closed-loop connections at 322k ops/s means about 1.6 ms per op of queueing plus hop latency.

## Prediction judgments

- PRED-F3-M0-SPREAD: falsified, and not near the line.
Floor was the K2 carried cells (SET 10.79M at 5.12x, GET 10.25M at 4.23x, INCR 11.30M at 4.81x); measured SET 64B is 4.53M at 0.93x, GET 4.75M at 0.77x on the min harness view (ab), INCR 4.77M at 0.85x.
aki's absolute throughput is 2.2-2.4x below the K2 floor while the rivals roughly doubled against the f1-era denominators (redis 8.8 with io-threads 4 does 4.8-5.7M here).
Both halves of the miss are real: aki got slower than the f1 substrate and the rivals got faster.
- PRED-F3-M0-HOT: middle band.
Floor 2.0x missed; measured 1.50x on both hot rows (min over harness and rival, aki 8.9-9.2M vs redis 5.4-5.9M).
Per the prediction's own terms the 1.5-2.0 band engages F13 same-key batching rather than failing the milestone outright, and 1.50x sits exactly on the band edge; recorded as band-entry, still a gate MISS.
- PRED-F3-M0-BIGVAL: falsified.
2x with bounded RSS was predicted; measured 1.57x best case (set_64k, ab only after the rb side died on arena full), 0.39-0.62x on big reads, RSS 12-26 GB, and the rows are bandwidth-confounded on top.
The falsifier condition (whole-value buffering) was not directly observed in the profile, but the RSS behavior says the streaming window does not bound memory in practice.
- PRED-F3-M0-LTMSTR: falsified as measured.
Floor 2x; measured 0.01x-0.02x against rivals that evict instead of retaining.
Even granting the semantic gap, the absolute vlog read rate (40k ops/s) cannot clear any version of this row.
- PRED-F3-M0-P1REC: falsified.
Expected band 1.4-2.4x at 50 connections; measured 0.43-0.45x.

## Profile evidence

Profiles taken after the matrix on the two directed miss cells, f3srv rebuilt with a pprof listener (scratch build, same commit, not committed), driven by redis-benchmark GET at P16 c512 threads 8 for 30 s.
perf is not available on this WSL2 image, so the redis comparison is throughput-only: under the identical sustained drive redis served 2.13M ops/s at 1KiB while the instrumented f3srv served 2.49M, which also shows the gate-cell gap (0.67x) is wider than the steady-state engine gap; more on that in suspects.

get 64B, top of 200 s of samples across 8 CPUs (f3prof, 30 s wall):

```
      flat  flat%   sum%        cum   cum%
    60.92s 30.44% 30.44%     60.92s 30.44%  internal/runtime/syscall/linux.Syscall6
    53.36s 26.66% 57.11%     53.36s 26.66%  time.runtimeNow
    16.79s  8.39% 65.50%     18.36s  9.17%  store.(*Store).recordMatches (inline)
     5.38s  2.69% 68.18%     39.56s 19.77%  shard.(*worker).drainAndExecute
     5.36s  2.68% 70.86%      5.36s  2.68%  runtime.futex
     5.12s  2.56% 73.42%      5.12s  2.56%  runtime.memmove
     2.77s  1.38% 74.81%     56.13s 28.05%  time.Now
     1.80s   0.9% 75.70%      2.73s  1.36%  sync/atomic.(*Pointer[...]).Store (inline)
     1.65s  0.82% 76.53%      1.78s  0.89%  store.Hash
     1.65s  0.82% 77.35%      5.70s  2.85%  runtime.chanrecv
     1.65s  0.82% 78.18%      1.65s  0.82%  sync/atomic.(*Pointer[...]).Load (inline)
     1.40s   0.7% 81.07%        20s  9.99%  store.(*Store).findEntry
```

get 1KiB, same drive:

```
    80.70s 39.62% 39.62%     80.70s 39.62%  internal/runtime/syscall/linux.Syscall6
    31.66s 15.54% 55.16%     31.66s 15.54%  time.runtimeNow
    21.69s 10.65% 65.81%     21.69s 10.65%  runtime.memmove
    20.59s 10.11% 75.92%     22.01s 10.81%  store.(*Store).recordMatches (inline)
     3.98s  1.95% 80.03%     46.01s 22.59%  shard.(*worker).drainAndExecute
     1.89s  0.93% 80.96%     33.55s 16.47%  time.Now
     1.50s  0.74% 82.49%     70.93s 34.82%  shard.(*Conn).deliver
```

Full tables are committed next to this note (m0-gate/get64b.top.txt, m0-gate/get1k.top.txt) and the pprof binaries stay on the box.

Mutex and block profiles: contention is concentrated in the waker (runtime.unlock under waker.wake at 32 percent of mutex delay, waker.park via Conn.Wait at 27 percent); block time is 99 percent idle parking in Conn.Wait, which is expected shape, not a finding.

Atomics check: the automated LOCK-prefix disasm scan came back malformed (the parser did not survive pprof's routine separators), so the spec assertion of no atomics beyond the inbound queue is checked from the CPU profile instead: sync/atomic Pointer Load/Store show up inline on the hot path at about 1.7 percent combined, plus runtime.futex at 2.7 percent from the waker.
That is more than the inbound queue alone; the assertion does not hold as stated and the exact call sites need a follow-up with a fixed scan.

Alloc and hop evidence (committed as m0-gate/benches.txt):

- TestDrainedPathZeroAllocs passes at commit 2e4c23f: the drained path is allocation-free.
- BenchmarkDrainExecute: 1483-1501 ns/op, 0 allocs.
- store BenchmarkGet 143.6-144.6 ns/op, BenchmarkSet 149.2-153.5 ns/op, 0 allocs.

The engine core does a GET in 144 ns; the same op through the hop costs about 1.5 us in the bench and the server serves 64B GETs at 4.75M ops/s across 8 CPUs, which is about 1.7 us of CPU per op.
The gap between 144 ns and 1.7 us is the whole story of the miss.

## Suspects, in order

1. time.Now on the data path: 28 percent of GET CPU at 64B, 16 percent at 1KiB.
Something calls the wall clock per operation (expiry checks or stats are the obvious candidates).
Cache a coarse clock per drain batch and this is the single largest recoverable block.
2. Per-op hop cost: BenchmarkDrainExecute says 1.5 us per op through the queue against 144 ns in the engine, a 10x tax that P1 exposes brutally (0.31x) and P16 cannot fully amortize.
The waker futex traffic (32 percent of mutex delay in wake) is part of this; the batch path is amortizing wakeups less than the design assumed.
3. store.recordMatches: 8-11 percent flat and inlined into the lookup, growing with value size; whatever match bookkeeping it does is the second engine-side block after the clock.
4. Syscall time (30-40 percent) is high but the rivals pay the same wire; with io-threads 4 they overlap it better than the current reader/writer goroutine layout, visible in aki's p99 running 1.4-1.9x redis on every in-memory row (2.9ms vs 1.9ms class on get_64b).
5. The steady-state instrumented server beat redis at 1KiB (2.49M vs 2.13M) while the gate cell shows 0.67x; the gate cells interleave aki-bench preloads, FLUSHALL rejections and reconnect storms per rep, so connection setup and preload-adjacent behavior contribute to the measured gap beyond the steady state.
Worth a dedicated look before the next run.
6. Memory: 269 B/key at 64B (2.3x valkey), the arena never reclaims, the resident cap does not bound RSS, and there is no used_memory surface.
These are correctness-of-accounting items, not tuning.

## Sweep appendix (non-gating)

The sweep rows ran once each (1 warm, 1 measured, aki-bench only) and agree with the gated matrix: sweep_set_256b_zipf 0.94x, sweep_get_256b_zipf 0.77x, sweep_set_4k_zipf 1.06x, sweep_get_4k_zipf 0.66x, sweep_set_64b_c50 0.79x, sweep_get_64b_c50 0.69x, sweep_mixed_64b 0.83x.
The supplementary all-32-CPU rows (server on 0-15, clients on 16-31, marked, outside the gate protocol) read 1.15x SET and 1.10x GET, consistent with the halved-box picture.
