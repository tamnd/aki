# M0 headline re-run, 2026-07-10

Re-run of the headline cells at main `514d57b` on the gate box, replacing the contaminated rows from the first gate run (results/f3/m0-gate.md).
Between the two runs: #564 wake coalescing and the 64KiB reply buffer, #566 arena-view replies, #567 FLUSHALL, #569 arena reclaim, #570 allocator-held used_memory, #571 boundary-aware flush and unpinned workers.
Harness fixes since the gate run: aki-bench#40 warm windows and generator-bound detection, aki-bench#41 value-bearing gating; this run uses `-warm 3s`, 3 timed windows, none discarded, FLUSHALL between reps on all servers.

## Provenance

- aki `514d57b`, aki-bench `1450cb4`, sha256 in m0-rerun/binaries.sha256
- redis 8.8.0, valkey 9.1.0, io-threads 4, jemalloc, same builds as the gate run
- Box: i9-13900K 8P+16E, WSL2; servers taskset 0-7, generator 8-15
- Raw: m0-rerun/cells (per-rep JSON/CSV), m0-rerun/summary.txt, run scripts alongside
- Box copy: /root/f3gate/m0-rerun; the original gate tree /root/f3gate/m0 is untouched

## Ratio discipline

The summary.txt ratio column quotes the rival at min(aki-bench, redis-benchmark), which flatters aki: redis-benchmark is a closed-loop generator that underdrives the rivals by 20-45% here (the same artifact aki-bench#40 exists to catch; its detector fires on nothing in this run because aki-bench itself is not the bound).
The honest gate reading is the minimum of the per-harness ratios, each harness compared with itself.
That column is what the table below reports and what any pass/miss judgment uses.

## Results (min of per-harness ratios)

| cell | aki ab | rival ab | ab ratio | rb ratio | min ratio | old gate | p99 aki/redis (us) |
|---|---|---|---|---|---|---|---|
| SET 64B 1M uniform P16 | 4.65M | redis 4.84M | 0.96x | 1.15x | 0.96x | 0.93x | 4284 / 2226 |
| GET 64B 1M uniform P16 | 7.03M | redis 5.67M | 1.24x | 1.29x | 1.24x | 0.77x | 2775 / 1910 |
| INCR 1M P16 | 5.88M | redis 5.37M | 1.10x | 1.19x | 1.10x | 0.85x | 3217 / 1995 |
| SET 1KiB 1M P16 | 4.94M | redis 4.66M | 1.06x | 1.27x | 1.06x | n/a | 3625 / 2630 |
| GET 1KiB 1M P16 | 4.29M | redis 4.27M | 1.00x | 1.73x | 1.00x | 0.67x | 4694 / 2654 |
| GET 64B zipfian | 5.92M | redis 5.43M | 1.09x | n/a | 1.09x | n/a | 3228 / 1987 |
| single-hot-key SET P16 | 9.23M | redis 5.38M | 1.72x | 1.50x | 1.50x | n/a | 2040 / 2036 |
| SET 4KiB sustained | 1.80M | redis 1.57M | 1.15x | 1.27x | 1.15x | died (arena full) | 8987 / 8098 |

Rival is redis in every cell; valkey trails redis throughout.

## Reading

- Every headliner improved. GET 64B 0.77x to 1.24x, GET 1KiB 0.67x to 1.00x, INCR 0.85x to 1.10x. SET 64B is the one cell still under parity (0.96x, and 1.52x over valkey).
- The 4KiB sustained-overwrite cell that killed the gate run with `arena full` now runs 3x20s windows clean at 1.8M ops/s with arena_used flat around 296MB; #569's reclaim carries it.
- The transport analysis predicted the cheap wins would land around 0.9-1.1x on 64B reads; they landed at 1.24x. The remaining path to 2x on the uniform point cells is the M10 reactor: the deficit left is the serialized read/wake/write band that io-threads parallelizes for redis.
- Memory bar (RSS under 2x rivals): bytes/key by used_memory is below both rivals everywhere it is meaningful (GET 64B 113 vs 127/115, GET 1KiB 1073 vs 1327/1331), but RSS is over the bar on the preloaded GET rows: 550MB vs ~130MB at 64B (about 4x), 3.24GB vs ~1.33GB at 1KiB (about 2.45x). That is arena reservation showing up as touched pages, the same shape as the gate run; cap-aware provisioning is queued with the LTM residency slice.
- p99 is inside the 3x bar on every cell but no longer at-or-better than redis the way the 64B server2/3 runs showed; the WSL2 tails run wider. Worth a look when the reactor work starts.

## Caveats

- aki-bench SET/INCR windows only touch 50-90k distinct keys of the nominal 1M keyspace (all three servers equally), so bytes/key is meaningful only on the preloaded GET rows. Harness issue, filed to fix.
- redis-benchmark `--threads 4` numbers are generator-bound for the rivals in this run; they appear in the rb-ratio column (aki rb vs rival rb, self-consistent) but never as the rival's capacity.
- Single-run medians of 3 windows per cell; this is a perf-round checkpoint, not a gate run. The full matrix re-gate stays scheduled.
