# f3 M2 zset exit gate: ZADD clears 2x, range reads regress under 1x, the rest sit below the floor

Run date 2026-07-12, gate box GamingPC.
Flat ZADD clears the bar at 3.4x to 3.8x, but that is the only family over 2.0x.
The point reads (ZSCORE, ZCARD, the cardinality-band ZSCORE) land 1.18x to 1.95x, short of the floor on the reactor dispatch ceiling.
ZRANK sits at descent parity with the skiplist, 0.95x on the uniform draw and 1.12x on the zipf draw, so PRED-F3-M2-ZRANKZIPF is missed below its own F13 band on the c1m cell.
The headline miss is ZRANGE and ZRANGEBYSCORE: aki runs them at 0.50x to 0.72x, slower than Valkey, a regression from v1's 1.59x to 2.20x, so PRED-F3-M2-ZRANGE is falsified.
ZUNIONSTORE over small inputs is pathological, 2.76 ops/s against 25k, an identical fixed cost whether the union is 256 or 1M wide, which traces to the same fixed collection-creation cost that puts peak VmHWM at 2.4x to 3.8x the rivals even though aki's marginal bytes per entry is leaner than both.
The one clean win beyond ZADD is the ZREM tail: the v1 7.6-8.1ms shoulder is gone, aki's p99 now beats both rivals, so PRED-F3-M2-ZREMTAIL's shoulder clause clears even though the throughput floor does not.
No constant was tuned mid-run; the frozen thresholds from the M2 labs were used everywhere.
This note records the numbers, adjudicates the five filed predictions, and hands the misses to follow-ups.

The run used the main tip at M2 slices 1 through 7 merged, before the #646 count-prefix slice landed; the #646 end-to-end delta on zrank_zipf_c1m is a separate box A/B, filed as a follow-up below.

Part of the M2 exit gate, tracking issue #544.

## Provenance

- Box: GamingPC, i9-13900K, 56 GiB RAM, WSL2 kernel 6.18.33.2-microsoft-standard-WSL2, 32 logical CPUs, NVMe-backed ext4.
- aki commit f53ce7995d9fad023c8c43a9e79fa20e70bfc95b (main tip at run start, M2 slices 1-7 merged, pre-#646), go1.26.0.
- f3srv sha256 2bc1b6cae14364ba97686f5259d3985abb9a29db69e141abbb4120ef65a47c2a, built linux/amd64 at that commit.
- aki-bench commit dd865386b30f6e305e9f95ed7b0a9317bdd12ff8, sha256 971c7cb62b29828b4dfa584f5ceadfe8c40a18abb20cdc4e54a0bad8a685e4d0; the driver script is committed next to this note as m2-gate/runner.py so the cell definitions are reproducible.
- Rivals: Redis 8.8.0 (jemalloc 5.3.0, build 5f155628c849f81c), Valkey 9.1.0 (jemalloc 5.3.0, build 62d4a6f3cac454c6), both with io-threads 4, launched fresh per batch in clean workdirs with persistence off.
- CPU split: servers pinned to CPUs 0-7 via taskset, the load generator to CPUs 8-15, same split for aki and both rivals.
- f3srv shards: 4 for the main matrix (the DefaultShards formula on the 8-CPU mask); 1 for the algebra cells, because f3srv routes keys by hash with no hash-tag support and the two union operands must co-locate on one owner. That single-shard framing understates aki on algebra relative to a co-location surface, and the rivals are single-threaded on the data plane anyway, so the algebra ratios are honest per-core numbers.
- Harness: aki-bench connect mode, closed loop, one process driving all three servers interleaved per cell, 3 measured reps per cell, warm inside each rep, fresh FLUSHALL plus preload per rep. Ratio is the minimum over both rivals and then over reps.
- Default drive is 512 connections at pipeline 16. The range cells (ZRANGE, ZRANGEBYSCORE) run at 16 connections pipeline 1 to bound the per-op 100-element reply array; the algebra cells run at 8 connections pipeline 1. The reduction is the same for all three servers per cell.
- Memory: VmHWM per server from /proc with a clear_refs peak reset before every rep. The marginal bytes-per-entry rows below are (mem at c1m minus mem at c10k) over the 990k added entries, which cancels the fixed server arena and isolates the per-entry cost.
- Per-rep watchdog: a 240s timeout kills a hung aki-bench and skips the rep, so one slow cell cannot wedge the campaign. Only the zrangebyscore_c1m preload ever approached it (a 1M-member load at pipeline 1); no measured rep was truncated.
- Raw per-cell JSON stays on the box under /root/f3gate/m2m3/m2/cells/; the reduced tables are transcribed here.

## Gate table

Bar: 2.0x over the worse-for-aki rival, minimum over reps.
Ops columns show the rep that produced the minimum ratio.
Members is the sorted-set cardinality under test; val is the member-byte width the cell drives.

Point reads (ZSCORE, cardinality-band ZSCORE, ZMSCORE, ZCARD):

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| zscore_c1 | 1 | 8.39M | 6.03M | 3.96M | 1.39x | MISS |
| zscore_c10 | 10 | 8.07M | 6.14M | 3.99M | 1.32x | MISS |
| zscore_c10k | 10k | 7.47M | 5.66M | 3.82M | 1.32x | MISS |
| zscore_c1m | 1M | 2.75M | 2.34M | 2.34M | 1.18x | MISS |
| band_100 | 100 | 9.36M | 5.99M | 4.19M | 1.56x | MISS |
| band_500 | 500 | 8.71M | 5.82M | 4.07M | 1.50x | MISS |
| band_2000 | 2000 | 8.22M | 5.71M | 3.93M | 1.44x | MISS |
| band_130k | 130k | 8.04M | 4.13M | 3.24M | 1.95x | MISS |
| band_300k | 300k | 5.44M | 3.14M | 2.79M | 1.73x | MISS |
| zmscore_c10k | 10k | 250k | 252k | 243k | 0.99x | MISS |
| zmscore_c1m | 1M | 127k | 67k | 98k | 1.30x | MISS |
| zcard_c10k | 10k | 10.62M | 7.42M | 4.72M | 1.43x | MISS |
| zcard_c1m | 1M | 10.85M | 7.33M | 4.73M | 1.48x | MISS |

Rank (ZRANK uniform and zipf s=0.99):

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| zrank_c10k | 10k | 3.74M | 3.96M | 3.04M | 0.95x | MISS |
| zrank_c1m | 1M | 1.30M | 1.22M | 1.37M | 0.95x | MISS |
| zrank_zipf_c10k | 10k | 6.41M | 4.08M | 3.04M | 1.57x | MISS |
| zrank_zipf_c1m | 1M | 2.22M | 1.98M | 1.99M | 1.12x | MISS |

Range reads, reduced concurrency (ZRANGE by rank, ZRANGEBYSCORE, 100-element windows):

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| zrange_c10k | 10k | 194k | 244k | 385k | 0.50x | MISS |
| zrange_c1m | 1M | 95k | 113k | 174k | 0.55x | MISS |
| zrangebyscore_c10k | 10k | 179k | 235k | 248k | 0.72x | MISS |
| zrangebyscore_c1m | 1M | 91k | 109k | 170k | 0.54x | MISS |

Writes and updates (ZADD member, ZINCRBY, ZREM hot, flat ZADD):

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| zaddmember_c1 | 1 | 8.58M | 4.96M | 3.32M | 1.73x | MISS |
| zaddmember_c10k | 10k | 7.76M | 4.49M | 3.21M | 1.73x | MISS |
| zaddmember_c1m | 1M | 2.47M | 2.09M | 2.07M | 1.19x | MISS |
| zincrby_c10k | 10k | 8.03M | 4.16M | 3.12M | 1.93x | MISS |
| zincrby_c1m | 1M | 2.50M | 2.03M | 2.03M | 1.23x | MISS |
| zrem_hot | 2M | 1.28M | 0.98M | 1.07M | 1.20x | MISS |
| zadd_flat_10k | 10k | 3.75M | 1.09M | 1.06M | 3.44x | PASS |
| zadd_flat_1m | 1M | 3.77M | 1.00M | 0.97M | 3.78x | PASS |

Algebra, single shard (ZUNIONSTORE of two equal-size sets, WEIGHTS 1 1):

| cell | operands | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| zunion_256 | 256 + 256 | 2.76 | 25101 | 19745 | 0.0001x | MISS |
| zunion_1m | 1M + 1M | 2.76 | 1.7 | 2.1 | 1.32x | MISS |

ZREM tail latency (the PRED-F3-M2-ZREMTAIL shoulder clause), p99 microseconds, minimum-ratio rep:

| server | p99 |
|---|---|
| aki | 7938 |
| valkey | 8830 |
| redis | 10592 |

aki's p99 is below both rivals; the v1 7.6-8.1ms deferral shoulder is gone.

Memory, marginal bytes per entry (mem at c1m minus mem at c10k over 990k entries) and peak VmHWM multiple at c1m:

| op family | aki B/entry | redis B/entry | valkey B/entry | aki peak VmHWM vs best rival |
|---|---|---|---|---|
| ZRANGE zset | 10.3 | 69.9 | 46.6 | 3.06x |
| ZRANK zset | 29.0 | 71.5 | 33.3 | 3.77x |

aki's marginal per-entry cost is leaner than both rivals; the peak-VmHWM multiple is fixed arena, not data (see the memory verdict below).

## Prediction adjudication

PRED-F3-M2-ZRANKZIPF (floor 2x, 1.5-2.0x engages F13, below 1.5x is a miss).
Falsified on the c1m cell. zrank_zipf_c1m read 1.12x, below the F13 band, a hard miss; zrank_zipf_c10k read 1.57x, inside the F13 band. The gap is structural: a counted B+ tree ZRANK and a skiplist rank are both O(log n) and both cache-resident on the zipf hot set, so they sit at descent parity, and the M2 lab 06 (#646) showed a count-accumulation constant factor does not close a 2x on a parity descent. Handed to the descent-structure follow-up.

PRED-F3-M2-ZRANGE (floor 2x, v1 read 1.59-2.20x).
Falsified, and a regression. All four range cells run 0.50x to 0.72x, aki slower than Valkey. Valkey is the binding rival and is markedly faster than Redis on the windowed range (385k vs 244k on zrange_c10k), so the bar moved up while aki moved down. The 100-element reply is small, so this is the leaf-walk plus RESP-encode path, not a reply-size artifact. This is the largest M2 miss and the top follow-up.

PRED-F3-M2-ZSETMEM (tree overhead 2-3 B/entry, over 5 B/entry blocks; watch item 40.2 B/entry on darwin).
The per-entry prediction holds: aki's marginal cost is 10.3 B/entry on the ZRANGE family and 29.0 B/entry on the ZRANK family (member plus score plus tree overhead), leaner than both rivals (33-72 B/entry) and far under the darwin 40.2 watch number. But the peak-VmHWM bar is missed: aki holds a 1M-member set at 2.4x to 3.8x the rivals' peak, all of it fixed arena and connection preallocation, not data. So the tree meets its overhead prediction while the process fails the peak-memory bar on a fixed cost that does not scale down. Split verdict, both recorded; the fixed-arena reduction is a follow-up (and is the same root cause as the ZUNIONSTORE pathology).

PRED-F3-M2-ZREMTAIL (floor 2x, p99 inside 125 percent of best rival, v1 shoulder must be gone).
Split. The p99 shoulder clause clears: aki's p99 is 7938us against Valkey's 8830us and Redis's 10592us, so the shoulder is not merely inside 125 percent of the best rival, it is under it, and lab 04's read of the v1 shoulder as a deferral artifact is confirmed by the #619 inline delete path. The 2x throughput floor is missed at 1.20x.

PRED-F3-M2-ZADD (hold or improve K4's carried 5.49-7.03x, any regression blocks).
Ambiguous, flagged. On the gate protocol (min over both rivals, connect mode, 512c/p16) flat ZADD clears the 2x milestone bar comfortably at 3.44x (10k) and 3.78x (1m), the only family to do so. But that is below K4's pinned 5.49-7.03x, which the prediction says blocks. K4's band was measured under a different framing than this connect-mode min-over-both-rivals protocol, so the two numbers are not directly comparable; the gate bar is met, the pinned band is not, and the framing needs reconciling before this is called a regression. Recorded, not adjudicated as a block.

## Verdict

M2 does not pass the 2.0x gate. Flat ZADD is the only family over the floor. The store is competitive but short of 2x everywhere else, and it regresses below 1x on range reads.

Misses handed to follow-ups, ranked by size of the lever:

1. Range reads (ZRANGE, ZRANGEBYSCORE) at 0.50-0.72x, a regression. The leaf-walk plus RESP-encode path is 2x slower than Valkey on a 100-element window. Biggest lever, top priority. Book on #544.
2. Fixed collection-creation cost. One root cause behind two misses: the peak-VmHWM multiple (2.4-3.8x, all fixed arena) and the ZUNIONSTORE small-input pathology (2.76 ops/s regardless of 256 vs 1M operands, an identical fixed per-op cost). ZUNIONSTORE creates a fresh destination zset each probe, so it pays the fixed arena setup every op; on the 1M union that cost hides under the merge and aki reaches 1.32x, on the 256 union it is the whole cost and aki is ~9000x slower. Shrink or lazily grow the per-zset arena. Book on #544; verify the ZUNIONSTORE root cause with a direct micro-timing.
3. Point-read dispatch floor. ZSCORE and ZCARD at 1.18-1.95x is the reactor dispatch ceiling (~1.5x measured in M0), not the zset store. The io_uring driver (task #9) is the untested lever here.
4. Rank descent parity. ZRANK at 0.95-1.12x is structural O(log n) vs the skiplist; the #646 SWAR count-prefix is a modest help. Owed: the box A/B of zrank_zipf_c1m on the pre-#646 vs post-#646 f3srv, to measure the shipped slice end to end.
