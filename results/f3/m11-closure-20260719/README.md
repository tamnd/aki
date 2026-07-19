# f3 closure verdict (M11-R2): the full 2x-gate campaign

This is the last row of the f3 gate campaign.
It closes M11 and, with M0 through M10 already DONE, the campaign as a whole.
Tip 3790e56, gate box i9-13900K (WSL2 Ubuntu, 14 pinned cores, idle), rivals redis 8.8.0 and valkey 9.1.0, frozen at the CF16 config (redis --io-threads 6, valkey --io-threads 4 --io-threads-do-reads yes), aki under the reactor at GOGC=20.

The gate law: a cell passes only when aki beats **both** rivals by 2x, ratio = min(aki/redis, aki/valkey), median-of-3.
The memory bar runs alongside: aki's peak VmHWM must stay at or under the worse rival for the same resident data, ideal 0.5x, and less-memory-for-the-same-data is the product pitch.
A row is RESOLVED when it is GREEN (passes 2x) or carries a STRUCTURAL verdict with box or lab evidence and a committed CSV.
A milestone is DONE when every non-checkbox row is resolved.

## The ledger

| milestone | rows | verdict |
|---|---|---|
| M0 strings | 10 | DONE. Binding trio G1/G2/G3 green (SET 2.33x/3.00x, GET 2.00x/2.33x, memory 0.98x redis peak / live-data win). Large-value bands declared bandwidth-bound, batch/grow variants near-2x coverage. |
| M1 set | 10 | DONE. Point ops + read-algebra pass 2x (SISMEMBER 2.92x, SADD 2.71x, SMEMBERS 2.29x, SINTER 2.25x, SRANDMEMBER 7.60x). Bulk-append + STORE + SMOVE-cross structural. Tiny-set memory structural (three-heap-object wall). |
| M2 zset | 10 | DONE. Point ops pass (ZSCORE 2.60x, ZPOPMIN 2.00x). Range reads dispatch-floor (valkey pass, redis skiplist ceiling). STORE compute-plus-write structural. Tiny-zset memory 1.80x structural (inline score codec). |
| M3 list | 10 | DONE. LINDEX 2.42x, LRANGE 2.42x, LSET 3.66x pass (packed-cursor edge). Point-writes + O(n) mutations (LINSERT/LREM) declared floor. Tiny-list memory structural. |
| M4 hash | 10 | DONE. Point ops pass (HGET 2.60x, HDEL 2.00x, HINCRBY 2.99x, HRANDFIELD 2.33x). HGETALL 2.08x pass. HMGET batch-read floor structural. Field-TTL correctness green. Tiny-hash memory 1.53x structural. |
| M5 stream | 8 | DONE. XLEN 3.33x pass, memory 0.13x PASS (7.5x less). XRANGE/XREAD win but sub-2x (wide-reply bandwidth floor). XADD write-floor structural. XREADGROUP/XTRIM correctness green. |
| M6 bitmap/HLL/geo | 8 | DONE. Point ops track the string/zset gate (no type overhead). BITOP/PFMERGE cross-shard-streaming structural. Sparse-bitmap + HLL memory 0.007x PASS (143x less). |
| M7 LTM | 6 | DONE. Both arms re-confirmed on the tip: equal-data 0.170x/0.274x peak, equal-cap 5.67x/3.59x coverage at equal memory. Write-path, peak-overshoot, read-tail declared structural (larger-than-memory-tier costs). See results/f3/m7-close-20260719. |
| M8-M11 | 5 R-rows | DONE. Regression-green on the final tip (point rows flat at the reactor ceiling after durability, expiry, and the box-free command closure landed). This verdict is M11-R2. |

Every milestone is DONE. Full per-row detail in the gate spec (2064/f3/gates/04-milestone-gates.md) and the per-milestone results directories under results/f3/.

## What passes 2x outright

The headline is green and holds through the whole build.
On the 1M-key dual-generator protocol, aki serves the string and collection point path at the reactor keyspace ceiling (7.96-7.98M ops), a flat 2.00-3.66x over both rivals:

- Strings: SET 2.33x/3.00x, GET 2.00x/2.33x.
- Collection point reads: SISMEMBER 2.92x, ZSCORE 2.60x, HGET 2.60x, LINDEX 2.42x, XLEN 3.33x.
- Point mutates: HDEL/ZREM/ZPOPMIN 2.00x, HINCRBY 2.99x, LSET 3.66x.
- Reply families: SMEMBERS 2.29x, HGETALL 2.08x, SRANDMEMBER 7.60x, SINTER 2.25x.

The 2x comes from the reactor networking edge: a fixed per-op dispatch/syscall saving that a keyspace lookup and a small reply cannot dilute.

## The memory bar

This is where aki wins hardest, and it is the product pitch.
On data-bearing shapes aki uses a fraction of a rival's resident memory for the same data:

| shape | aki | redis | valkey | ratio |
|---|---|---|---|---|
| 1M keys x 1032 B (LTM equal-data) | 354 MiB | 2078 MiB | 1290 MiB | 0.170x / 0.274x |
| 100k streams x 8 entries | 58 MiB | 440 MiB | 432 MiB | 0.13x / 0.14x |
| sparse bitmaps + HLL | 43 MB | 6.06 GB | 6.06 GB | 0.007x |

And at an equal memory budget the same mechanism becomes a coverage win: give all three 354 MiB and aki keeps 100% of a 1M keyspace while redis keeps 17.6% and valkey 27.9%, because aki spills cold values to disk and the rivals evict them.

The one place the bar inverts is tiny collections held purely in RAM (100k collections of 8 elements): aki runs 1.5-1.8x a rival's listpack there, because a live tiny collection is three heap objects (a map entry, a separately allocated struct, the copied key) where a rival packs one contiguous dict entry.
This is declared structural against the cross-type keyspace-unification arc, the one representational change that closes it; struct-slim and map-value-inline were both proven insufficient.

## The structural declarations, and why they are honest

Roughly half the rows are declared structural rather than green.
Each declaration names a specific physical cost, ties it to a box or real-store lab, and shows it is intrinsic, not a skipped optimization:

- **Collection write floor** (SADD/ZADD/HSET/LPUSH/XADD point and bulk): a per-op keyspace lookup plus an index insert dilutes the reactor edge to 1.2-1.8x. Box CPU profiles show ~30% non-syscall compute the rivals also pay; no cheap per-row lever cuts half of it.
- **Range-read dispatch floor** (ZRANGE/ZRANGEBYSCORE/ZRANGEBYLEX): valkey passes, redis is a skiplist-range ceiling. 2x of redis's efficient inline-member range exceeds aki's own fastest contiguous range-reply ceiling (LRANGE), so no zset walk change reaches it.
- **Batch-read floor** (HMGET/ZMSCORE): a ~6 KiB windowed array reply is below the streaming cutover, encode-and-dispatch bound where redis is maximally efficient.
- **O(n) list-mutation floor** (LINSERT/LREM): the packed layout that wins LINDEX/LRANGE/LSET makes a mid-list shift O(n); the same trade, inverted.
- **Wide-reply bandwidth floor** (XRANGE/XREAD): aki is the fastest of the three, but the ~900 KB reply is memory-bandwidth bound near a shared loopback ceiling, so the win is sub-2x.
- **Cross-shard streaming floor** (SMOVE/BITOP/PFMERGE cross): the spec carves the F17 intent-hop route out of the 2x point gate; these are scored with the streaming family, not the point path.
- **LTM larger-than-memory costs** (M7 G3/G4/G5): the spill write is a sequential-write floor, the arena overshoot is bounded churn headroom that reclaim returns, the read p99 is a disk-read tail. Each is the counterpart of a rival either dropping the data or paying 4-6x the memory to keep it. See results/f3/m7-close-20260719.

The test throughout is the fair-proof rule for LTM (never raw ops versus an evicting rival) and the reframe discipline for the rest (a declaration must price the cost and show the lever is spent or intrinsic, not assert defeat).

## Residual open levers (deferred, none gating)

- **Cross-type keyspace-unification arc**: arena-embed tiny collections to reach memory parity on the RAM-only tiny-collection rows (M1/M2/M3/M4 G10).
- **Reactor-boundary group-commit** (bands.go:127): move the spill pwrite off the owner thread to cut owner-stall latency on the write-under-spill path. An M8 STORE re-home; cannot lift the sustained spill rate above the sequential-bandwidth floor.
- **Streaming second-copy elision**: extend the reply-copy elision (that closed SMEMBERS/HGETALL) to the windowed range replies (ZRANGE/XRANGE); box-risky (block-band pin versus the LTM cold tier), bounded ~1.3-1.4x upside.
- **S3-FIFO probation queue + ghost ring**: the m7/03 lab model beats the shipped SIEVE residency by +0.11 hit ratio at ~150x less CPU; a box-risky engine swap. SIEVE already carries the binding M7-G1/G2 arms, so this is an upside lever, not a gap.

## Verdict

The f3 2x-gate campaign is complete.
Every point-path row that can pass 2x does, the memory bar is a decisive product win on every data-bearing shape, and every sub-2x row carries a structural verdict that names an intrinsic physical cost with box or real-store evidence.
aki is a Redis-wire-compatible store that matches or beats redis and valkey on the point path at a fraction of their memory, and holds a full keyspace on disk where they evict.
