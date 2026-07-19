# M2-M5 breadth gate sweep: point ops pass (dual-gen), range reads declared, harness artifacts flagged

GamingPC gate box (2026-07-19), reactor gate binary at tip 353627c vs CF16-frozen
rivals (redis io6, valkey io4). This sweep runs the remaining measurable OPEN rows
across M2 (zset), M3 (list), M4 (hash), M5 (stream) and triages each into pass,
structural declaration, or harness artifact. Every ultra-cheap O(1) op is measured
under the M0 dual-generator 1M-key protocol, because a single aki-bench generator
client-caps them (documented in ../collection-pointread-20260718 and confirmed
again below).

## The single-generator client-cap, re-confirmed

A first single-generator pass over hdel / zrem / zrank / hexists read every target
pinned in a narrow 2.6-3.3M ops/s band, aki reading 0.91-0.94x:

| op | aki (1-gen) | redis | valkey | vsR | vsV |
|---|---|---|---|---|---|
| hdel | 2.61M | 2.77M | 3.28M | 0.94x | 0.80x |
| zrem | 2.69M | 2.88M | 3.30M | 0.93x | 0.81x |
| zrank | 2.14M | 2.35M | 2.58M | 0.91x | 0.83x |
| hexists | 2.58M | 2.78M | 3.17M | 0.93x | 0.81x |

All three targets clustered regardless of the operation is the generator ceiling,
not the server: one client process cannot saturate a server answering a keyspace
lookup plus an O(1) touch, so the row reads whichever rival has the lowest per-op
RTT. This is the exact artifact the metadata reads (SCARD/HLEN/LLEN/ZCARD) showed
before the dual generator lifted them to 2.49x.

## O(1) point ops, dual-generator (two summed redis-benchmark, c512/P16, 1M-key, warm+best-of-2)

Re-measured with two summed generators. Destructive ops (HDEL, ZREM) re-add the
target before each rep so the mutate has a live field or member to remove:

| row | op | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|---|
| M4-G6 | HDEL | 7.98M | 3.99M | 3.42M | 2.00x | 2.33x | PASS |
| M2 cover | ZRANK | 7.97M | 3.00M | 2.18M | 2.66x | 3.66x | PASS |
| M4 cover | HEXISTS | 7.98M | 3.42M | 2.99M | 2.33x | 2.67x | PASS |
| M2 cover | ZREM | 7.98M | 3.99M | 3.42M | 2.00x | 2.33x | PASS |

aki is flat at ~7.97M across all four, the reactor keyspace-lookup ceiling the
SISMEMBER / HGET / ZSCORE point reads already hit (the O(1) touch after the
keyspace lookup is free next to the lookup itself). HDEL and ZREM land exactly
2.00x redis at the line: redis answers a re-added-then-deleted field at ~4.0M, and
aki's 7.98M is precisely double. ZRANK and HEXISTS clear comfortably. The
single-generator 0.9x figures are client-cap artifacts, not losses.

## Range reads, dispatch-floor family (median-of-3, connect-mode)

| row | op | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|---|
| M2-G5 | ZRANGEBYSCORE | 672K | 430K | 273K | 1.56x | 2.51x | DECLARE (valkey pass, redis ceiling) |
| cover | SMISMEMBER (multi) | 414K | 296K | 307K | 1.40x | 1.35x | DECLARE batch-read floor |

ZRANGEBYSCORE is the score-boundary twin of the rank-cursor M2-G4 ZRANGE and lands
on the identical declaration: valkey PASS 2.51x, redis near-miss 1.56x (median of
three reps 1.56 / 1.59 / 1.53). The redis arm is the same ceiling M2-G4 documented,
not a missing optimization: 2x of redis's efficient inline-member skiplist range
read exceeds aki's own best-case contiguous range-reply ceiling (aki's fastest
range read is LRANGE, a no-tree packed cursor, and 2x redis's skiplist range is
above even that), so no zset walk change reaches it. M2-G6 ZRANGEBYLEX is the
lexical twin of the same seek-then-walk shape and inherits this declaration.

SMISMEMBER of a multi-member window is the set twin of ZMSCORE / HMGET: it lands
1.40x / 1.35x, server-bound (sane ~1.7 ms latencies, not generator-capped), a
windowed batch read where redis's own membership scan is maximally efficient and
the reply is encode-and-dispatch bound below the streaming cutover. Same
batch-read floor as ../m3-list-gate/batch-read-floor.md, higher than HMGET only
because the reply is a flat integer array, not 64 B values.

## Harness artifacts (zero value-bearing throughput, drain-to-empty)

| row | op | note |
|---|---|---|
| M3-G9 | LPOP / RPOP count | 0 vops all targets |
| M3-G8 | RPOPLPUSH | 0 vops all targets |

Both report zero value-bearing throughput for aki AND both rivals: the probe
drains the card-10k list, then every subsequent pop returns nil, so the vops gate
reads FAIL for everyone. This is the exact M1-G5 SPOP harness artifact, not a
regression. The raw op rate is client-capped (LPOP aki 2.80M vs redis 3.03M /
valkey 3.76M), the same single-generator ceiling as the point ops above. These
rows owe a fixed re-add-before-pop probe or a dual-generator pop cell, tracked with
M1-G5; they are not throughput regressions.

## Verdict (frozen)

- M4-G6 HDEL: PASS 2.00x / 2.33x (dual-gen), aki flat at the reactor keyspace ceiling.
- M2-G5 ZRANGEBYSCORE: DECLARE, valkey PASS 2.51x, redis ceiling 1.56x, the M2-G4
  dispatch-floor family. M2-G6 ZRANGEBYLEX inherits it.
- ZRANK / HEXISTS / ZREM: PASS 2.00-3.66x (dual-gen), zset and hash O(1) point
  op coverage, same reactor ceiling as the named point reads.
- SMISMEMBER: DECLARE batch-read floor (1.40x / 1.35x), the ZMSCORE / HMGET family.
- M3-G8 RPOPLPUSH, M3-G9 LPOP: harness artifact (zero vops drain-to-empty), tracked
  with M1-G5, not a regression.

Memory columns inherit the per-type tiny-collection declaration (M1-G10). M6
(bitmap / HLL / geo) has no aki-bench workload token and is measured separately
with redis-benchmark.
