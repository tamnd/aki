# Collection write-floor correction (2026-07-22)

The gate rows M1-G3, M2-G2, M2-G3, M3-G1, M3-G2, M4-G2, M4-G3, and M5-G1 carried a
"collection point-write compute floor" verdict, declared STRUCTURAL at 1.30x-1.81x.
That verdict was an under-load artifact, not a real ceiling.

## What was wrong

The recorded triage read aki at low connection count (the same c50/single-generator
understatement documented in the M0 methodology note). At that load aki's eight-shard
reactor is idle-starved: the recorded ZADD point-write read aki at 2.08M ops/s.

## Re-measured, each engine at full load

Box run on the GamingPC, server pinned cores 4-17 (8 shards), client 18-31, each engine
launched clean (own `--dir`, stale rdb removed), aki-bench at c512 P16 warm-5s, 2M distinct
keys with one member each (the fresh-collection-per-op shape the floor claimed was
compute-bound).

| workload | aki ops/s | redis ops/s | valkey ops/s | vs redis | vs valkey | gate |
|----------|-----------|-------------|--------------|----------|-----------|------|
| SADD  (M1-G2) | 9,318,649 | 1,702,104 | 1,681,460 | 5.47x | 5.54x | PASS |
| ZADD  (M2-G2) | 8,544,657 | 1,489,356 | 1,502,427 | 5.74x | 5.69x | PASS |
| RPUSH (M3-G1) | 6,829,227 | 1,170,335 | 1,250,457 | 5.84x | 5.46x | PASS |
| HSET  (M4-G2) | 8,052,403 | 1,363,469 | 1,459,075 | 5.91x | 5.52x | PASS |
| XADD  (M5-G1) | 4,398,139 |   642,525 |   696,244 | 6.85x | 6.32x | PASS |

The card-10k bulk-append regime (reused keyspace, ~30 members per key) passes identically:
SADD 4.25x, ZADD 4.64x, RPUSH 6.37x, HSET 7.08x.

The rivals match their redis-benchmark ceilings (redis SADD 1.70M, ZADD 1.49M, RPUSH 1.17M,
HSET 1.36M, XADD 643K), so the comparison is fair, not an aki-bench rival understatement.

## What stays structural

Only the memory column. On 2M live tiny collections aki holds 1.2-6.7 GB against redis
0.18-5.5 GB, the three-heap-object-per-collection wall M1-G10 names (a live tiny collection
is a map entry plus a separately allocated struct plus the copied key, where a rival listpack
pays one contiguous dict entry). Close path is the keyspace-unification arena-embed arc, the
same lever that closes M1/M2/M4/M5 tiny-collection memory.

## Reproduce

`/tmp/abx/mpoint.sh` (point-write 2M-key) and `/tmp/abx/mbest.sh` (bulk card-100k) on the box.
Server `taskset -c 4-17 f3srv -net reactor`, rivals `taskset -c 4-17 {redis,valkey}-server`
with clean `--dir`, aki-bench `-connections 512 -pipeline 16 -warm 5s`.
