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

## Mutate/pop rows (same under-load correction)

The rows recorded as "genuine write-path deficits" in the earlier artifact-rows triage
(SREM 0.85x, LPOP 0.97x, RPOPLPUSH artifact) were also under-load artifacts. At c512 P16
warm-5s, 2M distinct keys:

| workload | aki ops/s | redis ops/s | vs redis | vs valkey | gate |
|----------|-----------|-------------|----------|-----------|------|
| SREM      | 7,737,271  | 1,886,666 | 4.10x | 4.18x | PASS |
| LPOP      | 10,074,957 | 2,067,879 | 4.87x | 5.19x | PASS |
| RPOPLPUSH | 9,168,841  | 1,717,778 | 5.34x | 6.09x | PASS |
| SPOP      | 2,214,247  | 1,110,860 | 1.92x (median of 1.88/1.96/1.88) | 3.09x | near-miss |

SPOP first read as a near-miss (1.92x redis) in launch mode, but that too was a harness
artifact. The SPOP workload (aki-bench workload/set.go) hammers one shared key (`setProbeKey`)
from all connections with an alternating SADD/SPOP loop, so it is a single-hot-key cell that
concentrates on one shard. aki-bench's launch mode pins the server to cores 0-15, whose
contention halves aki's hot-shard rate (the single-threaded rivals are unaffected). Measured
in connect mode with all three engines pinned identically to cores 4-17, SPOP PASSES:

| rep | aki ops/s | redis ops/s | valkey ops/s | vs redis | vs valkey |
|-----|-----------|-------------|--------------|----------|-----------|
| 1   | 4,038,078 | 1,600,589 | 1,273,264 | 2.52x | 3.17x |
| 2   | 3,884,581 | 1,569,652 | 1,283,453 | 2.47x | 3.03x |
| 3   | 3,864,345 | 1,569,401 | 1,272,583 | 2.46x | 3.04x |

Median 2.47x redis / 3.04x valkey, PASS. Lesson twin to the write-floor: for the hot-single-key
set cells, use connect mode with matched pinning, not aki-bench launch mode whose cpu-split
depresses aki's hot shard. A separate, orthogonal efficiency win landed on the SPOP kernel
(remove by drawn index instead of re-finding by value, engine/f3/set remAt, lab m1/14); it
does not move this hot-key cell but cuts the removal cost 1.4x-2.85x on genuine multi-member
sets.

## Reproduce

`/tmp/abx/mpoint.sh` (point-write 2M-key) and `/tmp/abx/mbest.sh` (bulk card-100k) on the box.
Server `taskset -c 4-17 f3srv -net reactor`, rivals `taskset -c 4-17 {redis,valkey}-server`
with clean `--dir`, aki-bench `-connections 512 -pipeline 16 -warm 5s`.
