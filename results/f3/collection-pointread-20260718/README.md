# Collection point-read gate, corrected protocol (2026-07-18)

The M1, M2, and M4 gate verdicts recorded the collection point reads as misses:
SISMEMBER at 1.31-1.45x, ZSCORE and the hash reads in the same 1.3-1.5x band.
Those runs predate the dual-generator discovery from the M0 headline work, which
showed that a single `redis-benchmark -r` client hard-caps at about 6.6M ops/s on
this box and silently under-measures any server faster than that, while the slower
rivals stay honest. That artifact made M0's own point ops read 1.4-1.8x until the
cell was driven with two summed generators, at which point GET jumped to its true
2.71x. This run re-measures the collection point reads under the corrected
protocol to see whether their misses were the same artifact.

## Protocol

Same M0 headline cell, one type at a time. Server pinned cores 4-17, GOMAXPROCS
14, 8 shards. Two redis-benchmark generators pinned 18-24 and 25-31, rates summed.
c256 each (512 total), P16, 1M-key uniform space, `-r 1000000`. Each type
preloads 1M distinct keys spread across the shards, then measures the point read,
warm plus two reps, best summed rate. Custom commands so the key carries
`__rand_int__` and every op lands on a random shard rather than one hot key:

- set: preload `SADD set:__rand_int__ hello`, read `SISMEMBER set:__rand_int__ hello`
- hash: preload `HSET hash:__rand_int__ f v`, read `HGET hash:__rand_int__ f`
- zset: preload `ZADD zset:__rand_int__ 1 m`, read `ZSCORE zset:__rand_int__ m`

Rivals are the CF16-frozen builds: redis 8.8.0 io-threads 6, valkey 9.1.0
io-threads 4 with reads on. aki at `main` `7a8e866`, goroutine driver.

## Throughput

Best summed ops/s, and the ratio over each rival.

| read | aki | redis io=6 | valkey io=4 | aki/redis | aki/valkey |
|---|---|---|---|---|---|
| SISMEMBER | 10.77M | 3.69M | 3.00M | **2.92x** | 3.59x |
| HGET | 9.57M | 3.69M | 2.99M | **2.60x** | 3.19x |
| ZSCORE | 9.57M | 3.69M | 3.00M | **2.60x** | 3.19x |

**All three clear 2x against both rivals.** The stale 1.3-1.5x misses were the
single-generator client cap, the same artifact that hid M0's point-op 2x, not a
type-kernel deficit. aki serves each read from one of eight parallel shard
workers while the single-threaded rivals (io-threads move socket bytes, never
execute a command) eat a 1M-entry hashtable lookup on one core, so the ratio is
architectural and lands where the M0 GET ratio does. The hash and zset reads sit
a little under the set read (9.57M vs 10.77M) because HGET walks the field map and
ZSCORE the member-to-score map where SISMEMBER answers a membership bit, but all
three are far over the floor.

## Memory

Peak VmHWM after the 1M-key preload, single-member collections.

| target | SISMEMBER VmHWM | HGET VmHWM | ZSCORE VmHWM |
|---|---|---|---|
| aki | 410.1 MB | 388.3 MB | 374.6 MB |
| redis io=6 | 93.0 MB | 92.8 MB | 90.0 MB |
| valkey io=4 | 92.0 MB | 92.2 MB | 93.0 MB |

**The memory bar fails hard here, about 4x.** This is the fixed per-collection
creation cost the M2 gate isolated, and it is at its worst on this workload
because each of the 1M keys holds exactly one tiny member, so aki's fixed
overhead per collection (the arena record, the collection header, the index
entry, and the connection fabric peak under churn) is the entire footprint with
no data to amortize it against. Redis and valkey hold a million one-member
collections in about 90 MB; aki needs about 4x that. This is now the clearest
cross-milestone statement of the open blocker: throughput passes across the type
point reads, and the fixed per-collection memory cost is the thing that fails the
product's own memory pitch on small collections.

## Verdict

The collection point-read throughput 2x holds across set, hash, and zset once the
cell is driven correctly, correcting the stale M1/M2/M4 verdicts. The open item is
not throughput on these reads, it is the per-collection fixed memory cost, which
is about 4x the rivals on single-member collections and is the same fixed-overhead
root the string gate isolated, amplified by minimal per-collection data.
