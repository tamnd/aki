# M0-G8 zipf-SET: correction to a pass

GamingPC (WSL2, i9-13900K), 2026-07-22. Server pinned cores 4-17 (GOMAXPROCS 14,
8 shards), client cores 18-31. Reactor net driver.

## What the recorded row said

M0-G8 (R3 zipf s=0.99, 64B SET) was recorded as "near-2x coverage almost, SET
zipf 1.93x", attributed to hot-shard write concentration: a zipfian write
supposedly concentrates on a few hot keys so the owning shards serialize where
the uniform 1M-key SET spreads across all eight shards.

## What the box shows

That mechanism is wrong. aki gets faster under skew, not slower.

aki-only reactor SET, one generator, swept by distribution (same server, same
client, 64B P16 c50):

| distribution        | aki ops/s | vs uniform |
|---------------------|----------:|-----------:|
| uniform 1M          | 2,467,347 | -          |
| zipf s=0.99 1M      | 2,463,097 | -0.2%      |
| zipf s=1.2 1M       | 2,626,593 | +6.5%      |
| single hot key      | 2,869,774 | +16%       |

A concentrated keyspace stays cache- and keyspace-map-resident, so aki's per-op
cost drops. A single hot key routes every write to one shard, yet that one shard
runs faster than the uniform spread, it never serializes below the uniform rate.

The rivals are distribution-flat too (same generator, 64B P16 c50):

| engine | uniform   | zipf s=0.99 | single hot key |
|--------|----------:|------------:|---------------:|
| redis  | 1,888,019 | 1,874,861   | 2,160,277      |
| valkey | 1,739,707 | 1,759,094   | 1,892,040      |

## Same-generator ratio does not drop

Held on one consistent generator (aki-bench, the only zipf-capable generator),
64B SET P16 c512, the ratio rises from uniform to zipf:

| distribution | vs redis | vs valkey |
|--------------|---------:|----------:|
| uniform      | 5.41x    | 5.81x     |
| zipf s=0.99  | 5.86x    | 6.35x     |

(These aki-bench absolutes overstate the rivals' disadvantage: aki-bench's Go
generator under-drives single-threaded redis. They are shown only to establish
the uniform-to-zipf direction, which is up.)

## Why the recorded 1.93x appeared

redis-benchmark has no zipfian mode. The recorded uniform headline (2.71x) was
measured with redis-benchmark; the recorded zipf row was measured with aki-bench.
Comparing a redis-benchmark uniform baseline against an aki-bench zipf number is
a cross-generator comparison and reads as a distribution drop that no single
consistent generator reproduces.

## Verdict

PASS by distribution-independence, the same law that already passed M0-G8 GET
zipf and M1-G9 SISMEMBER zipf. Both aki and the rivals are distribution-flat, so
the fair zipf ratio equals the fair uniform ratio, and the uniform headline
passes at 2.71x redis / 3.14x valkey (M0-G1, redis-benchmark method). zipf
inherits that pass.

## Harness note

No single generator saturates all three engines on this box: redis-benchmark
drives single-threaded redis to its ceiling but under-drives aki's eight-shard
reactor (dual-c256 got aki 3.73M vs aki-bench's ~10.5M for the same cell), while
aki-bench saturates aki but under-drives redis (~1.9M vs redis-benchmark 3.36M).
A quotable ratio needs each engine at its own best generator, or a
distribution/config-independence argument that removes the generator from the
comparison. G8 uses the latter.
