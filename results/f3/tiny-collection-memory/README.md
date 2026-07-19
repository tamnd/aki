# Tiny-collection memory: zset/list/hash structural, stream a strong PASS

GamingPC gate box (2026-07-19), f3srv `GOGC=20 -arena-mib 512 -net reactor` vs
redis 8.8.0 and valkey 9.1.0. Each engine loads 100000 collections of 8 elements
via `redis-cli --pipe`, fresh server per type, VmHWM peak read from
`/proc/PID/status`. This is the M2-G10 / M3-G10 / M4-G10 / M5-G8 memory column.

## The numbers

| type | aki kB | redis kB | valkey kB | aki/redis | aki/valkey | verdict |
|---|---|---|---|---|---|---|
| zset | 34420 | 19132 | 18620 | 1.80 | 1.85 | STRUCTURAL |
| list | 31056 | 18208 | 16944 | 1.71 | 1.83 | STRUCTURAL |
| hash | 33584 | 21972 | 20860 | 1.53 | 1.61 | STRUCTURAL |
| stream | 59820 | 450832 | 442324 | 0.13 | 0.14 | PASS |

## Stream is a strong PASS (M5-G8), 7.5x less memory

A stream of 8 entries costs aki 598 bytes but redis 4508 bytes and valkey 4423
bytes: aki holds the same 800k stream entries in 0.13x of the rivals' resident
memory. Redis's stream is a radix tree of listpack-packed macro-nodes with a
per-stream rax and consumer-group scaffolding that a tiny stream cannot amortize;
aki's stream is one packed block band per key with no per-entry tree node. Well
past the 0.5x memory-bar ideal, on the same data-bearing shape the row names.
M5-G8 PASS.

## zset/list/hash are the tiny-collection fixed-overhead wall (STRUCTURAL)

The three sub-collections land at 1.5x to 1.8x, above the bar. This is the same
wall M1-G10 (tiny set) declared structural with the full bytes-per-collection
breakdown: a live tiny collection in aki is a `map[string]*T` entry plus a
separately allocated per-collection struct plus the copied key string, three heap
objects whose fixed overhead (Go map bucket slot, the struct's malloc size class,
the key allocation) a rival listpack pays once as a single contiguous dict entry.
On an 8-element collection that fixed overhead does not amortize away. M1-G10
tested the two cheap levers (struct-slim, map-value-inline) and proved neither
reaches parity; the close path is the cross-type keyspace-unification / arena
embedding arc, a large shared slice, not a per-row lever. These three rows are
that same wall for zset, list, and hash, declared with the measured deltas here.

The gap is smaller than M1-G10's single-member set (2.3-2.6x) precisely because
8 elements amortize the fixed overhead better than one, and hash (1.53x) beats
zset (1.80x) because a hash entry carries no score.

### The zset row improved 2.11x to 1.80x (inline score codec, PR #1205)

zset was the worst of the three at 2.11x because the inline listpack band spent a
flat 8-byte IEEE-754 double on every score, where redis's listpack
integer-encodes a small score. PR #1205 class-tags the inline score (int8/int16/
int32, or an 8-byte float fallback), so an integer-scored member costs 2 to 5
bytes. On this exact shape (integer scores 0..7) aki dropped 40776 kB to 34420 kB,
a 15.6 percent cut, moving the row from 2.11x/2.20x to 1.80x/1.85x. Lab m2/11
carries the byte model and codec cost; the row stays structural because the score
was never the wall, the three-heap-object fixed overhead is.

## M3-G10 blocking-op correctness

M3-G10 also carries a correctness clause (BLPOP/BRPOP wake), not a throughput or
memory figure. Blocking list ops and their cross-shard wake are covered by the
engine suite (`engine/f3/list/blocking_test.go`, `blockcross_test.go`,
`blockmove_test.go`) and the barrier-vs-timer heartbeat fix (PR #680). Correctness
gate, green in the suite.

## Verdict

- M5-G8 stream memory: PASS, aki 0.13x (7.5x less resident memory for the same
  stream entries).
- M2-G10 / M3-G10 / M4-G10 tiny zset/list/hash memory: STRUCTURAL, the
  fixed-per-collection three-heap-object wall M1-G10 declared, close path the
  shared cross-type keyspace-unification arc. zset improved to 1.80x via the
  inline score codec.
- M3-G10 blocking-op correctness: green in the engine suite.

CSV: tiny-collection-memory.csv. Probe: zsetmem.sh pattern (VmHWM on a
`redis-cli --pipe` load), collmem.sh for the four-type sweep.
