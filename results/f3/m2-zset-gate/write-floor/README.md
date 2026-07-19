# Collection point-write compute floor (M2-G2/G3, M3-G1/G2, M4-G2/G3, M5-G1/G2)

The point-write mirror rows across the four collection types (ZADD, HSET, LPUSH,
RPUSH, XADD spread over a 1M-key space, pipeline 16) all land well under the 2x
bar, clustered around 1.3x. This is the write-side twin of the range-read
dispatch floor (task #17): the reactor's networking edge that carries the flat
string SET to 2x is diluted by the per-op collection compute a flat SET never
pays, so the ratio settles at the compute floor, not the SET ceiling.

## Box triage (GamingPC, CF16-frozen rivals redis io6 / valkey io4, 1M keys, P16, 8s + 3s warm)

| workload | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|
| ZADD  | 2080404 | 1526973 | 1533540 | 1.36x | 1.36x | FAIL |
| HSET  | 1905851 | 1402642 | 1472292 | 1.36x | 1.29x | FAIL |
| LPUSH | 1875676 | 1447070 | 1542869 | 1.30x | 1.22x | FAIL |
| RPUSH | 1937663 | 1523808 | 1634839 | 1.27x | 1.19x | FAIL |
| XADD  | 1485081 |  819482 |  922886 | 1.81x | 1.61x | FAIL |

aki is faster than both rivals on every row (1.19x to 1.81x). XADD is the closest
to the bar only because redis's stream append is itself the slowest rival op
here, not because aki's XADD compute is lighter.

## Why this is a floor, not a missing optimization

A box CPU profile of the ZADD hot path (`zadd-cpu-profile.txt`) splits the time:

| bucket | share | shared with redis? |
|---|---|---|
| syscall (networking IO) | 39% | aki wins here via the reactor |
| keyspace map lookup (reg.peek) | ~12% | yes, redis dict lookup is the twin |
| listpack member-existence scan | ~11% | yes, redis listpack scan is the twin |
| listpack insert-position scan | ~2.4% | yes (the one fusable pass) |
| listpack sorted insert (memmove) | ~4.6% | yes |
| dispatch / parse / reply / reactor | ~30% | partly, reactor is aki's edge |

The three collection-specific buckets (keyspace lookup, member scan, sorted
insert, ~30% together) are legitimate per-op work that redis and valkey also
perform on a small listpack-encoded collection. aki has no representational
advantage there for a tiny collection, since a listpack is already a tight
packed array and redis's is well tuned. To reach 2x aki would need roughly +47%
throughput, which means cutting more than half of the non-syscall compute. There
is no cheap per-row lever that does that.

The one fusable pass is the insert-position scan: for an insert-new (the common
case in a monotone-append workload) ZADD scans the listpack twice, once for
member existence and once for the sorted position. Fusing them into a single
scan is byte-identical but the profile bounds the win at ~2.4% compute (~2%
throughput), which does not move a 1.36x row to 2.0x. It is recorded here as a
measured sub-lever, not a close path.

## Verdict (frozen)

DECLARE STRUCTURAL, the collection point-write compute floor. aki is 1.2x to
1.8x faster than both rivals on every collection-write mirror, but the per-op
index compute (keyspace lookup + listpack scan + sorted insert), which a flat
SET does not pay, dilutes the reactor networking edge below the 2x bar. The
theoretical close path is the same cross-type keyspace-and-collection
representation arc that M1-G10 names for the tiny-collection memory wall, not a
per-type write lever. The memory column for these rows inherits the M1-G10
declaration (1M live map entries plus separately-allocated per-collection
structs).

Profile: `zadd-cpu-profile.txt`. Same reframe class as task #17 (range-read
dispatch floor).
