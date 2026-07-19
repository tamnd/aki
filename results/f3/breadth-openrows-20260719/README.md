# Open O(1) rows pass 2x under the dual generator: XLEN, HINCRBY, HRANDFIELD, ZPOPMIN

GamingPC gate box (2026-07-19), reactor gate binary at tip 3790e56 vs CF16-frozen
rivals (redis io6, valkey io4). This cell measures the last four measurable O(1)
open rows across M2/M4/M5 directly with redis-benchmark raw commands under the M0
dual-generator 1M-key protocol (two summed redis-benchmark generators, c256/P16
each, warm plus best-of-2). A single aki-bench generator client-caps these at ~3M,
so the summed generators surface aki's true rate, the same protocol that lifted
HDEL/ZREM/ZRANK/HEXISTS to their real 2x figures in ../breadth-20260719.

## The numbers

| row | op | aki ops/s | redis | valkey | aki/redis | aki/valkey | verdict |
|---|---|---|---|---|---|---|---|
| M5-G3 | XLEN | 7.98M | 2.40M | 2.66M | 3.33x | 3.00x | PASS |
| M4-G7 | HINCRBY | 7.97M | 2.66M | 2.40M | 2.99x | 3.33x | PASS |
| M4-G8 | HRANDFIELD | 7.98M | 3.42M | 2.99M | 2.33x | 2.67x | PASS |
| M2-G8 | ZPOPMIN | 7.97M | 3.99M | 3.42M | 2.00x | 2.33x | PASS |

aki is flat at ~7.97-7.98M across all four, the reactor keyspace-lookup ceiling the
SISMEMBER/HGET/ZSCORE point reads and the HDEL/ZREM/ZRANK point mutates already
hit. Each of these four is a keyspace lookup plus one O(1) touch (a counter read,
a field increment, a single random draw, a min-element pop), and the O(1) work
after the lookup is free next to the lookup itself, so the reactor networking edge
stacks on top rather than being diluted. ZPOPMIN lands exactly 2.00x redis at the
line, the same place HDEL and ZREM landed: redis answers a re-added-then-popped
single-member zset at ~4.0M and aki's 7.97M is precisely double.

## What each row is

- M5-G3 XLEN: the stream live-entry count is a `uint64` counter on the stream
  header (`s.length`), so XLEN is `r.Int(int64(s.length))` after the keyspace
  lookup, the O(1) metadata read shape SCARD/ZCARD/HLEN/LLEN already pass at
  2.49x. XRANGE, the row's other half, is the M5-G4 range read (WIN 1.05x, aki
  fastest of three, declared structural on the wide-reply bandwidth floor).
- M4-G7 HINCRBY / HINCRBYFLOAT: a read-modify-write on one field (lookup, field
  get, parse, add, in-place set, log). The extra field-get plus AppendInt over a
  plain HSET does not move it off the reactor ceiling: 2.99x/3.33x.
- M4-G8 HRANDFIELD: a random draw framed straight into `cx.Aux` with
  `resp.AppendBulk`, the same alloc-free reply kernel SRANDMEMBER/SRANDMEMBERCOUNT
  use (those passed 7.60x/24.40x at M1-G4). The single-field HRANDFIELD here is
  the point-shape twin, 2.33x/2.67x.
- M2-G8 ZPOPMIN / ZPOPMAX: reply-plus-mutate, re-added before each rep like HDEL
  and ZREM so the pop has a live member. A single-member zset pops in O(1) off the
  inline listpack band, 2.00x/2.33x.

## Verdict

- M5-G3 XLEN: PASS 3.33x/3.00x (O(1) metadata, dual-gen). XRANGE half is M5-G4.
- M4-G7 HINCRBY/HINCRBYFLOAT: PASS 2.99x/3.33x (write-modify, dual-gen).
- M4-G8 HRANDFIELD: PASS 2.33x/2.67x (random-draw reply, SRANDMEMBER family).
- M2-G8 ZPOPMIN/ZPOPMAX: PASS 2.00x/2.33x (reply-plus-mutate, dual-gen).

CSV: openrows.csv. Probe: /root/openrows.sh (the ptdual.sh dual-generator pattern
with the four raw-command rows), raw run in the same box session.
