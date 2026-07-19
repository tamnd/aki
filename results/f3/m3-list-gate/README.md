# M3 list gate + cross-type O(1) metadata reads

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), f3srv reactor gate
config vs CF16-frozen rivals (redis io6, valkey io4), card 10k collection, P16, 8s
+ 3s warm. The metadata O(1) reads use the M0 dual-generator 1M-key protocol
(two summed redis-benchmark generators) because a single aki-bench generator
client-caps ultra-cheap ops (see the metadata section).

## Passing rows

| row | workload | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|---|
| M3-G3 | LRANGE window 100 | 900K | 372K | 316K | 2.42x | 2.85x | PASS |
| M3-G4 | LINDEX (median-of-3) | 2712K | 1122K | 1056K | 2.42x | 2.57x | PASS |
| M3-G5 | LSET positional | 2548K | 696K | 625K | 3.66x | 4.08x | PASS |
| cover | LPOS scan | 55.3K | 22.6K | 19.1K | 2.45x | 2.90x | PASS |

All four are reads or positional writes where redis and valkey walk their
quicklist node chain to reach the index or window, crossing several listpack
nodes, while aki descends its packed cursor. LINDEX and LSET are the point twins
(one index, one bulk in or out); LRANGE is the window walk; LPOS is a full scan
where aki's contiguous packed scan beats the quicklist node-chain scan. In every
case the op itself is where aki's packed representation has a structural edge, so
the reactor networking edge stacks on top rather than being diluted by per-op
index compute (the write-floor case, results/f3/m2-zset-gate/write-floor). LSET
is the strongest, 3.66x/4.08x, because it is the write twin of LINDEX: redis
rebuilds or walks the quicklist to the index while aki does one in-place packed
write.

## O(1) metadata reads (SCARD, ZCARD, HLEN, LLEN), dual-generator protocol

The card counters are O(1) on both sides (redis stores a length field, aki too).
Under the single aki-bench generator they mismeasure: the op is so cheap that one
client process cannot saturate the server, so all three targets land client-bound
in a narrow 2.9-3.5M band and aki reads 0.92-0.95x, favoring whichever rival has
the lowest per-op RTT. This is the exact single-generator cap the M0 point-read
work documented (results/f3/collection-pointread-20260718): the same op shape
SISMEMBER/HGET/ZSCORE read 1.3x under one generator and 2.6-2.9x under two.

Re-measured with two summed generators (c512 total, P16, 1M-key, warm + best of 2,
reactor gate binary):

| read | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|
| SCARD | 7.18M | 2.88M | 2.87M | 2.49x | 2.50x | PASS |
| ZCARD | 7.17M | 2.88M | 2.87M | 2.49x | 2.50x | PASS |
| LLEN  | 7.19M | 2.87M | 2.87M | 2.50x | 2.50x | PASS |
| HLEN  | 7.17M | 2.87-3.60M | 2.87M | 2.00x (median-of-3) | 2.50x | PASS |

aki is rock-steady at ~7.17M across all four (keyspace lookup plus a counter read,
the SISMEMBER shape). HLEN is the marginal arm: redis's HLEN occasionally spikes
to 3.60M (25% run-to-run swing on redis's side, aki flat), so the redis ratio
reads 1.99x / 2.00x / 2.50x across three reps, median 2.00x, clearing the bar at
the line. The single-generator 0.92-0.95x numbers are client-cap artifacts, not
losses.

## Declared floors

| row | workload | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|---|
| M3-G6 | LINSERT (O(n) walk to pivot) | 2666 | 4648 | 3998 | 0.57x | 0.67x | DECLARE O(n) floor |
| M3-G7 | LREM (O(n) scan) | 1147K | 908K | 483K | 1.26x | 2.37x | DECLARE O(n) floor |

LINSERT walks the list to the pivot member and shifts to open a slot; on a card-10k
list that is an O(n) scan plus an O(n) memmove in a packed cursor, where redis's
quicklist inserts into the target listpack node without shifting the whole list.
aki is slower here (0.57x) because the packed contiguous representation that wins
the reads pays a memmove on a mid-list insert that the quicklist's node split
avoids. This is the structural cost twin of the read wins: the same packed layout
that makes LINDEX/LRANGE/LSET fast makes a mid-list insert an O(n) shift. LREM is
an O(n) scan-and-compact that clears valkey (2.37x) but not redis (1.26x), the
redis arm being an efficient listpack scan. Both are O(n)-in-cardinality list
mutations, declared as the list-mutation O(n) floor; they are not point ops and
not the milestone's throughput headline.

## Open

XRANGE (M5-G4) reads 0.76x/0.88x, a genuine loss on the stream range path, not a
client-cap artifact (the reply is wide, bandwidth bound). Flagged for a stream
range-cursor / reply-copy investigation, tracked under M5.
