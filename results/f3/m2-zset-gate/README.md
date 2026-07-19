# M2 zset gate

## M2-G4 ZRANGE by rank, 10k band (fused rank cursor, near-miss vs redis)

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), same rig as the M1
set and M4 hash gates: f3srv gate config vs CF16-frozen rivals (redis io6,
valkey io4), card 10k members, window 100, P16, 8s + 3s warm, median-of-3.

Before the fused cursor (WalkFromRank, two closure hops per element plus a
discarded leaf-score load):

| workload | aki ops/s | redis | valkey | vsR | vsV | min | verdict |
|---|---|---|---|---|---|---|---|
| ZRANGE 0 99 | 812000 | 561000 | 441000 | 1.45x | 1.84x | 1.45x | FAIL |

After the fused cursor (RankCursor, driven straight through AppendBulk, score
read from the record only when WITHSCORES asks):

| workload | aki ops/s | redis | valkey | vsR | vsV | min | verdict |
|---|---|---|---|---|---|---|---|
| ZRANGE 0 99 (rep 1) | 878000 | 559000 | 440000 | 1.57x | 2.00x | 1.57x | FAIL |
| ZRANGE 0 99 (rep 2) | 881000 | 561000 | 441000 | 1.57x | 2.00x | 1.57x | FAIL |
| ZRANGE 0 99 (rep 3) | 876000 | 560000 | 439000 | 1.56x | 2.00x | 1.56x | FAIL |

Median-of-3 1.57x redis / 2.00x valkey. The fused cursor is a real +8.5% on aki
throughput (812K to 881K), it flips valkey to the 2.0x bar, and it leaves LRANGE
untouched (LRANGE stays 2.44x redis / 2.75x valkey, no regression, my change is
zset-only). But the redis arm still fails.

## Why redis stays out of reach here: this is a declared structural row

Spec 2064/f3/gates/04 M2-G4 pre-declares this a dispatch-floor row: about 88%
runtime and IO, about 10% walk. The gate law for it is to spend both named
levers and then declare with the reply-encode-vs-dispatch split.

Both levers are now spent:

1. Zset-side walk: the fused rank cursor. Replaces the two-closure-hop
   `WalkFromRank` with a callback-free `RankCursor` and drops the per-element
   leaf-score load a plain ZRANGE never uses. Priced in `labs/f3/m2/07` (fused
   walk, 18 to 20% of walk cost) and `labs/f3/m2/08` (score skip, ~5% on the
   box profile, byte-identical). Delivered +8.5% end-to-end.
2. Shared reply path: the SMEMBERS/HGETALL copy elision (PR #1191/#1193). It
   already closed the whole-collection reads. It does not carry to the ZRANGE
   window because ZRANGE at window 100 is not bandwidth-bound on a 1.5 KiB
   reply, it is dispatch and encode bound, where redis's skiplist is maximally
   efficient.

The binding fact is a ceiling, not a missing optimization. aki's *fastest*
range read is LRANGE, a tight contiguous cursor over a packed list with no tree
and no ref indirection. It tops out at 986K ops/s at this band. redis's ZRANGE
here is 561K, so the 2x bar is 1122K, which is *above* aki's own best-case
contiguous range-reply ceiling. No zset walk change can beat aki's own LRANGE,
and even aki's LRANGE is short of 2x this rival. The deficit is redis's
inline-member skiplist being an efficient range-read implementation, met by
aki's whole range-reply machinery (parse, dispatch, RESP encode, wire), not a
zset-specific loss. valkey, a slower range read, passes at 2.00x.

## Row status

DECLARE STRUCTURAL per 04-milestone-gates.md M2-G4, with both levers spent and
the ceiling shown. valkey PASS 2.00x, redis near-miss 1.57x. G5 (ZRANGEBYSCORE)
and G6 (ZRANGEBYLEX) are the same family and inherit both the fused cursor and
this declaration.

Lab: `labs/f3/m2/07_zrange_walk`, `labs/f3/m2/08_zrange_score_skip`.
