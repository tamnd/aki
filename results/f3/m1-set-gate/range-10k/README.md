# M1 set range-read gate, 10k band

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), same rig as `../algebra-10k`.
f3srv gate config vs CF16-frozen rivals (redis io6, valkey io4), card 10k sets, P16, 8s + 3s warm.
These rows return real member payloads, so the server is the bottleneck and single-generator aki-bench is valid (unlike point ops, which need dual-gen).

| workload | aki ops/s | redis | valkey | vsR | vsV | min | verdict |
|---|---|---|---|---|---|---|---|
| SSCAN | 931440 | 271688 | 372536 | 3.43x | 2.50x | 2.50x | PASS |
| SRANDMEMBERCOUNT | 344868 | 45389 | 14135 | 7.60x | 24.40x | 7.60x | PASS |
| SMEMBERS | 6990 | 3546 | 2276 | 1.97x | 3.07x | 1.97x | near-2x |

SSCAN and SRANDMEMBERCOUNT clear 2x.
SMEMBERS lands at 1.97x against redis (3.07x against valkey): the known P1 full-range-reply dispatch-floor shared with LRANGE / HGETALL / ZRANGE (see aki task #17, "range-read deficit = P1 dispatch ceiling, not a collection lever"). It is a near-2x coverage-almost against one rival only, non-blocking, and closes with the P1 dispatch campaign rather than a set-specific change.

## SPOP is a harness artifact, not measured here

The SPOP cell reported 0 value-bearing ops on all three targets: the probe drains each set and subsequent pops return nil, so the vops gate reads FAIL for everyone. Raw pop rate was aki 2.95M vs redis 3.17M / valkey 3.75M, a generator-bound light mutate. It needs a fixed probe (re-add before pop) or the dual-generator protocol before it can gate; recorded for the record only.
