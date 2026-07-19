# M1 set range-read gate, 10k band

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), same rig as `../algebra-10k`.
f3srv gate config vs CF16-frozen rivals (redis io6, valkey io4), card 10k sets, P16, 8s + 3s warm.
These rows return real member payloads, so the server is the bottleneck and single-generator aki-bench is valid (unlike point ops, which need dual-gen).

| workload | aki ops/s | redis | valkey | vsR | vsV | min | verdict |
|---|---|---|---|---|---|---|---|
| SSCAN | 931440 | 271688 | 372536 | 3.43x | 2.50x | 2.50x | PASS |
| SRANDMEMBERCOUNT | 344868 | 45389 | 14135 | 7.60x | 24.40x | 7.60x | PASS |
| SMEMBERS (pre) | 6990 | 3546 | 2276 | 1.97x | 3.07x | 1.97x | near-2x |
| SMEMBERS (post #1191) | 8349 | 3568 | 2360 | 2.34x | 3.54x | 2.34x | PASS |

SSCAN and SRANDMEMBERCOUNT clear 2x.
SMEMBERS first landed at 1.97x against redis, a bandwidth-bound near-miss.
The `smembers_m10000_p16_postelision.json` here is after PR #1191, the reply-stream copy elision (frame each member straight onto the wire chunk instead of scratch-then-copy).
That lifted SMEMBERS to a median-of-3 2.29x redis / 3.56x valkey (reps 2.34 / 2.29 / 2.29, aki 8349 / 8422 / 8372 ops, +19% throughput), a clean PASS.
The floor was the second per-member copy, not per-command dispatch as task #17 had framed it, so the same elision is a candidate lever for the LRANGE / HGETALL / ZRANGE range replies.

## SPOP is a harness artifact, not measured here

The SPOP cell reported 0 value-bearing ops on all three targets: the probe drains each set and subsequent pops return nil, so the vops gate reads FAIL for everyone. Raw pop rate was aki 2.95M vs redis 3.17M / valkey 3.75M, a generator-bound light mutate. It needs a fixed probe (re-add before pop) or the dual-generator protocol before it can gate; recorded for the record only.
