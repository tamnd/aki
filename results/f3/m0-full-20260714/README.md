# Full f3 M0 gate rerun, 2026-07-14

The full M0 gate does not pass. Aki clears the 2x throughput-and-p99 bar on 23
of 40 scored profiles and misses it on 17. The recorded-only P1 profiles and all
nine non-gating appendix controls were also run, for 51 profiles total.

The result is materially better than the first M0 gate: small point operations,
cardinality sweeps, INCR, 64-byte Zipfian operations, MSET, the 1 KiB range
forms, and both hot-key profiles pass. There are no crashes, error replies, or
`arena full` failures. The remaining gate is owned by 1 KiB+ writes, 4 KiB+
point operations, large-value range operations, APPEND, MGET, and LTM.

## Protocol and provenance

- GamingPC, i9-13900K under WSL2 Linux 6.18.33.2, 32 logical CPUs and 56 GiB
  RAM; no swap was used.
- Aki main `69aa50aeb688e2e2c17690f845a4ce342ea35a1c`, built with Go 1.26.0.
- Redis 8.8.0 with the CF16-frozen `io-threads=6`.
- Valkey 9.1.0 with the CF16-frozen `io-threads=4` and threaded reads enabled.
- Gate split: server CPUs 4-17, client CPUs 18-31, Aki `GOMAXPROCS=14`, eight
  shards. The two explicitly supplementary all-32 rows use server CPUs 0-15
  and client CPUs 16-31, matching their original description.
- P16/c512 unless the profile name says otherwise; 3-second warm drive and
  three measured repetitions. Results are medians. The p99 factor is the worst
  measured Aki p99 divided by the better rival p99; the gate ceiling is 1.25x.
- Standard profiles use `aki-bench` for all three targets. Arbitrary MGET,
  GETRANGE, SETRANGE, and APPEND forms use two concurrent `redis-benchmark`
  generators to avoid the random-key client ceiling.
- LTM uses 1M keys, 1032-byte values, a 512 MiB total Aki resident-value budget,
  512 MiB `maxmemory` with `allkeys-lfu` on both rivals, and a 10k-key coverage
  probe. GET throughput is value-bearing throughput, excluding nil replies.

The exact binary hashes are in `env.txt`. The F3 engine/server packages passed
before this same Aki binary was built for the campaign.

## Scored verdict

| result | profiles | count |
|---|---|---:|
| PASS | >=2x the faster rival and p99 <=1.25x best rival | 23 |
| MISS | throughput below 2x (several also miss p99) | 17 |
| Crash/error/arena-full | none | 0 |

The 23 passes are:

- SET 16/64/256 B at 1M keys; GET 16/64/256 B and 1 KiB at 1M keys.
- SET/GET 64 B at 1k and 100k keys; INCR at 1k, 100k, and 1M keys.
- SET/GET 64 B Zipfian and GET 1 KiB Zipfian.
- MSET 64 B; native GETRANGE 1 KiB; fixed-window GETRANGE and SETRANGE 1 KiB.
- Hot-key SET and INCR.

### The 17 misses

Throughput is median operations/second. Binding ratio is Aki divided by the
faster rival.

| profile | Aki | Redis | Valkey | binding ratio | worst p99 factor |
|---|---:|---:|---:|---:|---:|
| SET 1 KiB, 1M | 5.343M | 2.913M | 2.049M | 1.83x | 0.88x |
| SET 4 KiB, 1M | 1.791M | 1.763M | 1.127M | 1.02x | 1.07x |
| SET 64 KiB, 100k | 56k | 209k | 151k | 0.27x | 3.60x |
| SET 1 MiB, 4k | 4.1k | 14.9k | 12.4k | 0.28x | 4.30x |
| GET 4 KiB, 1M | 1.482M | 1.176M | 656k | 1.26x | 2.37x |
| GET 64 KiB, 100k | 150k | 221k | 290k | 0.52x | 3.39x |
| GET 1 MiB, 4k | 13.0k | 21.9k | 20.1k | 0.60x | 2.36x |
| SET 1 KiB Zipfian | 5.969M | 3.099M | 2.222M | 1.93x | 0.93x |
| GETRANGE 64 KiB native | 989k | 2.301M | 2.107M | 0.43x | 2.68x |
| APPEND growth | 5.657M | 2.882M | 2.755M | 1.96x | 2.34x |
| MGET 16x64 B | 1.063M | 469k | 551k | 1.93x | 1.02x |
| GETRANGE 64 KiB fixed | 797k | 1.982M | 1.994M | 0.40x | 4.89x |
| SETRANGE 64 KiB | 3.922M | 1.984M | 1.992M | 1.97x | 1.56x |
| APPEND 1 KiB base | 2.393M | 2.993M | 2.995M | 0.80x | 4.77x |
| APPEND 64 KiB base | 3.929M | 1.978M | 1.996M | 1.97x | 1.74x |
| LTM GET | 1.808M | 1.209M | 1.108M | 1.50x | 3.00x |
| LTM SET | 1.351M | 1.103M | 970k | 1.23x | 19.00x |

Large-value loopback rows remain bandwidth-influenced, as documented by the
original M0 protocol. They are still honest gate misses: Aki is slower and has
worse tails under the specified end-to-end profile.

## LTM interpretation

Aki retained 100% of sampled keys in all LTM repetitions. Redis and Valkey
retained approximately 39-41% because they evicted under their 512 MiB ceiling.
The GET comparison therefore uses value-bearing operations, not their higher
raw rate including nil replies.

| profile | Aki value ops/s | Redis | Valkey | Aki coverage | Redis/Valkey coverage | Aki p99 |
|---|---:|---:|---:|---:|---:|---:|
| LTM GET | 1.808M | 1.209M | 1.108M | 100% | 40.0% / 39.6% | 9.8 ms |
| LTM SET | 1.351M | 1.103M | 970k | 100% | 39.1% / 40.9% | 160.7 ms |

The old 40k/s vlog GET path is now 1.81M/s, about 45x faster, but it is only
1.50x the faster rival and its p99 is 3.00x the best rival. LTM SET is 1.23x
with a severe 161 ms p99 shoulder. Both remain misses.

## Memory gate

The strict same-data memory bar (Aki resident and peak RSS at or below the
leaner rival) passes 9 of 40 scored profiles. The older under-2x ceiling passes
26 of 40. Aki's intrinsic ledger is often denser—for example, the 64-byte 1M
SET row uses 0.99x Valkey's `used_memory`—but process RSS remains higher on
small-value and small-cardinality rows because fixed arena/connection overhead
dominates. LTM GET peaks at 3.51x the leaner rival.

Consequently memory independently keeps M0 red even if throughput misses are
ignored. Exact per-cell RSS, HWM, ledger ratios, and both memory verdicts are in
`memory.tsv`.

## Recorded and supplementary profiles

P1 SET is recorded only, not scored: Aki is 0.46x at 50 connections and 1.42x
at 512 connections. The original 1.4-2.4x expectation at 50 connections is not
met.

All nine non-gating appendix controls were rerun. The 256-byte Zipfian SET/GET,
80/20 mixed 64-byte profile, and both all-32 controls clear 2x. The 4 KiB
Zipfian pair and both 50-connection controls do not.

## Final verdict

- **Headline 64-byte P16/c512 point operations: PASS.** In the full harness,
  SET is 2.80x and GET is 2.26x the faster rival. The corrected dual-generator
  headline-only run separately measured 3.42x and 2.85x.
- **Full M0 throughput/p99 gate: FAIL, 23/40 profiles pass.**
- **Strict memory gate: FAIL, 9/40 profiles pass.**
- **Robustness: PASS for this campaign.** No crash, command error, arena-full
  failure, or swap use occurred across 51 profiles.

`summary.tsv` is the machine-readable performance verdict, `memory.tsv` is the
memory verdict, `cells/` contains every raw JSON/CSV, memory snapshot, and
server log, and `run.sh` plus the two summarizers reproduce the campaign.
