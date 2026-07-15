# M0 headline gate rerun, 2026-07-14

The M0 headline throughput gate passes on GamingPC at aki main `69aa50a`.
Aki clears 2x against both Redis 8.8 and Valkey 9.1 on SET and GET. Redis is
the binding rival in both cells. The complete M0 gate is not green because the
strict same-data RSS/peak-memory bar still fails.

## Protocol

- GamingPC, WSL2 Linux 6.18.33.2, 32 logical CPUs, 56 GiB RAM, no swap used.
- Aki `69aa50aeb688e2e2c17690f845a4ce342ea35a1c`, Go 1.26.0.
- Redis 8.8.0 with the CF16-frozen `io-threads=6`.
- Valkey 9.1.0 with the CF16-frozen `io-threads=4` and
  `io-threads-do-reads=yes`.
- Server CPUs 4-17; client CPUs 18-31; aki uses `GOMAXPROCS=14` and 8 shards.
- P16, 512 total connections, 64-byte values, 1M uniform random keys.
- Two concurrent `redis-benchmark` generators, each c256, 7 threads, and 10M
  operations. Their rates are summed. This is the corrected protocol that
  avoids the single-generator random-key ceiling.
- One warm run followed by three measured repetitions. The table reports the
  median summed throughput and the worst p99 across both generators and all
  measured repetitions.
- The aki memory posture uses the M0 gate's small-point-cell settings:
  `batch-data-cap=1024`, `reply-ring=128`, and `free-list-cap=8`.

## Throughput and latency

| command | aki | Redis 8.8 | Valkey 9.1 | vs Redis | vs Valkey | binding ratio | aki worst p99 | best-rival worst p99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SET | 11.399M | 3.329M | 2.755M | 3.42x | 4.14x | **3.42x PASS** | 1.335 ms | 3.735 ms |
| GET | 11.399M | 3.995M | 3.804M | 2.85x | 3.00x | **2.85x PASS** | 1.247 ms | 2.391 ms |

Every measured aki repetition was 11.393-11.399M ops/s. The flat top says the
two client processes are at their combined generation ceiling, so the aki
numbers, and therefore the ratios, are conservative lower bounds. Both rivals
remain below that ceiling.

The latency clause also passes: aki's worst p99 is 0.36x the best rival on SET
and 0.52x on GET, comfortably inside the 1.25x ceiling.

## Memory verdict

The fresh-server, first 5M-SET preload produced the same approximately 993k-key
dataset on all three servers:

| server | VmRSS / VmHWM | used_memory | RSS vs lean rival |
|---|---:|---:|---:|
| aki | 201,912 KiB | 112,865,184 B | 1.57x |
| Redis 8.8 | 154,296 KiB | 135,020,224 B | 1.20x |
| Valkey 9.1 | 128,456 KiB | 114,375,344 B | 1.00x |

Aki's intrinsic ledger is slightly denser than Valkey (`used_memory` is 0.99x),
but process RSS is 1.57x Valkey. It clears the historical under-2x ceiling on a
fresh load but misses the tightened at-or-below-rival M0 bar.

The repeated FLUSHALL/preload sequence exposes a second red memory signal: by
the third GET preload aki reached 327,748 KiB peak versus Valkey's 132,912 KiB,
or 2.47x. Redis and Valkey stayed flat. This is resident slack across repeated
overwrite/load cycles, not live-data density, and keeps the full M0 verdict red.

## Verdict

- **2x performance objective: achieved.** SET passes at 3.42x and GET at 2.85x
  against the faster rival.
- **M0 throughput and latency: pass.**
- **Complete M0 gate: not yet green.** The strict memory bar fails at 1.57x
  fresh-load RSS and the repeated-load peak grows to 2.47x.

Raw client CSVs, memory snapshots, server logs, binary hashes, and the generated
summary are stored beside this report. The exact runner is `run.sh`.
