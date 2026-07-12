# f3 M3 list exit gate: indexed reads and head-tail ops clear 2x, positional writes are O(n) and time out

Run date 2026-07-12, gate box GamingPC, same campaign as the M2 gate (results/f3/m2-gate.md).
The list type splits cleanly along one line: reads and end operations pass, position-dependent writes collapse.
Indexed read (LINDEX) clears the bar at 4.06x on a 1M list and 7.99x at 10k, the head and tail ops (LPUSH, LPOP, RPOPLPUSH) clear 2.2x to 2.5x, and the cardinality-band LINDEX passes 4x to 6.8x, so the read and end paths are genuinely ahead of both rivals.
But LSET at a deep index runs at 0.028x on a 1M list (12k ops/s against 425k), LINSERT on a 200k list and LREM on a 2M list both fail to complete a single measured rep inside the 240s watchdog, and LRANGE runs at 0.51x to 0.52x, the same range-read regression the zset ZRANGE cells show.
The signature is one structure with fast indexed access and an O(n) positional-mutation path: LINDEX at 4.06x and LSET at 0.028x on the identical 1M list is the whole story.
No M3 predictions were pre-registered in results/f3/predictions.md before this run (the ledger stops at M2), so this note records the numbers as the M3 baseline rather than adjudicating filed predictions, and it flags that process gap for the rerun.
No constant was tuned mid-run.
This note records the numbers and hands the misses to follow-ups.

Part of the M3 exit gate, tracking issue #545.

## Provenance

Same box, binaries, rivals, and harness as the M2 gate; see results/f3/m2-gate.md for the full provenance block. In summary:

- Box: GamingPC, i9-13900K, 56 GiB, WSL2 6.18.33.2-microsoft-standard-WSL2, 32 logical CPUs, go1.26.0.
- aki commit f53ce7995d9fad023c8c43a9e79fa20e70bfc95b, f3srv sha256 2bc1b6cae14364ba97686f5259d3985abb9a29db69e141abbb4120ef65a47c2a.
- aki-bench commit dd865386b30f6e305e9f95ed7b0a9317bdd12ff8; driver committed as m3-gate/runner.py.
- Rivals Redis 8.8.0, Valkey 9.1.0, io-threads 4, fresh per batch, persistence off.
- Servers pinned CPUs 0-7, load generator 8-15. f3srv shards 4 (M3 has no algebra group).
- aki-bench connect mode, 3 reps per cell, FLUSHALL plus preload per rep.
- Reduction here is the median over the 3 reps, not the minimum, because the cold first rep after a fresh preload is a cache-warmup outlier on the fast point cells (band_500 read 0.62x on rep 0 then 4.01x and 4.13x; llen and lindex show the same smaller warmup step). The per-rep ratios are listed in the notes column so the warmup is visible. The catastrophic cells are flat across reps, so median and minimum agree there.
- Per-rep watchdog: a 240s timeout kills a hung aki-bench and records the cell with empty reps. LINSERT and LREM hit it on all three reps.
- Raw per-cell JSON stays on the box under /root/f3gate/m2m3/m3/cells/.

## Gate table

Bar: 2.0x over the worse-for-aki rival, median over reps.
Members is the list length under test; the hot cells drive a fixed request count against a preloaded list.
The reps column lists the per-rep min-over-both-rivals ratio so the warmup and the flatness are visible.

Reads and length:

| cell | len | aki | redis | valkey | ratio | verdict | reps |
|---|---|---|---|---|---|---|---|
| llen_c10k | 10k | 10.82M | 7.28M | 4.71M | 1.49x | MISS | 1.64, 1.46, 1.49 |
| llen_c1m | 1M | 10.96M | 7.22M | 4.70M | 1.52x | MISS | 1.52, 1.54, 1.50 |
| lindex_c10k | 10k | 9.41M | 1.18M | 1.08M | 7.99x | PASS | 7.60, 7.99, 8.03 |
| lindex_c1m | 1M | 3.93M | 967k | 913k | 4.06x | PASS | 4.06, 4.11, 4.04 |
| lpos_c10k | 10k | 8698 | 24693 | 20519 | 0.35x | MISS | 0.35, 0.35, 0.35 |
| lpos_c1m | 1M | 577 | 261 | 209 | 2.21x | PASS | 2.21, 2.21, 2.17 |

Cardinality-band LINDEX:

| cell | len | aki | redis | valkey | ratio | verdict | reps |
|---|---|---|---|---|---|---|---|
| band_100 | 100 | 5.63M | 3.34M | 2.78M | 1.69x | MISS | 1.71, 1.69, 1.68 |
| band_500 | 500 | 8.57M | 2.15M | 1.74M | 3.98x | PASS | 0.62, 4.01, 4.13 |
| band_2000 | 2000 | 8.31M | 1.21M | 1.10M | 6.85x | PASS | 6.85, 6.82, 7.34 |
| band_130k | 130k | 8.27M | 1.23M | 1.13M | 6.74x | PASS | 6.44, 6.74, 6.74 |

Range read, reduced concurrency (LRANGE, 100-element window):

| cell | len | aki | redis | valkey | ratio | verdict | reps |
|---|---|---|---|---|---|---|---|
| lrange_c10k | 10k | 163k | 244k | 312k | 0.52x | MISS | 0.52, 0.52, 0.52 |
| lrange_c1m | 1M | 89k | 113k | 175k | 0.51x | MISS | 0.51, 0.50, 0.51 |

Head and tail writes (fixed request count against a 2M-element list):

| cell | len | aki | redis | valkey | ratio | verdict | reps |
|---|---|---|---|---|---|---|---|
| rpushtail_hot | 2M | 7.95M | 4.18M | 3.13M | 1.90x | MISS | 3.19, 1.83, 1.86 |
| lpushhead_hot | 2M | 8.80M | 3.75M | 2.91M | 2.35x | PASS | 2.34, 2.13, 2.44 |
| lpop_hot | 2M | 8.10M | 3.62M | 2.75M | 2.24x | PASS | 2.19, 2.37, 2.26 |
| rpoplpush_hot | 2M | 6.87M | 2.73M | 2.11M | 2.52x | PASS | 2.37, 2.52, 2.76 |

Positional writes (the O(n) family):

| cell | len | aki | redis | valkey | ratio | verdict | reps |
|---|---|---|---|---|---|---|---|
| lset_c10k | 10k | 6.23M | 762k | 668k | 8.18x | PASS | 5.33, 8.12, 8.29 |
| lset_c1m | 1M | 12277 | 425k | 443k | 0.028x | MISS | 0.01, 0.03, 0.04 |
| linsert_hot | 200k | timeout | - | - | - | MISS | 240s watchdog, 3/3 reps |
| lrem_hot | 2M | timeout | - | - | - | MISS | 240s watchdog, 3/3 reps |

Memory, peak VmHWM multiple over the best rival, median rep (all cells): aki holds 3.0x to 37.3x the rivals' peak, the same fixed-arena and connection-preallocation cost the M2 gate isolated (marginal bytes per entry there was leaner than both rivals; the multiple is fixed overhead, not data). The multiple is largest on the small hot-key cells (lset_c10k 37.3x, rpoplpush_hot 35.3x) where the fixed arena dwarfs the tiny dataset.

## Verdict

M3 does not pass the 2.0x gate as a whole, but it is a sharper split than M2: the read and end-operation half of the list passes cleanly, the positional-mutation half collapses.

Passes (median >= 2x): LINDEX at 10k and 1M, the LINDEX bands from 500 up, LPUSH head, LPOP, RPOPLPUSH, LPOS on the 1M list, and LSET on the 10k list.

Misses handed to follow-ups, ranked by size of the lever:

1. Positional writes are O(n) with a bad constant. LSET at a deep index on a 1M list is 0.028x, and LINSERT (200k list) and LREM (2M list) do not finish a single rep in 240s. The tell is LINDEX at 4.06x versus LSET at 0.028x on the identical 1M list: indexed read is fast, indexed write walks and rewrites O(n). This is the list-mutation path, not the access path, and it is the top M3 lever. Book on #545, and verify LINSERT/LREM with a direct micro-timing since the watchdog leaves them without a number.
2. Range reads (LRANGE) at 0.51-0.52x, identical to the zset ZRANGE miss. This is the shared collection range-read path (leaf or node walk plus RESP array encode), 2x slower than Valkey on a 100-element window across both list and zset. One fix serves both milestones; this is the single cross-type lever. Book on #545 and cross-reference #544.
3. Fixed-overhead losses on small inputs. LPOS at 10k is 0.35x while LPOS at 1M is 2.21x, the same fixed-per-op signature as the M2 ZUNIONSTORE cell: aki's per-element work is faster (it wins the large scan) but a fixed per-op cost dominates the small one. band_100 at 1.69x is the small end of the same curve. Traceable to the same fixed collection cost as the peak-VmHWM multiple.
4. Length and small-band point reads (LLEN 1.49-1.52x, band_100 1.69x, RPUSH tail 1.90x) sit at the reactor dispatch ceiling, the same ~1.5x floor M0 measured; the io_uring driver (task #9) is the lever.

Process note: no M3 predictions were filed in the ledger before this run. The rerun after the positional-write and range-read fixes should pre-register floors from these numbers per doc 19 rule 1.3.
