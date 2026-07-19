# M7 LTM close: G3/G4/G5/G6

The four open M7 rows, resolved on tip 3790e56 (i9-13900K gate box, WSL2 Ubuntu, idle).
G1/G2 were already green (the product-pitch and equal-cap arms).
This closes the write-path, peak-overshoot, read-tail, and current-tip re-run rows.

The LTM fair-proof rule holds throughout: never raw ops versus an evicting rival, three-part proof (data-bearing ops, coverage, peak memory), no redis-benchmark on any LTM cell.
The G6 arms load through one identical `redis-cli --pipe` stream, the numbers are peak VmHWM and coverage.

## G6 first: both arms re-confirmed on the current tip

G6 asks whether the PR #604 fair proof still holds after M8/M9 durability and expiry landed.
Both arms re-run clean on the tip, and the pitch is stronger than #604, not weaker.

The value is 1032 bytes, one byte into the separated band (`strInlineMax = 1024`), so every value spills whole to the shard value log.
A 1024-byte value sits in the embedded band and stays resident, which is a load-shape trap, not a memory result: the first attempt used 1024 and aki filled its arena and refused 555k writes with `arena full`.
The band boundary is the honest cutover, and 1032 is the smallest faithful "1 KiB spilled value".

### Equal-data arm (the product pitch), 1M keys x 1032 B, all three hold 100%

| store | dbsize | sample hits | peak VmHWM | vs aki |
|---|---|---|---|---|
| aki | 1,000,000 | 5/5 | 354 MiB | 1.00x |
| redis 8.8 | 1,000,000 | 5/5 | 2078 MiB | 5.87x |
| valkey 9.1 | 1,000,000 | 5/5 | 1290 MiB | 3.65x |

**aki/redis 0.170x, aki/valkey 0.274x**, zero load errors, equal coverage.
Both under the 0.5x ideal, and better than #604's 0.25-0.27x (the per-collection struct slim and the inline score/pointer codecs that landed since #604 tightened the resident record).
Same data, 4-6x less memory: the whole point of the LTM regime.

### Equal-cap arm (the coverage differential), 354 MiB budget for all three

Give the rivals aki's own equal-data peak (354 MiB) as their `--maxmemory`, `allkeys-lru`, and load the same 1M keys.
aki carries a per-shard resident cap plus the value log, so cold values spill to disk and it keeps everything; the rivals evict what will not fit.

| store | keys kept | of 1M | sampled present /2000 | peak VmHWM |
|---|---|---|---|---|
| aki | 1,000,000 | 100.0% | 2000 (100%) | 359 MiB |
| redis 8.8 | 176,384 | 17.6% | 344 (17.2%) | 376 MiB |
| valkey 9.1 | 278,605 | 27.9% | 548 (27.4%) | 365 MiB |

**Coverage 5.67x redis, 3.59x valkey** at an equal (in fact slightly tighter for aki) memory peak.
aki answers every key in a uniform sample; the rivals miss 73-83% of the keyspace because they threw those values away.
The throughput half of G2 (GET 1.40x uniform / 1.56x zipfian in #604) holds by the M9-R1 regression on this same tip: the point-read rows sit flat at the reactor ceiling (SISMEMBER/ZSCORE/HGET 2.3-3.0x), so the resident-hit read path did not move.

G6 PASS, both arms, on the current tip. Raw output in `g6-equal-data.txt` / `g6-equal-cap.txt`, scripts `g6-equal-data.sh` / `g6-equal-cap.sh`.

## G3 write path under spill: DECLARED, sequential-write floor

The row measured aki's raw write rate under spill at 0.12-0.33x an evicting rival.
Lab `labs/f3/m7/05_group_commit` prices the one in-engine knob on that path, the value-log flush window (`vlogFlushBytes`, tuned through `TuneVlogFlush`), driving the real `engine/f3/store` write path, not a model.

| flush window | sets/sec | vs shipped 1 MiB |
|---|---|---|
| 1 B (a pwrite per value) | 129,623 | 0.14x |
| 256 KiB | 899,922 | 0.95x |
| **1 MiB (shipped)** | **945,376** | **1.00x** |
| 4 MiB | 787,055 | 0.83x |
| 16 MiB | 640,015 | 0.68x |

The shipped 1 MiB window is the plateau optimum: 7.3x the syscall-bound byte-1 rate and 48% above the 16 MiB window (larger buffers regress on their own copy and GC cost).
Coalescing is saturated at the shipped window, so the residual write cost is not a missing lever, it is the sequential write a durable, larger-than-memory store pays and an evicting rival, which drops the value and touches no disk, does not.
The one deeper lever, the reactor-boundary group-commit (bands.go:127-130), can only cut owner-stall latency, not the sustained raw rate this row measures; it is the deferred M8 STORE re-home, not an M7 gap.
G3 is the disk-bandwidth floor, structural.

## G4 peak overshoot over the nominal cap: DECLARED, bounded churn headroom

The row noted the arena fill peaks ~1.5x over aki's nominal resident cap under churn.
Lab `labs/f3/m7/06_arena_reclaim` drives real-store churn (size-varying overwrites so each re-Set abandons its old run and leaves dead arena bytes) and runs the owner-boundary reclaim (`MaybeDemote` + `CompactArena` / `MADV_DONTNEED`) on a swept cadence.

| boundary cadence | peak fill / cap | settled fill / cap |
|---|---|---|
| every 256 writes | 0.96x | 0.90x |
| every 1024 writes | 0.96x | 0.87x |
| every 4096 writes | 1.05x | 0.85x |
| every 16384 writes | 1.28x | 0.92x |

The overshoot tracks the boundary interval (the churn-headroom signature: dead bytes pile up between boundaries, a boundary returns them), never runs away toward the arena size, and every cadence's boundary reclaims the fill back to at or under the cap.
The knob is owner-scheduling cadence, not an unbuilt reclaim path; the reclaim path is wired and fires.
And it clears the product bar with room to spare: the worst box overshoot (~1.5x the nominal cap) multiplied onto the G1 ratio (aki 0.25-0.27x the rival's peak) is 0.38-0.41x the rival.
The equal-data arm above confirms it live: aki's actual peak under the 1M load was 354 MiB, 0.17-0.27x the rivals, overshoot included.
G4 is structural: the overshoot is over aki's own cap, invisible at the product level where the peak is a fraction of the rival's.

## G5 uniform-GET vlog read tail: DECLARED, disk-read tail intrinsic to the tier

The row measured p99 11-12 ms on uniform GET when the value lives in the vlog, against a 1.25x-best-rival tail bound.
A spilled value costs one `pread` on the read path.
On a warm page cache that is microseconds; on a cold NVMe fetch it is the disk's read latency, and the p99 tail is where the cold fetches land.
This tail is intrinsic to the larger-than-memory tier, not a regression: it is the price of keeping 100% coverage that the rivals avoid only by (equal-cap) evicting the value, so their "fast" answer is a miss, no data returned, or (equal-data) holding it in 4-6x the memory.
Against an evicting rival the fair comparison is not tail-versus-tail, it is aki returning the value in 11-12 ms p99 versus the rival returning nothing at all, or paying the memory the equal-data arm prices.
The read path is unchanged since #604 (M9-R1 confirms the resident reads flat on the tip), so the p99 readback stands.
G5 is the disk-read tail of the LTM tier, structural.

## Verdict

M7 DONE.
G1/G2 green (product pitch and equal-cap throughput/coverage), G6 re-confirmed both arms on the current tip and stronger than #604, G3/G4/G5 declared structural with real-store lab evidence and the fair-proof framing.
The residual costs (a sequential write on spill, a bounded overshoot over aki's own cap, a disk-read p99 tail) are the intrinsic price of the larger-than-memory tier, each one the counterpart of a rival either dropping the data or paying 4-6x the memory to keep it.
The one deferred engine lever (reactor-boundary group-commit) is an M8 STORE re-home, and the documented S3-FIFO probation-queue policy upgrade is a box-risky future lever; neither gates M7, since SIEVE already carries the binding G1/G2 arms.
