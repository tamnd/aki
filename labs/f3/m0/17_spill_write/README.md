# Lab 17: the LTM spill write path

The question: run 3 measured SET uniform on a full store (2M x 1032B, aki resident cap 512MiB) at 0.18x the best rival with p99 14ms, while the GET cells on the same setup win.
Where does an over-cap SET's time actually go, and which lever fixes it: writing overwritten cold values straight to the log instead of arena-then-demote, batching the log appends, or both?

## Method

In-process, no server, no wire, one cell per invocation so maxrss is honest.
The dataset and cap are the run 3 LTM shape: 2M keys of 1032B values (separated band), resident cap 512MiB, a quarter of the value bytes.
The loop emulates the shard worker's boundaries exactly as lab 13 did: batches of 1024 SETs, then the between-drains demote-or-compact check, verbatim.
The sweep axes are the placement policy for an overwrite of a log-resident value (`-place arena`: admit then demote later, the pre-slice behavior; `-place log`: append the new bytes straight to the log) and the vlog pending-buffer flush threshold (`-flush 1` flushes every append, the pre-slice synchronous posture).
Batch p99 is the wall time of 1024 SETs plus the boundary check, the in-process stand-in for the wire p99.

Run one cell:

    go run ./labs/f3/m0/17_spill_write -dist uniform -place log -flush 0

## Diagnosis (darwin arm64, 2026-07-10)

The arena arm reproduces the run 3 pathology in-process: 0.10M sets/s and 0.83 demotions per uniform SET.
A CPU profile of that arm says where the time goes: 56% in relocateLive (CompactArena's full-index walk, dragged in at nearly every boundary because demotion keeps punching 1KiB dead holes across the arena segments), 33% in pwrite syscalls (the demotion flushes), and most of the rest scanning buckets under demoteBucket.
The structural read: a uniform overwrite of a log-resident value was admitted to the arena only for the demotion hand to move it right back to the log, and the round trip dragged the boundary IO and a 2M-entry compaction walk with it.
A write carries no read evidence, so under the residency policy's own logic those bytes never belonged in the arena in the first place.

## Results (darwin arm64, 2026-07-10)

flush=0 keeps the shipped default (vlogFlushBytes, 1MiB).
This box's disk write path saturates near 240MB/s, so the log arms here are IO-bound floor numbers; the placement delta, the demotion column, and the p99 shape are the transferable facts.

| dist | place | flush | sets/s | batch p99 | batch max | log B/set | demotes/set | maxrss |
|---|---|---|---|---|---|---|---|---|
| uniform | arena | 1 | 0.09M | 33.4ms | 107.6ms | 856 | 0.83 | 727MiB |
| uniform | arena | 0 | 0.10M | 30.6ms | 68.0ms | 856 | 0.83 | 732MiB |
| uniform | log | 1 | 0.19M | 15.7ms | 33.1ms | 856 | 0 | 727MiB |
| uniform | log | 64Ki | 0.22M | 15.6ms | 33.4ms | 856 | 0 | 727MiB |
| uniform | log | 0 | 0.28M | 14.8ms | 26.1ms | 856 | 0 | 732MiB |
| uniform | log | 4Mi | 0.26M | 38.0ms | 57.0ms | 856 | 0 | 747MiB |
| zipfian | arena | 0 | 0.37M | 25.2ms | 37.5ms | 196 | 0.19 | 732MiB |
| zipfian | log | 0 | 0.36M | 12.6ms | 25.0ms | 677 | 0 | 732MiB |

A profile of the uniform log arm at the default flush is 97.6% pwrite: the cell is pure disk-write bandwidth once the churn is gone, which is the honest floor for a store that persists every spilled value where the capped rivals evict theirs.

Lab 13's frozen residency cells, rerun on the new code as the no-regression check: zipf s=0.99 cap 512MiB two-touch dk=8 reads 0.2162 log reads/GET at 78.38% hit (frozen: 0.216, 78.4%), uniform 0.8295 at 17.05% (frozen: 0.830, 17.0%).
Demotion batching through the shared buffer behaves identically to the retired private staging.

The known trade the zipfian rows price: a write-hot, rarely-read key stays log-resident under log-direct placement (write heat never promotes), so a SET-only zipfian workload appends 677B/set where arena placement appended 196B.
The gate LTM cells (SET uniform, GET uniform, GET zipf) do not sit in that regime, reads still promote as before, and the p99 is halved even there; write-heat marking is a candidate follow-up if a mixed write-hot cell ever gates.

## Results (GamingPC i9-13900K WSL2, wire A/B, 2026-07-11)

Same-session interleaved A/B at run 3's LTM protocol: one f3srv arm at a time on the run 3 posture (4 shards, 256MiB arena, 128MiB resident cap each, fresh vlog dir per arm), rivals redis 8.8/valkey 9.1 at maxmemory 512mb allkeys-lfu up for the whole cell, arm order alternating per rep, aki-bench 3 reps of warm 3s + 20s, FLUSHALL between invocations.
old = 0082229 (the run 3 binary state), new = this slice; VmRSS sampled at 1s.

| cell | arm | vops/s (3 reps) | median | p50 | p99 | p999 | aki RSS peak |
|---|---|---|---|---|---|---|---|
| SET uniform | old | 192.2k / 192.0k / 192.6k | 192.2k | 544-612us | 13.8-14.1ms | 1.2-1.8s | 951MiB |
| SET uniform | new | 334.2k / 325.5k / 317.2k | 325.5k | 496-546us | 12.7-13.0ms | 380-390ms | 1128MiB |
| GET uniform | old | 802.7k / 782.8k / 780.7k | 782.8k | ~1.1ms | 11.4-11.6ms | 14.7-15.5ms | 744MiB |
| GET uniform | new | 932.9k / 927.2k / 930.6k | 930.6k | ~0.88ms | 11.0-11.4ms | 12.0-12.5ms | 749MiB |
| GET zipf | old | 1.85M / 1.80M / 1.72M | 1.80M | ~0.49ms | 1.2-1.3ms | 12-13ms | 737MiB |
| GET zipf | new | 1.97M / 1.85M / 1.86M | 1.86M | ~0.46ms | 1.1-1.2ms | 12ms | 744MiB |

SET uniform is 1.69x the old binary in the same session, and the rival reference in the same invocations held flat (redis 1.06-1.08M, valkey 0.94-0.97M), so the cell ratio moves from 0.18x to 0.31x.
The tail is the bigger story: p999 falls from 1.8s to 385ms because the demote-then-compact churn is gone from the steady state, and the remaining p99 (12.8ms) now matches the winning GET uniform cell's 11ms, which is this cell's disk-IO envelope, not a write-path pathology.
Both GET control cells improved (1.19x and 1.04x), so no read regression; the uniform gain is the same churn removal seen from the read side, since the measured window no longer competes with demotion IO from the preload's overwrites.
Rival value-bearing GETs were 661k (uniform) and 1.31-1.33M (zipf), so the read cells now sit at 1.41x and 1.40x.

Memory: the new arm's SET-cell VmRSS peak is 1128MiB against the old arm's 951MiB in the same session, 2.20x the 512MiB cap against run 3's recorded 2.45x, with rivals peaking at 535MiB (redis) and 894MiB (valkey).
The bar was "do not raise the 2.45x peak" and it holds; the delta against the old arm tracks the 1.7x higher write volume per window, not a new resident structure, and the read cells' peaks moved less than 1%.

What is left on the table: the new arm still runs ~4.5M demotions per SET window because an overwrite of an arena-resident value still lands in the arena and pushes the hand.
Log-directing those too would kill the remaining churn, but it would also evict read-promoted keys on their first write, which the gate GET cells never price; that is the follow-up to weigh, not a free move.
The vlog grew to 2.3-2.6GiB per 20s window (old: 1.7GiB) since compaction still only runs at idle boundaries; unbounded growth under sustained saturation is a known pre-existing behavior, unchanged here.

## Verdict

Log-direct placement for cold overwrites plus batched vlog appends at the shipped 1MiB flush threshold.
Frozen: SET uniform LTM 192k to 325k vops/s (1.69x, cell ratio 0.18x to 0.31x), p999 1.8s to 385ms, GET uniform 1.19x and GET zipf 1.04x better, RSS peak 2.20x cap versus the 2.45x bar, lab 13's residency numbers unchanged.
The cell is now inside its disk-IO envelope; the next lever is placement for arena-resident overwrites, priced against read-promotion loss.
