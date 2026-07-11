# f3 M1 set exit gate: the kernels hold, the 2.0x wire bar does not

Run date 2026-07-11, gate box GamingPC.
Enumeration and the count-form draws clear 2x, the algebra STORE family recovers from v1's 0.30-0.55x to 1.2-4.5x, and every carried command beats both rivals, but the 2.0x floor is missed on the point ops, the equal-overlap SINTER row, and the hot SPOP row.
SPOP lands at 1.50M ops/s, inside the prediction's own 1.06M-2.1M band, so PRED-F3-M1-SPOP engages the F13 partitioned-draw escalation rather than failing the milestone outright.
The memory bar is missed: aki holds a 1M-member set at about 56.5 bytes per member of process RSS against Valkey's 36.7.
No constant was tuned mid-run; the frozen thresholds from the M1 labs were used everywhere.
This note records the numbers, adjudicates the five filed predictions, and hands the misses to follow-ups.

Part of the M1 exit gate, tracking issue #543.

## Provenance

- Box: GamingPC, i9-13900K, 56 GiB RAM, WSL2 kernel 6.18.33.2-microsoft-standard-WSL2, NVMe-backed ext4.
- aki commit 09011f8fd01e49a28be30c799658aa878d9e768e (main tip at run start), go1.26.0.
- f3srv sha256 71af35efa4a02dc3bbe9241ba865eb745e486fe272e0c7a7fdca9ad03f4f9a2d, built linux/amd64 at that commit.
- aki-bench sha256 13ca3b0c33e3d2c2f8a9c371eb4d7317bf4f41b7ef1b402da70914c4c17f0cce, built at aki-bench b54ddb475174756cb08326704e68dc2772bed64c with uncommitted local additions to the set workloads (single-set probe and algebra shapes); the driver script is committed next to this note as m1-gate/runner.py so the cell definitions are reproducible.
- Rivals: Redis 8.8.0 (jemalloc 5.3.0, build 5f155628c849f81c, sha256 3bea75409d8182add0459b2765cd62cd5962ffbcf8adb8ccf637205e70cbadb0), Valkey 9.1.0 (jemalloc 5.3.0, build 62d4a6f3cac454c6, sha256 3ee5b35da0bd9916c7058c83221972d06fe69182e0790029a183ca8ffd0cf4af), both with io-threads 4 (the setting the M0 sweep froze), launched fresh per batch in clean workdirs with persistence off.
- CPU split: servers pinned to CPUs 0-7 via taskset, the load generator to CPUs 8-15, same split for aki and both rivals.
- f3srv shards: 4 for the main matrix (the DefaultShards formula on the 8-CPU mask); 1 for the algebra cells, because f3srv routes keys by hash with no hash-tag support and the algebra operands must co-locate on one owner.
That single-shard framing understates aki on algebra relative to a co-location surface, and the rivals are single-threaded on the data plane anyway, so the algebra ratios are honest per-core numbers.
- Harness: aki-bench connect mode, closed loop, one process driving all three servers interleaved per cell, 3 measured reps per cell, warm inside each rep, fresh FLUSHALL plus preload per rep.
redis-benchmark has no set-shape coverage beyond its defaults, so this campaign is single-harness; ratio is the minimum over both rivals and then over reps.
- Default drive is 512 connections at pipeline 16; cells that return large or per-op-array replies run at reduced concurrency to bound client buffers, and the reduction is recorded per row.
- Memory: VmHWM per server from /proc with a clear_refs peak reset before every rep; baselines (kB) aki 6824, redis 9120, valkey 9296 (main batch) and aki 6740, redis 8832, valkey 9104 (algebra batch).
- Raw per-cell JSON stays on the box under /root/f3gate/m1-gate/; the reduced table is committed as m1-gate/cells.csv, the microbenchmark outputs as m1-gate/portconfirm.txt and m1-gate/lab0{1,2,3}.txt, the memory probe as m1-gate/memprobe.txt.

## Gate table

Bar: 2.0x over the worse-for-aki rival, minimum over reps.
Ops columns show the rep that produced the minimum ratio.
All members are the harness default short members; the single set under test is the band its cardinality dictates (inline listpack at 1 and 10, native at 10k through 130k, partitioned above 262144).

Point ops and membership:

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| sismember_c1 | 1 | 8.90M | 6.77M | 4.33M | 1.31x | MISS |
| sismember_c10 | 10 | 9.67M | 6.66M | 4.27M | 1.45x | MISS |
| sismember_c10k | 10k | 9.09M | 6.39M | 4.31M | 1.42x | MISS |
| sismember_c1m | 1M | 8.51M | 6.07M | 4.29M | 1.40x | MISS |
| saddmember_c1 | 1 | 9.00M | 6.63M | 4.39M | 1.36x | MISS |
| saddmember_c10 | 10 | 8.46M | 6.51M | 4.40M | 1.30x | MISS |
| saddmember_c10k | 10k | 7.79M | 6.26M | 4.20M | 1.25x | MISS |
| saddmember_c1m | 1M | 8.35M | 5.99M | 4.33M | 1.39x | MISS |
| scard_c10k | 10k | 9.53M | 7.38M | 4.71M | 1.29x | MISS |
| scard_c1m | 1M | 10.13M | 7.37M | 4.76M | 1.38x | MISS |
| smismember_c10k | 10k | 427k | 380k | 320k | 1.13x | MISS |
| smismember_c1m | 1M | 424k | 368k | 318k | 1.15x | MISS |
| srem_c1m | 2M | 6.23M | 5.98M | 3.50M | 1.04x | MISS |
| smove_c1m | 2M | 6.46M | 4.86M | 3.07M | 1.33x | MISS |

Draws:

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| srandmember_c1 | 1 | 5.02M | 4.75M | 1.50M | 1.06x | MISS |
| srandmember_c10 | 10 | 5.27M | 4.83M | 1.23M | 1.09x | MISS |
| srandmember_c10k | 10k | 4.79M | 3.50M | 1.41M | 1.37x | MISS |
| srandmember_c1m | 1M | 5.02M | 2.22M | 786k | 2.27x | PASS |
| srandcount_c10k | 10k | 92k | 51k | 16k | 1.83x | MISS |
| srandcount_c1m | 1M | 93k | 35k | 10k | 2.65x | PASS |
| spop_native10k | 200k | 1.50M | 1.79M | 800k | 0.84x | MISS |
| spop_hot4m | 4M | 1.50M | 1.15M | 395k | 1.30x | MISS |
| spop_hot4m_p1 | 4M | 277k | 303k | 346k | 0.80x | MISS (recorded) |

Enumeration:

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| smembers_c1 | 1 | 461k | 216k | 91k | 2.13x | PASS |
| smembers_c10 | 10 | 461k | 216k | 92k | 2.13x | PASS |
| smembers_c10k | 10k | 8.4k | 4.1k | 2.7k | 2.04x | PASS |
| smembers_c1m | 1M | 83.6 | 25.4 | 20.7 | 3.30x | PASS |
| sscan_c10k | 10k | 999k | 293k | 374k | 2.67x | PASS |
| sscan_c1m | 1M | 463k | 351k | 345k | 1.32x | MISS |

Algebra, single shard, operands co-located (a and b at the stated cardinality, 50 percent overlap):

| cell | card | aki | redis | valkey | ratio | verdict |
|---|---|---|---|---|---|---|
| sinter_1m | 1M | 30.2 | 9.1 | 16.4 | 1.84x | MISS |
| sinter_256 | 256 | 60k | 65k | 90k | 0.66x | MISS |
| sinter_10 | 10 | 53k | 41k | 42k | 1.28x | MISS |
| sintercard_1m | 1M | 46.3 | 12.4 | 27.2 | 1.70x | MISS |
| sinterstore_1m | 1M | 16.8 | 6.4 | 11.5 | 1.47x | MISS |
| sinterstore_10k | 10k | 4477 | 1262 | 1569 | 2.85x | PASS |
| sunionstore_1m | 1M | 8.5 | 3.5 | 7.1 | 1.20x | MISS |
| sunionstore_10k | 10k | 3171 | 701 | 680 | 4.52x | PASS |
| sdiffstore_1m | 1M | 16.5 | 6.4 | 11.2 | 1.48x | MISS |
| sdiff_1m | 1M | 30.6 | 6.0 | 9.1 | 3.36x | PASS |
| sunion_1m | 1M | 9.7 | 3.0 | 4.9 | 1.98x | MISS |

10 PASS, 29 MISS on the 2.0x bar, plus one recorded-only P1 row.

Row notes:

- Non-store algebra and the large SMEMBERS rows return the full member array per op, so they run at reduced concurrency (sinter/sdiff/sunion and sinter_256/sinter_10 at 8 connections pipeline 1, smembers_c10k at 50x1, smembers_c1m at 16x1, sscan_c1m at 50x4); the 1M STORE and CARD rows run at 16x1 because each op is O(1M) server work and a single owner saturates on a handful of connections.
Rivals ran the identical reduced drive, so the ratios are like for like.
- sinterstore_10k was first run at the default 512x16 and aki collapsed to about 15 ops/s with a 60 s p99 while the rivals degraded gracefully to 1.3-1.6k; the committed row is the 16x1 re-run at 2.85x, and the collapse is recorded as a robustness follow-up below, not silently discarded.
- SREM, SMOVE and SPOP are destructive, so those rows run request-capped at half the preloaded cardinality (2M sets, 1M requests; SPOP 4M and 2M) to keep the set populated through the measurement.
- spop_hot4m_p1 is the pipeline-1 record row (50 connections), same shape as the M0 P1 record: aki 277k against redis 303k and valkey 346k, 0.80x, the per-op hop latency with no batch to amortize it.

## Band-transition sweep

SISMEMBER on one set at cardinalities crossing every threshold (listpack cap 128, engagement 262144), same drive per cell:

| card | band | aki | ratio |
|---|---|---|---|
| 100 | inline | 7.95M | 1.25x |
| 500 | native | 7.25M | 1.14x |
| 2000 | native | 7.14M | 1.13x |
| 130k | native | 7.02M | 1.15x |
| 300k | partitioned P4 | 7.18M | 1.14x |
| 2M | partitioned P8 | 7.49M | 1.23x |

No cliff and no double-cost window: aki absolute throughput stays inside 7.0-8.0M across every transition and the ratio never leaves the 1.13-1.25 band.
The exit-gate item on band transitions is met even though the absolute ratios sit under the 2x bar with the rest of the point ops.

## Hot-key SPOP

The headline F2 row: one hot set, partitioned band engaged, P16 drive at 512x16, request-capped so cardinality stays in the millions.

- aki 1.50M ops/s, redis 1.15M, valkey 395k; ratio 1.30x, minimum over reps.
- The frozen floor is absolute: clear the Valkey bar of 2.12M ops/s, target 4.2M.
aki at 1.50M misses the floor.
- The same prediction pre-registered the reading: below 1.06M kills F2's story, 1.06M-2.1M engages the F13 partitioned-draw escalation rather than failing the milestone.
1.50M is inside that band, so F13 is engaged.
- Context the adjudication does not lean on: on this box at this shape neither rival comes near its own historical bar either (valkey 395k against the 2.12M its 8.1 lineage posted on the f1 box), so part of the absolute miss is the box and the closed-loop drive, but the bar was frozen as an absolute and is scored as written.
- The native-band row is a real miss with no band to hide in: spop_native10k at 0.84x, aki 1.50M against redis 1.79M.
aki SPOP throughput is flat from 200k to 4M members, which says the draw kernel is not the limiter; the per-op wire and dispatch path is.

## Algebra shapes

Equal-overlap 1M-by-1M lands at 1.84x (SINTER), 1.70x (SINTERCARD), 1.98x (SUNION), 3.36x (SDIFF); the STORE forms land at 1.20-1.48x at 1M and 2.85-4.52x at 10k.
The asymmetric skewed-pair shape the SINTER prediction names is not in this run: the harness set workloads build equal-cardinality operand pairs, and the gap is recorded as a follow-up rather than papered over with a hand-built cell.
The small-pair sinter_256 row at 0.66x is the one algebra row where aki loses outright; 256 members sits just above the listpack cap on both sides, where the rivals intersect two listpacks by linear scan and aki pays the fixed native-table walk plus reply build.
The v1 flank this milestone attacked is confirmed recovered: v1 STORE forms ran 0.30-0.55x, this run's worst STORE row is 1.20x and the 10k rows clear 2x with room.

## Memory column

The F14 bar is bytes per member at or under the Valkey embedded-entry bar, peak counts.

Steady-state probe (one 1M-member set, low-concurrency preload, 8 s settle, full table in m1-gate/memprobe.txt):

| server | RSS delta B/member | HWM delta B/member | MEMORY USAGE B/member |
|---|---|---|---|
| aki | 56.5 | 56.5 | no surface |
| redis | 31.7 | 34.7 | 41.3 |
| valkey | 36.7 | 41.7 | 32.7 |

aki holds the same 1M members in about 1.5x Valkey's process RSS and 1.35x its peak, against a bar of at or under.
MISS.
The slice-2 structural accounting of 21.71 B/member remains true of the table itself; the gap is everything around it, the Go heap headroom the GC keeps, per-shard slab and vector growth padding, and reply buffers, which the process-level bar rightly counts.
Under the full-rate campaign drive the per-cell VmHWM columns in cells.csv are worse (aki 88-242 B/member at 1M-4M cardinalities against valkey 43-47), because pipelined preload at 512 connections balloons transient heap; the steady probe is the fair floor and it still misses.
aki also has no MEMORY USAGE or used_memory surface, so rivals report themselves and aki is measured from /proc; the missing surface is recorded again as a gap.
During the probe aki answered DBSIZE 0 with the set present and serving, the known wb-drift DBSIZE bug, noted here because it also blocks any INFO-keyspace-based accounting.

## Carried v1 passes

The f1 carried wins (K3 SISMEMBER 7.50x/8.04x, K4 SADD 6.14x-7.57x, K11 SRANDMEMBER count 7.51x/14.13x, K14 SMEMBERS 2.5x/3.2x) were measured on the v1 multi-key spread harness, which this campaign's single-set cells do not reproduce, so like-for-like ratios are not available and this table reads holds-above-parity, not matches-the-old-number.

| carried | this run | held |
|---|---|---|
| K3 SISMEMBER | 1.31-1.45x across all bands | yes, above parity everywhere |
| K4 SADD | 1.25-1.39x across all bands | yes, above parity everywhere |
| K11 SRANDMEMBER count | 1.83x at 10k, 2.65x at 1M | yes, clears 2x at 1M |
| K14 SMEMBERS | 2.04-3.30x across all bands | yes, clears 2x everywhere |

No carried command regressed below parity, so nothing here blocks under the any-regression-blocks rule, but the spread-shape rerun is owed before the carried numbers can be called matched.

## SSCAN tail

v1 passed SSCAN throughput with the tail failing; this run reads both.

- sscan_c10k: 2.67x throughput, aki p99 19.8ms against the better rival's 29.3ms; tail better than both rivals.
- sscan_c1m: 1.32x throughput, aki p99 879us against the better rival's 759us, 1.16x, inside the 1.25x tail allowance the M0 gate used.

Tail green on both rows; the 1M row's throughput sits under 2x with the rest of the large-set wire-bound rows.

## Linux confirms of the frozen constants

The M1 labs froze their constants on darwin with the Linux read deferred to this run; all four confirms ran on the box at the gate commit (m1-gate/portconfirm.txt, lab01.txt, lab02.txt, lab03.txt).

- Member table (lab 01): at 7/8 load the group-stepped SWAR probe examines 1.23 groups per lookup against 2.66 for triangular and 4.46 for linear, same ordering and margins as darwin; the frozen 7/8 plus 8-wide groups stand.
- Inline threshold (lab 02): the listpack linear scan crosses the native-table cost between 16 and 32 entries on this box too (11.2ns at 16 against a 11.7-14.2ns flat table cost), so the 128 cap remains a Redis-parity constant well past its own perf crossover, as documented; caps stand.
- Merge-versus-probe crossover (lab 03): merge element cost 3.5-4.9 ns per element, stable across cardinality and member size; the model crossover at the 40ns DRAM probe brackets 7.3-10.5, so the pre-registered k=7 is confirmed as the DRAM-regime constant on Linux, with the same cache-resident caveat (measured crossover 1-2 on this box's large caches).
- Port bars (lab 06 rerun): SRandMember 100k 4.48ns beats the 4.8 bar; SRandMember 1M 16.79ns misses the 12.2 bar (darwin posted 11.17, so this is box DRAM latency on the partitioned locate, flagged below); SChurnMaintain4k 203.5ns beats the 411 bar by 2x.

The flat-merge question is settled: darwin's ~19 ns/member at 1M was not a machine artifact.
Linux posts 18.22 ns/member at 1M against 8.6 at 10k and 9.5 at 100k, so the flat single-thread merge is genuinely DRAM-bound at large cardinality, on both platforms, and the f1 ~6 ns/member figure was a per-partition cache-resident rate at P=256.
The 5.78ms 1M-by-1M K16 bar therefore stays contingent on the per-partition merge fan-out that #599/#601 deferred, exactly as the port-confirm lab framed it; nothing in the kernel regressed.

On SetAlgebraMaintain the judgment is to keep it default off through M1.
The gate's algebra misses are wire and single-thread merge/probe costs, not missing sorted-array freshness, and turning maintenance on would tax every SADD for a lever whose payoff (the partition-parallel merge) is not wired yet.
Revisit when the fan-out lands.

## Prediction adjudications

- PRED-F3-M1-SPOP: floor missed, escalation band entered.
Measured 1.50M ops/s hot-key P16 against the 2.12M floor and 4.2M target.
Per the prediction's own terms 1.06M-2.1M engages the F13 partitioned-draw escalation rather than failing the milestone outright; F13 is engaged, and the row stays a gate MISS.
- PRED-F3-M1-SINTER: falsified at the floor.
Equal-overlap 1M-by-1M measured 1.84x against the 2x floor; the skewed-pair half of the prediction was not measurable on this harness and is owed.
The named falsifier (inline sorted-array maintenance missing its write gate) did not occur; maintenance is off and the miss is single-thread merge cost plus wire, per the Linux flat-merge numbers above.
- PRED-F3-M1-STORE: falsified at the floor, flank recovered.
Floor 2x; measured 1.20x (SUNIONSTORE), 1.47x (SINTERSTORE), 1.48x (SDIFFSTORE) at 1M, with the 10k rows at 2.85x and 4.52x.
Against the v1 0.30-0.55x this is a 3-4x recovery, but the prediction is scored as written: MISS.
- PRED-F3-M1-INLINE: falsified for the point ops, held for enumeration.
Floor 2x at cardinality 1 and 10; measured SADD 1.30-1.36x, SISMEMBER 1.31-1.45x, SMEMBERS 2.13x.
MISS.
- PRED-F3-M1-SETMEM: falsified.
At or under the Valkey embedded-entry bar; measured 56.5 B/member steady-state process RSS against Valkey's 36.7 (peak 56.5 against 41.7).
MISS.

## Honest misses and the shape of the gap

The misses cluster into one shape: every cell whose per-op server work is small (point ops, small draws, small algebra) sits at 1.0-1.5x, and every cell whose per-op work is large (enumeration, count draws, big SDIFF, 10k STOREs) clears 2x.
aki's absolute point-op ceiling here (8.5-10.1M ops/s) is the same ceiling the M0 string gate measured, so the set kernels did their job and the remaining gap is the M0 wire and dispatch path, not set-model design.
The individual misses on their own terms:

- spop_native10k 0.84x and spop_hot4m_p1 0.80x: the only throughput rows where aki loses to a rival outright; SPOP's reply-plus-mutate path is more per-op wire work than SISMEMBER and redis's is cheaper.
- sinter_256 0.66x: just-above-listpack pair where rivals' listpack scan beats the native walk.
- sinterstore_10k at 512x16 collapsed (15 ops/s, 60 s p99) while rivals degraded to 1.3-1.6k; aki has no backpressure shaping for O(n) commands piling up on one owner.
The 16x1 re-run at 2.85x is the throughput truth, and the collapse is the robustness finding.
- SRandMember1M microbench 16.79ns against the 12.2ns f1 bar on this box (beat it on darwin); the partitioned prefix-sum locate pays box DRAM latency, worth a look but not gate-relevant since the wire dominates at 200x that cost.
- Memory 1.5x over the bar as measured at process level.

## Verdict

The M1 2.0x exit gate is not met.
10 of 39 gated cells pass, the band-transition and SSCAN-tail items are green, the carried commands hold above parity, and all frozen constants confirmed on Linux, but the point-op family, the equal-overlap SINTER row, the 1M STORE rows, and the memory bar miss.
Per the pre-registered terms SPOP enters the F13 escalation rather than failing M1 outright.
The misses point at the shared wire and dispatch ceiling and at process-level memory, not at the set structures this milestone built.

## Follow-ups

- F13 partitioned-draw escalation for SPOP, owed by the prediction's own terms; pair it with a look at the SPOP reply path that loses to redis at native band.
- Per-partition merge fan-out (#599/#601 deferral) before the algebra rows are read again; the Linux flat-merge numbers say the kernel is DRAM-bound flat and the fan-out is the designed lever.
- Skewed-pair algebra workload in aki-bench so the missing half of PRED-F3-M1-SINTER becomes measurable.
- Multi-key spread set workload in aki-bench so the carried K3/K4/K11 numbers can be compared like for like.
- Memory diet round for the process-level bar: GC headroom, slab and vector growth padding, and a used_memory plus MEMORY USAGE surface so aki reports itself.
- Backpressure shaping for O(n) commands queueing on one owner (the sinterstore_10k 512x16 collapse).
- Small-pair algebra fast path for the just-above-listpack band (sinter_256).
- The wb-drift DBSIZE bug, already tracked, re-hit here.
