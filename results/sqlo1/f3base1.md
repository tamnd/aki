# S0 baseline: Redis 8.8, Valkey 9.1, and f3 on the core suite

Milestone S0 (tamnd/aki#710), exit gate; spec 2064/sqlo1 doc 13.
Harness: tamnd/aki-bench at 632f6b9; aki checkout 2a207b3 serving the f3 driver.
Box: the gate box (GamingPC, WSL2, Linux 6.18, 32 cpus), 2026-07-15, rivals pinned at redis-server 8.8.0 and valkey-server 9.1.0.
Grid: cap and data arms x read mixes 10/50/90 x values 16/128/512/4096 B x uniform/zipf x scales 1/4/16, 2 reps, 64 conns at pipeline 16, alternating run order, 10 s measured after 5 s warm.
Raw data beside this note under results/sqlo1/f3base1/: one directory per runner invocation with results.csv, manifest.txt, per-cell JSON, and an empty failures.txt each, plus the run scripts and the resume log.
merged.csv is the deduplicated union of all seven, 864 rows, the full 144-cell grid with no holes; where cap-r10 and cap-r10b overlap (the first r10 run died and the resume re-ran its tail), the resume row wins.

## The table

Geomean over cells of value-bearing ops/s ratio (f3 over rival) and peak VmHWM ratio:

| slice | cells | ops vs redis | ops vs valkey | peak mem vs redis | peak mem vs valkey |
|---|---|---|---|---|---|
| all | 144 | 0.49x | 0.46x | 1.43x | 1.28x |
| cap r10 | 24 | 0.30x | 0.29x | 2.51x | 2.16x |
| cap r50 | 24 | 0.33x | 0.31x | 2.55x | 2.19x |
| cap r90 | 24 | 0.48x | 0.43x | 2.57x | 2.24x |
| data r10 | 24 | 0.66x | 0.63x | 0.74x | 0.70x |
| data r50 | 24 | 0.66x | 0.61x | 0.81x | 0.73x |
| data r90 | 24 | 0.67x | 0.61x | 0.86x | 0.83x |

By value size and by client scale, all arms pooled:

| slice | ops vs redis | ops vs valkey | slice | ops vs redis | ops vs valkey |
|---|---|---|---|---|---|
| v16 | 0.57x | 0.52x | x1 | 0.75x | 0.69x |
| v128 | 0.38x | 0.35x | x4 | 0.59x | 0.55x |
| v512 | 0.32x | 0.32x | x16 | 0.27x | 0.25x |
| v4096 | 0.83x | 0.75x | | | |

## Reading it

This is the bar table the sqlo1 milestones measure against, not a verdict on anything sqlo1 has built.
The gate is 2.0x, and f3 on this box sits at 0.49x overall, consistent with the known Linux baseline where f3 runs at roughly 0.4x of its macOS shape.
The two structural reads that matter for sqlo1:

- The data arm is the like-for-like arm and there f3 holds 0.61x to 0.67x of the rivals while peaking at 0.70x to 0.86x of their memory, so the memory story already points the right way at equal data.
- The cap arm ratios are not like-for-like on retained data: at 128 MiB the rivals evict hard (redis keeps 8 to 11 percent of the keyspace at r10 v16 x16 while f3 keeps everything), so the rival ops there are served over a sliver of the data. The coverage_fraction column is the corrective; any cap-arm comparison must be read with it.

The clear weak spots, in case later milestones want to know where the floor is: concurrency scaling (x16 drops f3 to 0.27x while x1 is 0.75x) and the mid sizes (v128 and v512 are the worst size rows in both arms).
The single worst cells are cap r10/r50 at v128 and v512 under x16, where f3 all but stalls at 0.01x to 0.02x of valkey.

One f3 finding came out of the data arm: every v4096 cell has f3 coverage at 0.474 while both rivals hold 1.0, across all mixes, dists, and scales.
The data arm is supposed to be the everyone-holds-everything arm, so half the 4 KiB keyspace going unretrievable is an f3 retention bug or a config ceiling (f3srv ran shards 4 arena_mib 256), and it flatters f3's v4096 throughput row.
Filed as context for the f3 side; sqlo1 only needs to know the v4096 rival numbers are the trustworthy ones.

## Rival sanity

The harness reproduces the rivals' published headline shape.
Peak measured: redis 2.67M ops/s and valkey 3.15M ops/s, both at data r90 v16 uniform x1 under 64 conns at pipeline 16, with valkey ahead of redis on multi-threaded IO as its own benchmarks claim.
Those magnitudes match the vendors' pipelined small-value numbers for this class of box, so rival misconfiguration is ruled out as an explanation for any future sqlo1 win.

## Provenance

Everything needed to re-run is in the raw directories: manifest.txt pins binaries, commits, flags, cap, and timeouts per invocation; the per-cell JSON carries the full sample; run-f3base.sh and run-f3base-resume.sh are the exact scripts; f3base-resume.log is the watcher log of the resumed tail.
All seven failures.txt files are empty.
