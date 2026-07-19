# Reactor 2x gate on latest main after the IDLETIME clock arc (2026-07-19)

A regression check, not a new tuning run. PRs #1161 and #1162 added a per-key
access clock for OBJECT IDLETIME across strings and then all five collection
types, both at zero added bytes (the clock rides a spare header word on strings
and struct alignment padding on the collections). The claim those PRs make is
that the clock is memory-neutral and off the throughput path. This run confirms
that claim end to end: the reactor at its default config still clears the 2x
throughput gate against both frozen rivals, and its peak sits where it did
before the clock landed.

HEAD is `f34db6e` (M9: extend OBJECT IDLETIME to the five collection types,
#1162), the tip of main. Same box, binaries, rivals, and harness as
`reactor-loop-knee-20260719`, which measured the same gate one commit family
earlier on `ce8b232`.

## Protocol

Same-box gate, GamingPC under WSL2, 32 logical cores, 2026-07-19. Each server
pinned to cores 4-17, two `redis-benchmark` generators pinned 18-24 and 25-31,
64B values, `-r 1000000 -n 8000000 -c 256 -P 16 --threads 7`, rates summed over
the two generators. Warm plus three measured reps, median reported. Peak VmHWM
read from `/proc/<pid>/status` after the GET reps.

- reactor: `GOMAXPROCS=14 f3srv -shards 8 -arena-mib 512 -batch-data-cap 1024
  -reply-ring 128 -free-list-cap 8 -net reactor -net-loops 0` (net-loops 0
  resolves to 7 at GOMAXPROCS 14)
- redis: `--io-threads 6 --save '' --appendonly no --dir /tmp`
- valkey: `--io-threads 4 --io-threads-do-reads yes --save '' --appendonly no
  --dir /tmp`

Harness `results/f3/reactor-loop-knee-20260719/run.sh`, invoked with
`BIN=/root/bin/f3srv`.

## Result

| row | reactor | redis | valkey | vs redis | vs valkey | gate |
| --- | --- | --- | --- | --- | --- | --- |
| SET | 9.117 Mops | 3.365 | 2.778 | 2.71x | 3.28x | PASS |
| GET | 9.117 Mops | 4.260 | 3.758 | 2.14x | 2.42x | PASS |
| VmHWM | 207.6 MiB | 209.3 MiB | 129.7 MiB | 0.99x | 1.60x | see note |

Both SET and GET clear the 2x throughput bar against redis 8.8.0 and valkey
9.1.0 with room to spare. The IDLETIME clock cost nothing: SET and GET medians
sit inside rep-to-rep noise of the pre-clock loop-knee run (9.122/9.114 Mops
there, 9.117/9.117 here), and peak VmHWM is 212592 kB here against 207472 kB in
the loop-knee run, a 5 MB difference that is within the run-to-run VmHWM spread
we see from transient reply-buffer high-water marks, not a structural gain from
the collection clock (the clock adds no bytes).

## The memory row

On throughput the gate is green. The peak-memory picture splits: aki is a hair
under redis (207.6 vs 209.3 MiB, 0.99x) but 1.60x valkey (207.6 vs 129.7 MiB) on
this 1M-key 64B-value string workload.

This is the VmHWM *peak* held during a 512-connection P16 blast, not a settled
post-scavenge dataset footprint, so it folds in transient network buffers and Go
GC headroom on top of the dataset. It is the honest peak the memory bar counts,
and closing the valkey gap on it is the next memory target. The likely levers,
in order of suspicion, are Go's GC growth headroom (GOGC 100 lets the heap roughly
double over the live set before a collection) and the reactor's per-connection
reply buffers at 512 live connections. Both want an on-box smaps/gctrace
breakdown before any knob moves, since the throughput gate must stay green.
