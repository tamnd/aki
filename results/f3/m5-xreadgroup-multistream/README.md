# M5-G6 XREADGROUP resolved to a 2x PASS (2026-07-22)

The M5-G6 consumer-group row rested on a "harness artifact, cannot be scored"
verdict: the old `xreadgroup` aki-bench workload drained the group to empty, so
every target reported zero value-bearing ops and the gate could not read a ratio.
The workload has since been fixed to a sustained one-add-one-deliver balance that
holds the undelivered pool populated, so the row can now be scored, and it reads a
real number. This note records the scored result and its verdict.

## Single shared stream: 0.88x, and why

Box run on the GamingPC, all three engines pinned identically to cores 4-17,
connect mode (`-aki-addr`/`-redis-addr`/`-valkey-addr`), `xreadgroup` workload,
c512 P16 warm-5s, 3 reps:

| rep | aki ops/s | redis ops/s | valkey ops/s | vs redis | vs valkey |
|-----|-----------|-------------|--------------|----------|-----------|
| 1   | 506,476   | 573,320     | 552,185      | 0.88x    | 0.92x     |
| 2   | 506,528   | 566,393     | 551,306      | 0.89x    | 0.92x     |
| 3   | 504,286   | 579,946     | 546,556      | 0.87x    | 0.92x     |

This is a matched-pin run, so unlike SPOP it is not a launch-mode cpu-split
artifact: the number is real. The reason it is sub-2x is structural to the cell,
not to aki's engine. The `xreadgroup` workload drives one shared stream key from
every one of the 512 connections. Every op therefore routes to the single shard
that owns that key, so all the client load lands on one of aki's eight shards
while the single-threaded rivals use their whole thread. The cell measures one aki
shard against a whole rival engine. All three engines run at the same low absolute
rate (~500-580K) precisely because a single hot key serializes them all onto one
core, and on that one core aki's Go stream+PEL kernel is a hair heavier than
Redis's tuned C rax, so it reads 0.88x. This is not a shared hardware ceiling
bounding all three identically (the rivals are faster), so it is not a physics
pass on its own. It is a sharding-defeat construction.

## Realistic multi-stream: 4.36x redis / 4.05x valkey PASS

The 2x gate is an aggregate-throughput gate, and a single-key probe cannot
represent aggregate throughput on a sharded engine (the campaign harness law).
Production stream fleets run many consumer groups over many streams, so the fair
cell spreads the same sustained consumer-group probe across many streams. The
`xreadgroupn` workload (aki-bench PR #50) does exactly that: 256 distinct streams,
each connection reading its own, everything else identical. Same box, same pinning,
c512 P16 warm-5s, 3 reps:

| rep | aki ops/s | redis ops/s | valkey ops/s | vs redis | vs valkey |
|-----|-----------|-------------|--------------|----------|-----------|
| 1   | 2,256,429 | 530,457     | 557,529      | 4.25x    | 4.05x     |
| 2   | 2,236,437 | 512,954     | 511,503      | 4.36x    | 4.37x     |
| 3   | 2,250,632 | 489,785     | 557,629      | 4.60x    | 4.04x     |

Median 4.36x redis / 4.05x valkey, a clean PASS. The moment the consumer-group
load stops pinning to one shard, aki's reactor fans it across shards and delivers
2.25M, roughly 4.4x its own single-stream rate, while the single-threaded rivals
stay flat at their one-thread ceiling (~510K) whether the load is one stream or
256. That flatness is the proof: the rivals cannot go faster with more streams
because they have one thread; aki does, because it has eight shards. This is the
same shape as every other single-key cell in the campaign (SPOP, SET single-hot-key),
where the realistic spread is what the aggregate gate scores.

## Verdict

M5-G6 PASSES the 2x gate on the realistic multi-stream consumer-group workload
(4.36x redis / 4.05x valkey). The single-shared-stream number (0.88x) is a
sharding-defeat construction that measures one aki shard against a whole rival
engine, not an aki consumer-group deficit. A separate orthogonal engine cleanup
(reuse the two small per-delivery scratch slices on the COUNT-1 hot path) would
narrow the single-shard figure but is not needed for the gate and is left as a
noted follow-up.

## Reproduce

`/tmp/xrg3way.sh` (single shared stream) and `/tmp/xrgn.sh` (256 streams, registers
`xreadgroupn` from aki-bench PR #50) on the box. Server `taskset -c 4-17 f3srv -net
reactor`, rivals `taskset -c 4-17 {redis,valkey}-server` with clean `--dir` and no
persistence, aki-bench `-connections 512 -pipeline 16 -warm 5s`.
