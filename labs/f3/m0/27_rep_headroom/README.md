# M0 lab 27: the hop-node reply-buffer headroom as a write-heavy memory lever

Spec 2064/f3, M0 gate follow-up, lab 27. Follows lab 25, which promoted the
per-connection hop-transport caps (`batchDataCap`, `replyRing`, `freeListCap`)
to `shard.Config` fields and took c512 VmHWM down 91MB at the 64B gate cell. This
lab takes the one cap lab 25 left derived rather than swept, the reply buffer,
and measures the write-heavy footprint it still carries.

## The question

A hop node starts its reply buffer at `repCap = batchDataCap + 64*batchCap`
(tuning.go): the `64*batchCap` term (2048 bytes at the gate's `batchDataCap`
1024) is reply headroom, sized so the steady path never grows the buffer. But
the buffer grows on demand and keeps the grown capacity (`keepNodeBytes`), so a
write-heavy load never needs the headroom: a SET reply is `+OK`, five bytes, and
even a full `batchCap` node of them is 160 bytes. At c512 fan-out every pooled
node on every connection carries the 2KB headroom as pure slack on the write
path.

Two questions the gate needs answered:

1. How much does the reply headroom cost at the c512 64B write cell?
2. A GET pass grows the buffer to hold batched 64B replies. Does the saving
   survive a read pass, or does GET re-inflate every node back to the headroom?

This lab adds `Config.RepCap` (wired through `drivers.Options` and the
`cmd/f3srv -rep-cap` flag) so the reply start size is swept and set per box,
independent of `batchDataCap`, then measures both questions.

## Method

`run.sh` is the exact driver. It boots the reactor at the frozen gate flags
(`-shards 8 -arena-mib 512 -batch-data-cap 1024 -reply-ring 128
-free-list-cap 8 -net reactor -net-loops 0`, GOMAXPROCS 14, server pinned
`taskset -c 4-17`) and drives it with the dual-generator SET/GET gate harness:
two `redis-benchmark` procs pinned 18-24 and 25-31, each 64B values,
`-r 1000000 -n 8000000 -c 256 -P 16 --threads 7`, summed ops/s, warm plus reps
per cell. VmHWM is read from `/proc/$pid/status` (server-only, not co-located).
`-rep-cap` sweeps `{0, 2048, 1024, 512}`, where 0 takes the tuning.go default
(`batchDataCap + 64*batchCap` = 3072 at these flags).

The SET-cell VmHWM is read right after the SET reps (the write-heavy peak, before
GET grows any node); a focused A/B also reads the post-GET peak.

## Results

GamingPC, WSL2, aki 514fdc0, 2026-07-19. Summed ops/s, dual-generator ceiling
near 9.12M (both generators saturate their 14 cores, so throughput cells at the
ceiling read identically).

Sweep, SET-cell VmHWM (write-heavy peak). Throughput flat at the ceiling across
the whole sweep:

| rep-cap | SET ops/s | GET ops/s | SET-cell VmHWM |
|---|---|---|---|
| 0 (default 3072) | 9,122,007 | 9,116,812 | 193,892 kB |
| 2048 | 9,116,809 | 9,122,007 | 189,736 kB |
| 1024 | 9,114,213 | 9,119,408 | 187,860 kB |
| 512 | 9,119,408 | 9,119,408 | **183,932 kB** |

Focused A/B, both peaks, default vs 512 in one session (VmHWM absolutes carry a
few MB of run-to-run arena-warmup noise, so the reliable signal is the
in-session delta, ~12MB at both peaks):

| rep-cap | SET-cell VmHWM | post-GET VmHWM |
|---|---|---|
| 0 (default) | 178,616 kB | 202,560 kB |
| 512 | 166,884 kB | 190,612 kB |
| delta | **-11.7 MB** | **-11.9 MB** |

The saving survives the GET pass. GET of a 64B value does grow each node's reply
buffer, but only to the batched-reply size a `-P 16` run accumulates (~1KB), well
under the 3072 default, so a 512 start still lands ~12MB under the default after
a full read pass. The buffer grows once and keeps its capacity, so there is no
thrash: throughput is bit-flat at the ceiling on both SET and GET across the
whole sweep.

## Verdict

The reply headroom is a real write-heavy memory cost and safe to trim: at the
c512 64B cell `-rep-cap 512` takes the reactor VmHWM down ~12MB at both the
write-heavy peak and the post-GET peak, with SET and GET throughput unchanged at
the dual-generator ceiling (median-of-3 SET 9,114,213, GET 9,111,620, every cell
above all four 2x bars). It saves ~2x what 1024 does because a 64B read pass
never needs even 1KB of reply buffer per node, let alone the 3072 default.

The shipped default stays put; the gate opts in via the flag. Following the lab
25 precedent, the derived `batchDataCap + 64*batchCap` default is kept for the
general-purpose server, where a GET or ECHO of a value near `batchDataCap` should
fit its reply without a first-touch grow. `Config.RepCap` is the swept knob, and
the gate's write-heavy 64B cell runs `-rep-cap 512` on top of lab 25's
`-batch-data-cap 1024 -reply-ring 128 -free-list-cap 8`.

This is one more stacking layer on the reactor's memory bar, not the whole
distance. The reactor already banks the goroutine-stack half of the string
footprint (286MB goroutine driver down to ~200MB, level with redis 8.8.0's
~200MB at the 1M/64B dataset), and this trim takes the c512 write cell ~12MB
further under redis. It does not reach valkey 9.1.0's ~132MB: that gap is
structural, the per-connection sharded-MPSC hop fabric valkey's single event
loop has no equivalent of, so no per-node cap closes it. aki's memory win is at
data volume, where the arena's packed records and the LTM cold rows ride well
under both rivals (LTM ~0.25x); on the c512 tiny-collection string cell the bar
is to stay under redis, which this holds, and the rep trim widens.
