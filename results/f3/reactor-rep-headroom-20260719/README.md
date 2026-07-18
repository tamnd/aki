# Reactor rep-buffer trim: memory bar at the write cell (2026-07-19)

The paired gate run for the reply-headroom change (`labs/f3/m0/27_rep_headroom`,
PR branch `f3-reactor-rep-headroom`). Lab 27 added `Config.RepCap` (the
`-rep-cap` flag) so the hop node's reply-buffer start size is swept independent
of `batchDataCap`, and found the derived `batchDataCap + 64*batchCap` default
carries ~2KB of reply headroom per node that a write-heavy load never uses. This
run confirms end to end that the reactor at `-rep-cap 512` still clears the 2x
throughput gate against both CF16-frozen rivals while shaving the c512 write
cell's peak, the memory half of the simultaneous pass on the reactor driver.

## Protocol

Same-box gate, GamingPC under WSL2, 32 logical cores, aki `514fdc0`
(`f3-reactor-rep-headroom`), 2026-07-19. Server pinned to cores 4-17, two
`redis-benchmark` generators pinned 18-24 and 25-31, each 64B values, `-r
1000000 -n 8000000 -c 256 -P 16 --threads 7`, rates summed. Warm plus three
measured reps, median reported. SET-cell VmHWM read after the SET reps (the
write-heavy peak), final VmHWM after the GET reps.

- reactor: `GOMAXPROCS=14 f3srv -shards 8 -arena-mib 512 -batch-data-cap 1024
  -reply-ring 128 -free-list-cap 8 -rep-cap 512 -net reactor -net-loops 0` (the
  frozen gate flags plus the new rep-cap override).
- rivals: the CF16-frozen redis 8.8.0 (`--io-threads 6`) and valkey 9.1.0
  (`--io-threads 4 --io-threads-do-reads yes`) numbers, unchanged from the
  loop-knee run (`../reactor-loop-knee-20260719`).

## Result

Median summed ops/s, three reps, reactor at `-rep-cap 512`.

| server | SET Mops | GET Mops | VmHWM |
|---|---|---|---|
| reactor (rep-cap 512) | 9.114 | 9.112 | ~190 MiB |
| redis io=6 | 3.365 | 4.260 | 200 MiB |
| valkey io=4 | 2.778 | 3.759 | 132 MiB |

Ratios, reactor over each rival (unchanged from the default-rep-cap run, the
throughput is ceiling-bound and rep-cap does not touch it):

| | SET | GET |
|---|---|---|
| reactor / redis | 2.71x | 2.14x |
| reactor / valkey | 3.28x | 2.42x |

The rep-cap A/B, default vs 512 in one session (VmHWM absolutes carry a few MB of
run-to-run arena-warmup noise; the in-session delta is the signal):

| rep-cap | SET-cell VmHWM | post-GET VmHWM |
|---|---|---|
| 0 (default 3072) | 178,616 kB | 202,560 kB |
| 512 | 166,884 kB | 190,612 kB |
| delta | -11.7 MB | -11.9 MB |

## Verdict

`-rep-cap 512` takes the reactor's c512 64B peak down ~12MB at both the
write-heavy and post-GET peaks with SET and GET throughput unchanged at the
dual-generator ceiling, so it clears 2x against both rivals exactly as the
default-rep-cap reactor did and lands the peak a further ~12MB under redis's 200
MiB. The GET pass grows each node's reply buffer only to the ~1KB a `-P 16` run
of 64B replies accumulates, well under the 3072 default, so the saving survives
a read pass and there is no grow-thrash (throughput is bit-flat across the whole
`{0,2048,1024,512}` sweep).

This does not reach valkey's 132 MiB on the 1M-key 64B string workload. That gap
is structural: aki's per-connection sharded-MPSC hop fabric has no equivalent in
valkey's single event loop, so no per-node buffer cap closes it. The rep trim is
the last cheap per-node lever on the string write cell; the memory win aki
carries is at data volume (the arena's packed records and the LTM cold rows ride
well under both rivals), and on this tiny-collection string cell the bar is to
stay under redis, which the reactor now does with room. This change delivers the
memory half of the simultaneous pass on the driver that already carries the
throughput half.
