# M0 lab 26: the reactor loop-count knee, re-swept

Lab 19 (`labs/f3/m0/19_loop_count`) froze the reactor's default loop count at the
2/5 network share of the doc 03 section 2.2 core split, `GOMAXPROCS*2/5`, from a
sweep on the gate box's 8-cpu server mask that put the knee at 3 loops. That
sweep ran on the pre-M10 surface (commit c76d6c0, 2026-07-11), before the M10
pull-forward landed the batched owner-to-loop wakes (one eventfd write per
touched loop per worker drain pass, not one per dirty connection) and the
per-loop buffer leasing. Both cut the per-loop cost of an extra loop, so the
oversubscription penalty a loop past the knee used to pay is smaller now.

This lab re-runs the sweep on the current surface, at the real gate config
(GOMAXPROCS 14, 8 shards) and at an 8-cpu control, to check whether the knee is
still where lab 19 left it. It is not: it moves up to half the cores at both
counts, and the 2/5 default now undershoots enough to cost the reactor the 2x
GET gate.

## Protocol

Dual-generator SET/GET, the collection/M0 gate harness: two `redis-benchmark`
procs, each 64B values, `-r 1000000 -n 8000000 -c 256 -P 16 --threads 7`, summed
ops/s, warm plus two reps per cell. The reactor server is pinned to the server
mask at `-shards S -net reactor -net-loops L`, the generators to two disjoint
7-core masks off the server. `-net-loops` sweeps around the candidate knee.

- 14-cpu gate config: server `taskset -c 4-17`, generators 18-24 and 25-31,
  8 shards (the frozen gate flags, `shard.DefaultShards()` at GOMAXPROCS 14).
- 8-cpu control: server `taskset -c 4-11`, generators 12-18 and 19-25, 4 shards
  (`DefaultShards()` at GOMAXPROCS 8), the mask lab 19 measured.

`run.sh` is the exact driver. It is loop-count-internal (one knob, one harness,
one box session), so no rival ratios are computed here; the slice's `results/`
run pairs the winning loop count against the CF16-frozen rivals.

## Results

GamingPC, WSL2, aki 8949dea, 2026-07-19. Summed ops/s, best of the two measured
reps (the harness ceilings near 9.12M as the two generators saturate their 14
cores, so cells at the ceiling read identically).

14-cpu gate config, 8 shards:

| loops | SET Mops | GET Mops | VmHWM |
|---|---|---|---|
| 4 (old gate hardcode) | 6.39 | 6.39 | ~195 MiB |
| 5 (`GOMAXPROCS*2/5`) | 7.98 | 7.10 | ~197 MiB |
| 6 | 9.12 | 7.98 | ~194 MiB |
| 7 (`GOMAXPROCS/2`) | 9.12 | 9.12 | ~199 MiB |

8-cpu control, 4 shards:

| loops | SET Mops | GET Mops |
|---|---|---|
| 3 (lab 19 knee, `GOMAXPROCS*2/5`) | 4.79 | 4.79 |
| 4 (`GOMAXPROCS/2`) | 6.84 | 6.84 |
| 5 | 6.84 | 6.84 |

Both sweeps put the knee at `GOMAXPROCS/2`: 4 loops at 8 cpu (+43% over the lab
19 knee of 3, and 5 loops does not regress off it), 7 loops at 14 cpu. The 2/5
default lands one to two loops short of the knee at both counts. At 14 cpu it
picks 5 loops, which holds GET to 7.1 Mops; at loops 7 GET reaches 9.12 Mops. The
512-conn peak barely moves across the sweep (194 to 199 MiB), so the extra loops
buy throughput without spending the reactor's lean-memory advantage.

## Verdict

The lab 19 knee was a pre-M10 reading. On the current surface the reactor's
loop-count knee is `GOMAXPROCS/2` at both 8 and 14 cpu, so `defaultNetLoops`
moves from `GOMAXPROCS*2/5` to `GOMAXPROCS/2` (`f3srv/drivers/reactor_linux.go`).
Half the cores oversubscribes the 3/5-shards, 2/5-loops split by one thread, and
the current surface absorbs it because the P16 point-op gate is loop-bound, not
shard-bound: the owners park while the loops saturate, so a loop outearns the
shard core it borrows. The switch steers only the opt-in event-loop drivers
(reactor and uring, which share `defaultNetLoops`); the default goroutine driver
ignores the knob. A shard-bound workload that wants the core back dials the split
with `-net-loops`.

The throughput this unlocks is the 2x gate: at 14 cpu, 8 shards, 7 loops the
reactor runs SET and GET at 9.12 Mops each, which the slice's paired rival run
(redis 8.8.0 io=6, valkey 9.1.0 io=4, same harness) turns into 2.7x/2.2x SET/GET
over redis and 3.3x/2.4x over valkey, both ops over both rivals, at a 512-conn
peak of about 195 MiB against the goroutine driver's 286 MiB. The re-sweep is
what lets the lean driver clear the throughput gate the fat one already cleared.
