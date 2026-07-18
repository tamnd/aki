# Reactor 2x gate at the re-swept loop default (2026-07-19)

The paired rival run for the loop-knee change (`labs/f3/m0/26_loop_knee`, PR
branch `f3-reactor-loop-knee`). Lab 26 re-swept the reactor's loop-count knee on
the current surface and moved `defaultNetLoops` from `GOMAXPROCS*2/5` to
`GOMAXPROCS/2`. This run confirms end to end that the reactor at its **default**
config now clears the 2x throughput gate against both CF16-frozen rivals, and
records the peak it holds while doing so.

The question the run answers: does the reactor, launched with no loop-count
override (`-net-loops 0`, which now resolves to 7 at GOMAXPROCS 14), clear 2x on
SET and GET against redis 8.8.0 and valkey 9.1.0? Before the change the default
resolved to 5 loops and held GET to 7.1 Mops, 1.67x redis, short of the gate.

## Protocol

Same-box gate, GamingPC under WSL2, 32 logical cores, aki `ce8b232`
(`f3-reactor-loop-knee`), 2026-07-19. Each server pinned to cores 4-17, two
`redis-benchmark` generators pinned 18-24 and 25-31, each 64B values, `-r
1000000 -n 8000000 -c 256 -P 16 --threads 7`, rates summed. Warm plus three
measured reps, median reported. Peak VmHWM read from `/proc/<pid>/status` after
the GET reps.

- reactor: `GOMAXPROCS=14 f3srv -shards 8 -arena-mib 512 -batch-data-cap 1024
  -reply-ring 128 -free-list-cap 8 -net reactor -net-loops 0` (the frozen gate
  flags, loop count left to the default).
- redis: `redis-server --io-threads 6 --save '' --appendonly no`.
- valkey: `valkey-server --io-threads 4 --io-threads-do-reads yes --save ''
  --appendonly no`.

## Result

Median summed ops/s, three reps.

| server | SET Mops | GET Mops | VmHWM |
|---|---|---|---|
| reactor (default loops=7) | 9.122 | 9.114 | 207 MiB |
| redis io=6 | 3.365 | 4.260 | 200 MiB |
| valkey io=4 | 2.778 | 3.759 | 132 MiB |

Ratios, reactor over each rival:

| | SET | GET |
|---|---|---|
| reactor / redis | 2.71x | 2.14x |
| reactor / valkey | 3.28x | 2.42x |

The reps are tight (SET within 0.03%, GET within 0.03%); the summed figure sits
on the two-generator harness ceiling near 9.12 Mops, so the true reactor
throughput is at least this, and the ratios are lower bounds (the rivals are the
slower servers and are not generator-bound).

## Verdict

The reactor at its default config clears 2x on both SET and GET against both
rivals: the loop-count re-sweep is what lets the lean event-loop driver pass the
throughput gate the fat goroutine driver already passed. The 512-conn peak is 207
MiB, well under the goroutine driver's 286 MiB and level with redis's 200 MiB,
though still above valkey's 132 MiB on this 1M-key 64B string workload, so the
memory bar against valkey stays a separate open front (the collection footprint
and the string RSS both feed it) and is not what this change addresses. What this
change delivers is the throughput half of the simultaneous pass, on the driver
that carries the memory half.
