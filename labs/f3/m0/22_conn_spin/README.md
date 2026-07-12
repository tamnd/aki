# Lab 22: connection-writer spin vs park at pipe=1

Spec 2064/f3, M0 gate follow-up. Owns the `connSpinHighWater` constant in
`engine/f3/shard/tuning.go`.

## Question

The M0 point-op gate cells at pipe=1 (GET and SET) lose to redis 8.8.0 and
valkey 9.1.0 at high connection counts. The 512-conn CPU profile put about 40
percent of aki's CPU in the connection writer's completion spin: `Conn.idleOnce`
polling the outbound queue before it parks (19.6% cum in `idleOnce`, 11.3% in
`mpsc.ready`, plus the atomic loads), driven by `spinWindow = 4us`.

Lab 11 already froze the shard *worker's* spin at 0 for the saturated-cores
reason: on one box the server and its clients share cores, so a microsecond a
worker burns spinning is stolen from the net goroutines that feed it. The same
logic was never applied to the connection *writer*, which kept the 4us window
lab 3 sized for a low-fan-out regime. At 512 connections that spin is a core
stolen from the eight shard workers draining tiny replies.

Does parking the writer immediately at high fan-out free those cores for real
throughput, and where is the crossover below which the spin still pays?

## Method

Two arms, set through `shard.SetConnSpinHighWater`:

- **spin** (`alwaysSpin = 1<<30`): high-water past any live count, the writer
  never collapses its spin. The fixed-4us control.
- **park** (`alwaysPark = 1`): smallest high-water, any live connection parks
  the writer at once.

Sweep the two arms over connections {32, 128, 256, 512} at pipe=1 GET, 8 shards,
nKeys = 1<<18 at 64B, 2s per cell, one f3srv in process on loopback. Run with
`go run ./labs/f3/m0/22_conn_spin`.

CPU/op is process rusage (user+system) over the cell; the loopback clients live
in the same process, so it is server+client and only same-row comparison means
anything.

## Numbers (GamingPC box, in process, all 32 cores)

`go run ./labs/f3/m0/22_conn_spin`, GET 64B, 262144 keys, pipe=1, 8 shards, 2s
per cell. The server and its loopback clients share all 32 cores here (no gate
split, GOMAXPROCS 32), so the crossover sits higher than the dedicated-core gate
run below; the shape is what the lab isolates.

| conns | spin ops/s | spin CPU/op | park ops/s | park CPU/op | park/spin |
|---|---|---|---|---|---|
| 32 | 444065 | 31.5µs | 308094 | 22.6µs | 0.69 |
| 128 | 1148531 | 25.8µs | 685181 | 16.7µs | 0.60 |
| 256 | 1197758 | 26.0µs | 1004921 | 16.0µs | 0.84 |
| 512 | 1183260 | 26.3µs | 1324506 | 15.6µs | 1.12 |

Two things fall out. Park crosses spin between 256 and 512 connections once the
box saturates, and park costs about 40 percent less CPU per op at every count
(15.6µs vs 26.3µs at 512c): the spin burns cores whether or not it catches a
reply. Below the crossover spin still wins throughput because the freed cores
have nothing else to run and the in-process clients would rather the reply came
back without a park/wake. The crossover is later here than on the gate below
because all 32 cores are shared; with GOMAXPROCS 14 and clients on their own
cores the knee drops to 80-100, which is why the default high-water scales with
GOMAXPROCS rather than sitting at a fixed count.

## Gate verify: the adaptive default fires correctly

The in-process lab isolates the writer-spin term; the gate proves the shipped
default picks the right arm under the real driver. Median of 3, server pinned to
cores 4-17 (14 cores, GOMAXPROCS 14), client to 18-31, shards 8, against redis
8.8.0 and valkey 9.1.0. Ratio is `min(aki/redis, aki/valkey)`. Three arms:
`default` (no flag, adaptive high-water GOMAXPROCS*6 = 84), `park`
(`-conn-spin-highwater 1`, always park fast), `spin` (`-conn-spin-highwater
100000`, never collapse).

| wl | conns | default | park | spin |
|---|---|---|---|---|
| SET | 50 | 0.49 | 0.48 | 0.49 |
| SET | 512 | 1.77 | 1.75 | 1.24 |
| GET | 50 | 0.47 | 0.45 | 0.47 |
| GET | 512 | 1.71 | 1.70 | 1.16 |

At 512c the default tracks the park arm (SET 1.77 vs 1.75, GET 1.71 vs 1.70) and
beats the fixed-spin arm by about half a turn (SET +0.53x, GET +0.55x): the
collapse fires. At 50c the default tracks the spin arm (SET 0.49, GET 0.47) with
no regression, and it dodges the small penalty the park arm pays there (SET
0.48, GET 0.45) because park-fast trades a wake for the reply while cores sit
idle. The adaptive switch takes the best of both ends.

The 50-conn cells stay below 1x (valkey-bound, a separate low-conn ceiling this
lever does not touch). Range ops (large ~1KB replies) move only +1-7% under
park-fast: those cells are bytes-bound on the `write()` syscall, not on the
spin, so the range deficit is a separate structural ceiling (H1 territory).

## Verdict

Collapse the connection-writer spin to 0 once the live-connection count reaches a
high-water, keep the 4us spin below it. The high-water is `GOMAXPROCS*6`
(`defaultConnSpinHighWater`), so it tracks the core count rather than a fixed
connection number and lands at 84 on the 14-core gate box, inside the measured
80-100 knee. The switch is a plain load of the runtime's live-connection counter
(`Runtime.live`, bumped by every driver's `register`/`unregister` through
`ConnOpened`/`ConnClosed`), read per idle turn in `Conn.spinBudget`.

Frozen into `engine/f3/shard/tuning.go` as `connSpinHighWater`.
