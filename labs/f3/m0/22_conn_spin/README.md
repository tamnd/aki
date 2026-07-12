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

## Numbers (GamingPC gate box, GOMAXPROCS 14)

TODO fill from `go run ./labs/f3/m0/22_conn_spin` on the box.

| conns | spin ops/s | spin CPU/op | park ops/s | park CPU/op | park/spin |
|---|---|---|---|---|---|

## Gate-box A/B this reproduces

The in-process lab isolates the writer-spin term; the decisive numbers are the
median-of-3 gate runs on the GamingPC box, server pinned to cores 4-17 (14
cores, GOMAXPROCS 14), client to 18-31, shards 8, against redis 8.8.0 and
valkey 9.1.0.

SET pipe=1, ratio = aki/redis, 4us vs park-fast:

| conns | 4us | park-fast |
|---|---|---|
| 64 | 0.52 | 0.51 |
| 128 | 0.75 | 0.82 |
| 192 | 0.87 | 1.04 |
| 256 | 1.01 | 1.27 |
| 384 | 0.87 | 1.62 |
| 512 | 1.28 | 1.76 |

At 512c park-fast lifts SET from 1.19x to 1.75x and GET from 1.25x to 1.72x vs
redis (1.87-1.89x vs valkey). The 50-conn cells are unchanged (0.45-0.49x,
valkey-bound, a separate low-conn ceiling). The crossover knee is near 80 to 100
connections, about six per core on the 14-core box, and below it the spin still
pays because cores sit idle.

Range ops (large ~1KB replies) move only +1-7% under park-fast: those cells are
bytes-bound on the `write()` syscall, not on the spin, so the range deficit is a
separate structural ceiling (H1 territory), not this lever.

## Verdict

Collapse the connection-writer spin to 0 once the live-connection count reaches a
high-water, keep the 4us spin below it. The high-water is `GOMAXPROCS*6`
(`defaultConnSpinHighWater`), so it tracks the core count rather than a fixed
connection number and lands at 84 on the 14-core gate box, inside the measured
80-100 knee. The switch is a plain load of the runtime's live-connection counter
(`Runtime.live`, bumped by every driver's `register`/`unregister` through
`ConnOpened`/`ConnClosed`), read per idle turn in `Conn.spinBudget`.

Frozen into `engine/f3/shard/tuning.go` as `connSpinHighWater`.
