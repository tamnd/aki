# M0 driver-sweep gate, current main (2026-07-18)

Fresh M0 headline gate on `main` at `7a8e866`, run on the GamingPC box under
WSL2 (32 logical cores, 56 GiB, Go 1.26.0). The point of this run is two things
at once: refresh the M0 2x throughput evidence on the current surface after all
the M7 through M11 work landed, and settle the open question of whether the
io_uring driver lifts the point-op path over the default goroutine driver.

aki is swept across its three network drivers. The rivals are the CF16-frozen
builds: redis 8.8.0 with `--io-threads 6`, valkey 9.1.0 with `--io-threads 4
--io-threads-do-reads yes`. Both link jemalloc 5.3.0.

## Protocol

Same harness as the 2026-07-14 M0 gate (`run.sh` here). Server pinned to cores
4-17, two redis-benchmark generators pinned to 18-24 and 25-31 with their rates
summed, so the single-generator random-key cap does not bind. P16, 512 total
connections (256 per generator), 64-byte values, 1M-key uniform space, 10M ops
per generator. Warm plus three reps, median of the summed throughput, worst p99
across reps. GET cells preload 2.5M x 2 keys first. aki launches with
`GOMAXPROCS=14 -shards 8 -arena-mib 512`, the driver set with `-net <driver>
-net-loops 4`. All three drivers engaged with no fallback notice in the server
log.

## Throughput

Median summed ops/s, and the ratio of the best aki driver (goroutine) over each
rival.

| target | SET ops/s | GET ops/s |
|---|---|---|
| aki-goroutine | 11.399M | 11.399M |
| aki-reactor | 6.145M | 6.144M |
| aki-uring | 6.657M | 6.656M |
| redis io=6 | 3.330M | 4.205M |
| valkey io=4 | 2.755M | 3.804M |

| ratio | SET | GET |
|---|---|---|
| aki-goroutine / redis | 3.42x | 2.71x |
| aki-goroutine / valkey | 4.14x | 3.00x |

**2x throughput objective: achieved.** The default goroutine driver clears 2x on
both workloads against both rivals, with the tightest margin at GET vs redis
(2.71x). This holds the 2026-07-14 result on the current surface: SET is
identical to the byte, GET is within noise (the earlier run read 2.85x vs a
slightly slower redis GET number on that day; both are well over the floor).

## The io_uring question, settled

The standing hypothesis from the M0 perf round was that an event-loop driver, and
io_uring in particular, was the lever for the point-op dispatch ceiling. This run
falsifies that for throughput. The goroutine driver runs at 11.4M ops/s; uring
runs at 6.66M and reactor at 6.14M, so both event-loop drivers regress about 1.7x
against goroutine at c512/P16 on this box. io_uring does beat reactor by 8%, so
the uring path is real and slightly ahead of the epoll path, but neither
approaches the goroutine driver.

The reading is that at this connection count and pipeline depth the Go runtime's
own netpoller plus goroutine-per-connection scheduling is already at the wire
ceiling, and moving to a hand-rolled event loop with a fixed loop count trades
that away. The collection point-op misses in the M1, M2, and M3 gates (SISMEMBER,
ZSCORE, LLEN at 1.3-1.5x) are therefore not a net-driver ceiling. GET rides the
same wire and clears 2.71x, so those misses are per-op work in the type kernels
and the reply encode, not the driver. The io_uring lever is closed for
throughput; the goroutine driver stays the default.

## Memory

Peak VmHWM and the live-data ledger, both workloads. VmHWM is the process high
water mark; `used_memory` is each server's own accounting of live data.

| target | SET VmHWM | GET VmHWM | used_memory (SET, 1M keys) |
|---|---|---|---|
| aki-goroutine | 285.9 MB | 304.6 MB | 113.5 MB |
| aki-reactor | 186.2 MB | 210.0 MB | 113.5 MB |
| aki-uring | 357.8 MB | 380.6 MB | 113.5 MB |
| redis io=6 | 177.6 MB | 155.2 MB | 135.9 MB |
| valkey io=4 | 132.8 MB | 134.0 MB | 115.1 MB |

Two facts pull in opposite directions. On the live-data ledger aki is the
leanest of the three: 113.5 MB against valkey's 115.1 and redis's 135.9 for the
same 1M 64-byte keys, so the data model itself carries the set more densely than
either rival. On the process peak aki is the fattest: the goroutine driver holds
285.9 MB on the fresh SET, 2.15x valkey's 132.8 MB, and the bar wants aki under
the best rival.

The gap between 113.5 MB of live data and 285.9 MB of process peak is fixed
overhead: per-connection hop buffers and free lists, goroutine stacks for 512
connections, and GC headroom over the arena. The driver choice moves it a lot.
The reactor driver holds the same 1M keys at 186.2 MB, 1.40x valkey, because its
event loops do not carry a goroutine stack per connection; uring is worst at
357.8 MB. So there is a real throughput-for-memory trade across the drivers:
goroutine is fastest and fattest, reactor is leanest and slowest, uring is worst
on both and not worth running.

**Memory bar: not met on any driver.** The live-data pitch holds (aki is denser
than both rivals on the ledger), but the process peak fails the strict bar on
every driver, best case 1.40x on reactor. This is the true open blocker for a
green M0 gate, and it is a fixed-overhead problem, not a per-entry one. The lever
is the per-connection buffering and GC headroom, the same fixed-cost root the M2
and M3 gates isolated behind their peak-VmHWM multiples.

## Verdict

The M0 2x throughput objective is achieved on the current surface against both
CF16-frozen rivals, tightest at 2.71x. The io_uring driver is measured and does
not lift throughput, so that lever is closed and the point-op collection misses
are kernel work, not the wire. The memory bar is the one open item: aki's
live-data ledger is leaner than both rivals, but its process peak is 2.15x valkey
on the fast driver and 1.40x on the lean one, a fixed-overhead cost that no
driver choice erases.
