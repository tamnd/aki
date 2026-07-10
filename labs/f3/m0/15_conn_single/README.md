# Lab 15: single-goroutine connection loop vs the reader/writer pair

Part of issue #542, M10 pull-forward slice 2 per spec 2064/f3/08 section 4.1.

## Question

Doc 08 section 4.1 prescribes one goroutine per connection: read, parse, dispatch, wait on the batch's completions, drain, flush, repeat.
The M0 driver shipped a reader/writer pair instead, which costs one goroutine per connection plus a worker-to-writer channel wake and a writer park on every request-reply round.
Lab 14 priced the wake band at about 1.9 wakes/op at P1 c50 and about 3 ns/op at P16 on this box.
Prediction filed before the run: P16 within noise, P1 latency and throughput up a little (one goroutine handoff removed per round), wakes/op at P1 dropping from about 1.9 toward 1.0, writes/op unchanged at one per round.

## Setup

Apple M4 (darwin/arm64), in-process f3srv over loopback, 4 shards, 5s per cell.
Sweep: shape {single, pair} x {P1 c50, P16 c128} x {GET 64B (1M keys), GET 1KiB (256K keys)}.
Per cell: ops/s, process CPU per op (server plus client, the lab 11 caveat), akinet counters per op, and at P1 the client-observed RTT p50/p99.
The wire cross-check runs the built f3srv binary against redis-benchmark in a separate process, three alternating reps per cell, plus a 512-idle-connection RSS column.

## Results, in process

| cell | shape | ops/s | CPU/op | writes/op | reads/op | wakes/op (wk+conn) | parks/op (wk+conn) | p50 | p99 |
|---|---|---|---|---|---|---|---|---|---|
| GET 64B P1 c50 | single | 215801 | 38.5us | 1.000 | 1.000 | 0.760+0.867 | 0.759+0.867 | 217.0us | 529.3us |
| GET 64B P1 c50 | pair | 215105 | 39.0us | 1.000 | 1.000 | 0.869+0.999 | 0.868+0.999 | 217.4us | 523.7us |
| GET 64B P16 c128 | single | 2973678 | 2.84us | 0.062 | 0.063 | 0.026+0.131 | 0.026+0.131 | - | - |
| GET 64B P16 c128 | pair | 2994213 | 2.83us | 0.062 | 0.063 | 0.033+0.143 | 0.033+0.143 | - | - |
| GET 1KiB P1 c50 | single | 213641 | 38.5us | 1.000 | 1.000 | 0.758+0.871 | 0.757+0.871 | 218.7us | 545.3us |
| GET 1KiB P1 c50 | pair | 214136 | 39.2us | 1.000 | 1.000 | 0.861+0.999 | 0.860+0.999 | 219.2us | 521.8us |
| GET 1KiB P16 c128 | single | 2345362 | 3.56us | 0.062 | 0.063 | 0.023+0.132 | 0.023+0.132 | - | - |
| GET 1KiB P16 c128 | pair | 2348381 | 3.58us | 0.062 | 0.063 | 0.027+0.137 | 0.027+0.137 | - | - |

## Results, wire (redis-benchmark, separate process)

GET 64B P16 c128, three alternating reps:

| rep | single | pair |
|---|---|---|
| 1 | 2.148M | 2.115M |
| 2 | 1.981M | 2.210M |
| 3 | 2.192M | 2.131M |
| median | 2.148M | 2.131M |

GET 64B P1 c50, three alternating reps:

| rep | single | pair |
|---|---|---|
| 1 | 158.2k | 157.3k |
| 2 | 158.1k | 150.9k |
| 3 | 150.0k | 150.1k |
| median | 158.1k | 150.9k |

GET 1KiB P16 c128: single {1.743M, 1.440M, 1.465M} median 1.465M, pair {1.600M, 1.590M, 1.384M} median 1.590M; the run-to-run spread swallows the difference in both directions.

P1 latency (single rep pairs, p50/p99 msec): single 0.159/0.367 and 0.183/0.383, pair 0.175/0.367 and 0.175/0.375; overlapping.

Counters per op on the wire (from INFO around the timed window):

| cell | shape | writes/op | wakes/op (wk+conn) | parks/op (wk+conn) |
|---|---|---|---|---|
| GET 64B P1 c50 | single | 1.000 | 0.714+0.806 | 0.714+0.806 |
| GET 64B P1 c50 | pair | 1.000 | 0.880+1.000 | 0.880+1.000 |
| GET 64B P16 c128 | single | 0.063 | 0.053+0.100 | 0.053+0.100 |
| GET 64B P16 c128 | pair | 0.063 | 0.071+0.132 | 0.071+0.132 |

RSS at 512 idle connections (one PING each, then quiet): single 54.3MiB, pair 69.5MiB, about 31KiB per connection saved.

## Verdict

Frozen 2026-07-10, single as default, pair kept behind Options.ConnShape / -conn-shape for the M10 A/B.

Throughput and latency are within noise in every cell on this box, both in process and on the wire; the wire P1 median leans single (+4.8% at 64B) but the rep spread overlaps.
The structural claims all hold and are counter-proven: writes/op is unchanged (1.000 at P1, 0.063 at P16, so the boundary flush discipline is shape-independent), and total wakes/op at P1 drops from 1.88 to 1.52 on the wire (conn-side 1.00 to 0.81) rather than all the way to 1.0, because the single goroutine's post-publish spin window now absorbs some worker wakes the pair's long-parked writer always paid.
The predicted 150-300 ns/op is real in the counter bands (about 0.5 wake+park pairs per op removed) but invisible under a 170us client-bound loopback RTT, which is exactly the lab 14 regime statement: wakes are a P1-at-scale lever, not a laptop-loopback one.
The unambiguous wins are one goroutine per connection (15.6MiB RSS at 512 idle conns, about 31KiB each, which the f3 memory bar banks directly) and conformance with the section 4.1 spec shape the section 6 budget costs out.
The pair shape earned no cell and keeps no default; it stays behind the knob solely so the M10 decision A/B on the gate box can retire it with numbers.
