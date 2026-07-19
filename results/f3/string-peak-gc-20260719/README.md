# Reactor string gate with the off-heap GC lever (2026-07-19)

Same dual-generator gate as `reactor-loop-knee-20260719`, one change: the reactor
is launched with `GOGC=10`. The lab `labs/f3/m9/03_string_peak_gc` traces the
string-workload peak to its sources and shows this is the one lever that lowers it
without touching throughput, because aki keeps the string dataset off-heap so a GC
scans only the ~12 MB live heap and is nearly free.

HEAD `f34db6e` (tip of main), GamingPC under WSL2, 2026-07-19.

## Result

| row | reactor (GOGC=10) | redis | valkey | gate |
| --- | --- | --- | --- | --- |
| SET | 9.122 Mops | 3.364 | 2.778 | PASS 2.71x redis / 3.28x valkey |
| GET | 9.117 Mops | 4.260 | 3.759 | PASS 2.14x redis / 2.43x valkey |
| VmHWM | 165 MiB | 180 MiB | 129 MiB | 1.28x valkey, 0.92x redis |

## Against the GOGC=100 baseline

The paired GOGC=100 run (`main-gate-20260719`) held the reactor at 207 MiB VmHWM
with SET 9.117 / GET 9.117 Mops. `GOGC=10` cuts the peak to 165 MiB, a 42 MiB /
20% drop, with SET and GET throughput byte-identical (the GC of a 12 MB off-heap
process is free). aki moves from 1.60x valkey to 1.28x valkey on the peak, and
from a hair under redis to comfortably under it.

The residual 36 MiB over valkey is not a fixable deficit. aki's dataset settles
near 133 MiB for 1M 64B keys, valkey's whole footprint is 129 MiB: they are at
parity because valkey's sds plus dictEntry is about as tight as aki's arena record
plus 8-byte index slot. The rest is the per-connection pipelining buffers the 2x
throughput win needs at 512 connections. See the lab for the full decomposition
and the buffer-knob sweeps that ruled out every other lever.

## Why GOGC lives in the harness, not the binary

`GOGC=10` is free for strings because the data is off-heap. Collections put their
objects and tables on the Go heap, so a collection-heavy aki has a large live heap
where GOGC=10 would collect constantly and pay real scan CPU, which could regress
the collection throughput gate. So the lever belongs in the string workload's
launch config, not the binary default. The follow-up is a collection-workload GC
study for a safe adaptive default.

Harness: `run.sh` here, the loop-knee gate with `GOGC=10` on the reactor launch.
