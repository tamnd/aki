# Where the reactor's string-workload peak memory goes, and the free GC lever (2026-07-19)

The reactor clears the 2x throughput gate on the 1M-key 64B string SET/GET row
against both frozen rivals with room to spare, but its peak VmHWM sits above
valkey: 207 MiB against valkey's 132 MiB on the dual-generator gate. The memory
bar counts peak VmHWM, so this is the open half. This lab traces the peak to its
sources on the box and finds the one lever that moves it without touching
throughput.

## What the peak is made of

Four on-box probes on the GamingPC under WSL2, reactor `-shards 8 -arena-mib 512
-net reactor -net-loops 0`, redis-benchmark `-r 1000000 -d 64 -P 16`.

**The dataset is off-heap and ties valkey.** A `GODEBUG=gctrace=1` load of 632k
distinct keys held the Go heap at 12 MB live (`gc 7: 17->17->12 MB`) while total
VmHWM was 86 MiB. The keys and values live in the mmap arena (`MAP_ANON`, not GC
scanned), the index is a custom open-addressed table of 8-byte slots (no key
bytes on the heap). Extrapolated to 1M keys the dataset settles near 133 MiB,
which is valkey's whole footprint (132 MiB). aki cannot go below valkey on this
row: valkey's sds plus dictEntry for a 64B value is about as tight as aki's arena
record plus index slot. They are at parity by construction, not by a fixable
deficit.

**The gap over valkey is transient buffers plus GC headroom, and it scales with
connection count.** Filling the full 1M keyspace and reading VmHWM at each stage:

| stage | conns | VmHWM |
| --- | --- | --- |
| dataset settled (VmRSS after 3s) | 256 | 157 MiB |
| after GET blast (reply buffers) | 256 | 166 MiB |
| dual-generator gate | 512 | 207 MiB |

Doubling the connection count from 256 to 512 adds roughly 40 MiB. Each
connection carries a 64 KiB read buffer, a 64 KiB reply buffer, one open
`hopBatch` per shard, and a reorder ring of `replyRing` slots (runtime.go: "the
hopBatch data/reply buffers and the reply reorder ring dominate resident
footprint").

## The two knobs that do nothing, and the one that works

**Shrinking the per-connection read/reply buffers does not help.** Throughput is
flat across every size (the 64B P16 workload never fills a 64 KiB buffer), but
VmHWM barely moves and the smaller sizes trend worse:

| read/reply buf | SET | GET | VmHWM |
| --- | --- | --- | --- |
| 64k / 64k | 3.98M | 3.98M | 163 MiB |
| 16k / 16k | 3.98M | 3.98M | 159 MiB |
| 8k / 8k | 3.98M | 3.99M | 169 MiB |
| 4k / 4k | 3.98M | 5.29M | 166 MiB |

**Shrinking the reorder ring and batch caps is counterproductive.** The lean
configs used *more* memory than the baseline, and the smallest batch cap hurt SET
throughput:

| ring / batchcap / free / buf | SET | GET | VmHWM |
| --- | --- | --- | --- |
| 128 / 1024 / 8 / 64k (default) | 3.98M | 3.98M | **159 MiB** |
| 64 / 1024 / 4 / 32k | 3.98M | 4.00M | 191 MiB |
| 32 / 512 / 4 / 16k | 3.97M | 3.98M | 182 MiB |
| 32 / 256 / 2 / 16k | 3.18M | 3.97M | 181 MiB |
| 24 / 256 / 2 / 8k | 3.19M | 3.98M | 179 MiB |

Smaller buffers churn: they reallocate and grow under load, and the garbage they
leave inflates the peak faster than the smaller live size shrinks it. The
reactor's shipped defaults are already the memory-optimal buffer point. This is
the important negative result, it rules out the obvious lever.

**GOGC is the lever, and it is free here.** Because the string dataset is
off-heap, the Go heap is almost all transient connection-buffer churn over a
12 MB live set. Default GOGC=100 lets that churn accumulate to 2x live before a
collection. Lowering GOGC forces earlier collections, and a collection of a
12 MB heap that does not scan the arena is nearly free, so throughput does not
move:

512 connections, baseline buffers:

| GOGC | SET | GET | VmHWM |
| --- | --- | --- | --- |
| 100 | 4.77M | 4.79M | 189 MiB |
| 25 | 4.77M | 5.96M | 166 MiB |
| 10 | 4.77M | 4.77M | **165 MiB** |

## Gate-level confirmation

The real dual-generator gate, aki launched with `GOGC=10`, everything else the
frozen gate config:

| row | reactor | redis | valkey | gate |
| --- | --- | --- | --- | --- |
| SET | 9.122 Mops | 3.364 | 2.778 | PASS 2.71x / 3.28x |
| GET | 9.117 Mops | 4.260 | 3.759 | PASS 2.14x / 2.43x |
| VmHWM | **165 MiB** | 180 MiB | 129 MiB | see note |

`GOGC=10` cuts the reactor peak from 207 to 165 MiB, a 42 MiB drop, with SET and
GET throughput byte-identical to the GOGC=100 run. aki moves from 1.60x valkey to
1.28x valkey and drops well under redis. The residual 36 MiB over valkey is the
dataset parity (aki 133, valkey 129, structural on pure strings) plus the
pipelining buffers the throughput win needs.

## Why this is not baked into the binary default

`GOGC=10` is free for a string workload because the data is off-heap. Collections
are the opposite: set, zset, hash, list, and stream objects and their internal
tables live on the Go heap, so a collection-heavy aki has a large live heap where
GOGC=10 would collect constantly and pay real scan CPU. A blanket low GOGC could
regress the collection throughput gate, which the mandate forbids. So the lever
belongs in the workload's launch config, not the binary: the string gate harness
sets it, and a string-dominant deployment can. The follow-up is a
collection-workload GC study to see whether an adaptive `debug.SetGCPercent` that
bounds absolute heap headroom (rather than a fixed percentage) is safe to make
the default. GOMEMLIMIT was tried here and went the wrong way (a 140 MiB limit
raised VmHWM to 167), so it is not that mechanism.

## Reproduce

`sweep.sh` runs all four sweeps on the box (dataset probe, read/reply buffer,
reorder ring, GOGC) and prints the tables above. Point `BIN` at an f3srv build
and the redis-benchmark tools under `/root/bin`.
