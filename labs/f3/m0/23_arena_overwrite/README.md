# Lab: arena footprint under overwrite churn vs the reclaim threshold

Spec 2064/f3, M0 gate follow-up, lab 23. Follows lab 20's named open lever.

## The question

The M0 headline throughput gate now passes at the mandated cell (64B, 1M-key
uniform, P16/512: SET 3.42x, GET 2.57x vs CF16-tuned rivals,
results/GamingPC/calibration/p16-512-1Mkey-vs-frozen.md), but the memory bar
fails: aki rides ~190MB VmHWM after the load against redis 151MB and valkey
126MB. Lab 20 fixed the GC-pacing breach (the arena maps anonymously now) and
its frozen verdict named the remaining suspect: at a value this small "the
arena's dead-record slack and per-shard reservation outweigh a value that
small." The write-heavy SET gate overwrites each of 1M keys ~10-15 times, so the
arena fills with superseded records. The hypothesis under test: the reclaim
threshold (a segment is a compaction victim only once dead*den >= fill*num,
frozen at 1/4 by lab 10) is the lever, and with 2.5-3.4x throughput surplus to
spend, a tighter threshold keeps less dead slack resident and drops the figure
under the rival bar.

## Method

`go run .` sweeps the threshold {1/2, 1/4 shipped, 1/8, 1/16} at the box gate
shape (default 8 shards x 256MiB arena, 1M keys of 16B key + 64B value, 15
overwrite rounds ~ 15M writes), each cell re-exec'd as its own child so VmHWM is
per-config. Each child fills then overwrites, calling CompactArena at a
drain-boundary cadence the way the shard worker does between drain passes (every
4096 writes, plus the ArenaTight backpressure trigger), then prints VmHWM, the
arena live/alloc ledger (dead = alloc - live), heapSys, and the write loop's
ops/sec. The rival bar for this dataset is redis 151MB, valkey 126MB VmHWM.

The store microbench is not the whole picture, so a server-side decomposition
probe backs it: `cmd/f3srv` loaded with the same 1M/64B dataset via
`redis-benchmark -t set -n 5M -r 1M -P 16` at c512 vs c50, reading the server's
VmHWM after the load. This isolates what the store owns from what the connection
fabric owns.

## Predictions (filed before the box run)

Threshold sweep: 1/2 rides highest (laxest, most dead slack retained), 1/16
lowest, with a monotone drop of tens of MB across the sweep, and the tighter
cells landing near or under the 151MB rival line. ops/sec flat or within a few
percent across the sweep (compaction is off the hot path). The tighter threshold
is the memory lever; ship it.

## Results

Box (GamingPC WSL2), Linux VmHWM, lab cross-built for linux/amd64, box gate
config 8 shards x 256MiB, 2026-07-12:

```
thresh=1/2  VmHWM= 127.9MB arenaLive=96.0MB arenaAlloc=96.0MB dead=0.0MB heapSys=31.3MB ops/s=7074666
thresh=1/4  VmHWM= 129.0MB arenaLive=96.0MB arenaAlloc=96.0MB dead=0.0MB heapSys=35.3MB ops/s=7116381
thresh=1/8  VmHWM= 130.0MB arenaLive=96.0MB arenaAlloc=96.0MB dead=0.0MB heapSys=35.3MB ops/s=7042290
thresh=1/16 VmHWM= 127.6MB arenaLive=96.0MB arenaAlloc=96.0MB dead=0.0MB heapSys=35.3MB ops/s=7127963
```

The prediction is wrong. Every threshold rides ~128MB VmHWM with dead=0.0MB and a
flat ~7.1M ops/sec. The between-drain compaction keeps up with the overwrite
churn regardless of threshold: no dead slack survives to a measurement point, so
the threshold has nothing to differentiate. The macOS run agrees (flat ~130MB at
all four cells). Critically, the store alone rides ~128MB (96MB arena live +
~32MB heap), already UNDER redis's 151MB for this dataset.

So the box's ~190MB gate figure is not the store. The c512-vs-c50 decomposition
probe, same server, same 1M/64B dataset, only the client connection count
differing:

| conns | server VmHWM |
|---|---|
| c512 | 228,624 kB |
| c50  | 144,940 kB |

The 83.7MB delta is per-connection buffers plus reactor free-lists: each
connection pins a 64KiB read buffer and a 64KiB reply buffer
(f3srv/drivers/server.go readBufSize/replyBufSize = 64<<10), and the reactor
loops retain up to loopBufFree=256 freed buffers each. At c50 aki is 145MB,
already below redis 151MB and near valkey 126MB. The memory-bar failure at the
gate is entirely the c512 connection fabric, not the data and not the arena.

## Verdict (frozen)

NEGATIVE on the hypothesis: the arena reclaim threshold is not the memory lever.
Under the gate's overwrite churn the between-drain compaction holds dead slack at
zero at every threshold {1/2 .. 1/16}, so tightening it moves nothing; the
1/4 ship value stays, unchanged and vindicated as throughput-neutral by lab 10.
The store footprint for 1M x 64B is ~128MB, already under both rivals.

The real M0 memory lever is per-connection buffer sizing at high fan-out. The
gate's 190-228MB is store (~128-145MB) plus ~64-84MB of c512 connection buffers
(64KiB read + 64KiB reply each, plus loop free-lists). At c50 aki already clears
the bar (145MB < redis 151MB). The pipeline batch at P16/64B is ~640 bytes, so
the 64KiB read buffer is ~100x oversized for the gate's actual depth. Lab 24
takes up the buffer-size sweep: make readBufSize/replyBufSize configurable and
find the size that holds P16/512 throughput while dropping c512 VmHWM under the
rival line.
