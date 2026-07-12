# Lab: the c512 connection-footprint lever

Spec 2064/f3, M0 gate follow-up, lab 24. Follows lab 23's named lever.

## The question

Lab 23 proved the arena reclaim threshold is not the M0 memory lever: dead=0 at
every threshold, the store alone rides ~128MB (96MB arena live + ~32MB heap),
already under redis's 151MB for the 1M/64B dataset. A c512-vs-c50 box probe then
located the gate's 190-228MB overage in the connection fabric: same server, same
dataset, only the client connection count differing, server VmHWM moved 228,624
-> 144,940 kB. This lab isolates which per-connection structure carries that
cost, and whether trimming it holds throughput.

## Method

Two lines of evidence.

Box A/B (authoritative, Linux VmHWM, reactor driver, real redis-benchmark). The
gate server (`cmd/f3srv`, 8 shards, `-net reactor`) is loaded with 5M SET into a
1M keyspace at 64B values, P16, c512, and the server's VmHWM read after the load.
The suspect knobs are swept two ways: the socket buffers (`-read-buf-kib` /
`-reply-buf-kib`, the lab knob this PR adds), and the per-connection hop
transport caps (a rebuild, since those are compile-time consts in
`engine/f3/shard/tuning.go`).

In-process harness (`go run .`, cross-platform, this directory). Boots the real
server (`drivers.Listen`, goroutine driver) on loopback and drives it with a
pipelined SET load, sweeping connection count {50,128,256,512} and the server's
read+reply buffer size {64,16,8,4 KiB} at c512, each cell a re-exec'd child so
VmHWM is per-config. This harness co-locates the load generator (one client
goroutine per connection, each with its own 64KiB bufio pair), so its absolute
VmHWM is client+server, not server-only; its job is the reproducible SHAPE of the
two sweeps, not the gate's absolute figure. macOS VmHWM is directional (the
Go-scavenger madvise artifact); the shape holds on either platform.

## Predictions (filed before the box run)

The socket buffers are the cost: 512 x (64KiB read + 64KiB reply) = 64MB, so
shrinking them to 4KiB drops c512 VmHWM by ~56MB and lands aki near or under the
151MB rival line. The hop transport caps are not the suspect.

## Results

The prediction is backwards. Box socket-buffer sweep, c512 P16 64B, VmHWM in kB:

| read+reply | VmHWM |
|---|---|
| 64KiB | 222,852 |
| 32KiB | 220,524 |
| 16KiB | 231,980 |
| 8KiB  | 231,848 |
| 4KiB  | 231,232 |

Flat within noise. The reactor leases the read and reply buffers from a per-loop
free list, so they are a small, pooled share of the per-connection cost, not the
64MB the arithmetic suggested. The socket buffers are NOT the lever.

The lever is the per-connection hop transport. Each `Conn` pools up to
freeListCap(64) `hopBatch` nodes, each carrying a data buffer (batchDataCap 8192)
and a reply buffer (repCap = 8192 + 64*32 = 10240), plus a replyRing(1024) reply
reorder ring of ~40B `parked` entries (~40KB/conn). A box rebuild shrinking those
three caps, batchDataCap 8192->1024, replyRing 1024->128, freeListCap 64->8,
loaded identically:

| build | c512 VmHWM | SET (2-client summed) |
|---|---|---|
| default caps | 231,616 kB | 7,980,847 |
| small caps   | 173,748 kB | 7,980,847 |

58MB off c512 resident, throughput bit-identical (both client-capped at the same
rate, so the smaller caps cost the server nothing): at the 64B gate cell a hop
node holds ~640B of the 8192-byte data buffer and ~640B of the 10240-byte reply
buffer, so 90% of each pooled node is slack, and 512 connections x a handful of
pooled nodes x ~18KB is the resident overage. `go test ./engine/f3/shard/
./f3srv/...` stays green under the shrunk caps.

The in-process harness reproduces the shape. Box (Linux, goroutine driver, client
co-located), VmHWM in kB:

sweep 1, connection count at default buffers: 50 -> 114,996; 128 -> 199,336; 256
-> 288,132; 512 -> 474,504. Footprint scales with connection count.

sweep 2, read+reply buffer size at c512: 64KiB -> 459,392; 16KiB -> 406,720;
8KiB -> 389,816; 4KiB -> 418,160. Muddied by the co-located client's own buffers
but broadly flat, no monotone drop, consistent with the clean box reactor sweep:
buffer size is not the lever.

## Verdict

The M0 memory-bar failure at the 64B gate cell is the per-connection hop
transport, not the arena (lab 23) and not the socket buffers (this lab): the
pooled `hopBatch` data/reply buffers (batchDataCap 8192, repCap 10240) and the
replyRing(1024) reorder ring, ~18KB per pooled node plus ~40KB of ring per
connection, all sized for batchCap=32 and separated-band values while the gate's
64B cell fills ~640B of each. Shrinking the three caps takes c512 VmHWM 231,616
-> 173,748 kB, 58MB, with zero throughput cost.

174MB still sits 23MB over redis's 151MB, so the caps are one stacking layer, not
the whole distance. The fix this lab motivates: promote batchDataCap (starting
size, it grows on demand like the read buffer), replyRing, and freeListCap to
`shard.Config` fields so they are swept, right-sized to the P16 gate depth, and
landed with the sweep data per the tuning.go rule; the arena's 96B/key record
layout and GOGC are the further levers to close the last 23MB. The socket-buffer
knob (`ReadBufBytes`, added here) stays as a lab instrument but is proven not to
move the gate cell.
