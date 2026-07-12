# Lab: the per-connection hop-transport caps as a swept memory lever

Spec 2064/f3, M0 gate follow-up, lab 25. Follows lab 24's located lever and
lands the fix it motivated.

## The question

Lab 24 located the M0 memory-bar overage in the per-connection hop transport:
each `Conn` pools `hopBatch` nodes carrying a data buffer (`batchDataCap` 8192)
and a reply buffer (`repCap` 10240), plus a `replyRing`(1024) reorder ring of
~40B `parked` entries. A box rebuild shrinking all three at once dropped c512
VmHWM 231,616 -> 173,748 kB (58MB) with SET throughput bit-identical. That
combined number left two questions the gate needs answered:

1. Which of the three caps carries the footprint?
2. `batchDataCap` is a node split threshold, not just a starting size: a fuller
   node splits at the cap, so a smaller cap splits a pipelined run of larger
   values into more nodes. Does shrinking it cost throughput away from the 64B
   gate cell?

This lab promotes the three sizes to `shard.Config` fields (`BatchDataCap`,
`ReplyRing`, `FreeListCap`, wired through `drivers.Options` and `cmd/f3srv`
flags) so they are swept and set per box, then measures both questions.

## Method

`go run .` boots the real in-process server (`drivers.Listen`, goroutine driver)
on loopback and drives it with a pipelined SET load of a 1M keyspace, then reads
VmHWM. Each cell is a re-exec'd child so VmHWM is per-config. Two sweeps:

- sweep 1 (attribution), c512 P16 64B: baseline (all defaults), then each cap
  shrunk alone (batchDataCap 8192->1024, replyRing 1024->128, freeListCap
  64->8), then all three together.
- sweep 2 (throughput floor), c512 P16, batchDataCap {8192,4096,2048,1024} x
  value {64,256,1024}, extended to {2048,4096} values in a follow-up cell.

The harness co-locates the load generator (one client goroutine per conn), so
its absolute VmHWM is client+server: the absolute gate figure is lab 24's box
A/B, and this lab reads the SHAPE. Numbers below are the authoritative Linux run
on the gate box (GamingPC, 32 CPUs, 8 shards), where 512 clients plus 8 shards
fit and throughput is server-bound at ~13.4M, not client-capped.

## Results

Sweep 1, attribution at the 64B gate cell. All three caps contribute
independently, and shrinking them raises throughput (less memory traffic, better
cache residency):

| build | c512 VmHWM | delta | SET ops/s |
|---|---|---|---|
| baseline (all default) | 467,952 kB | | 13,352,422 |
| batchDataCap 8192->1024 | 398,900 kB | -69 MB | 13,816,459 |
| replyRing 1024->128 | 438,736 kB | -29 MB | 13,408,198 |
| freeListCap 64->8 | 442,196 kB | -26 MB | 13,427,459 |
| all three small | 376,468 kB | **-91 MB** | **14,112,884 (+5.7%)** |

Sweep 2, the batchDataCap throughput floor across a value band, c512 P16. ops/s,
box:

| value | default (8192) | 4096 | 2048 | 1024 |
|---|---|---|---|---|
| 64B  | 13,390,613 | 13,615,310 | 13,647,997 | 13,904,323 |
| 256B | 11,232,696 | 11,113,852 | 11,396,222 | 11,159,460 |
| 1024B | 5,782,863 | 5,740,660 | **5,054,888** | 5,121,503 |
| 2048B | 3,155,760 | 3,039,748 | 3,049,182 | (n/a) |
| 4096B | 2,274,625 | 2,254,174 | 2,382,164 | (n/a) |

The floor is specific: `batchDataCap` at or below 2048 loses ~11% at 1024B
values, where the cap collapses a P16 run from many commands per node to one or
two and pays a hop per command. `batchDataCap=4096` holds throughput flat to
-3.7% across the whole 64B-to-4KiB band (it keeps four 1024B commands per node,
two at 2048B), while still saving 40MB at the 64B gate versus 8192 (436,668 vs
476,652 kB) because 8192 wasted half of every node at 64B.

## Verdict

The three caps are the M0 memory-bar lever, confirmed and attributed: at the
64B gate cell they take c512 VmHWM down 91MB with throughput up 5.7%, and each
one contributes on its own (batchDataCap -69MB, replyRing -29MB, freeListCap
-26MB, overlapping through shared node retention). This lands them as
`shard.Config` fields so the gate sets them per box.

The shipped defaults move by what the sweep proves safe for a general-purpose
server that has to serve every value size, not just the gate's 64B:

- `batchDataCap` default 8192 -> **4096**: -40MB at the 64B gate, throughput
  flat to +3.7% across 64B-to-4KiB, no split-collapse cost (the -11% floor only
  bites at cap <= 2048 on 1KiB values). This is the sweep-backed default change
  the tuning.go rule asks for.
- `replyRing` and `freeListCap` defaults stay put: their gate saving is real
  (-29MB, -26MB) and throughput-flat, but this sweep only exercised the P16 gate
  depth and one access pattern. A global `replyRing` cut caps deep-pipeline
  clients (the window must cover the pipeline depth), and a global `freeListCap`
  cut risks alloc churn under bursty fan-out, so both stay swept knobs. A
  pipeline-depth and access-pattern sweep is owed before either default moves.

The gate's 64B memory cell runs the Config overrides
`-batch-data-cap 1024 -reply-ring 128 -free-list-cap 8` (all-three-small: the
box A/B of lab 24 measured that server-only at 173,748 kB, and this lab confirms
+5.7% throughput at the cell), the same shape redis's gate runs io-threads=6.

This is one stacking layer, not the whole distance: 173,748 kB still sits ~23MB
over redis's 151MB for the 1M/64B dataset. The next lever is the arena's
~96B/key record layout (the store rides ~128MB, and the index is a third of it),
with GOGC after that.
