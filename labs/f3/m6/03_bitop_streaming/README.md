# Lab 03: BITOP streaming vs gather-then-materialize, the memory bound

Part of issue #548, the M6 bitmap milestone, lab 03, the BITOP memory decision (doc 15 section 5). This is the lab the co-located BITOP slice ships with, per the labs-per-perf-change rule: it prices the streaming form against the gather-then-materialize form so the memory bound is measured, not asserted.

## Question

BITOP AND|OR|XOR reads one or more source bitmaps and writes a result as long as the longest source; NOT is the single-source complement. The obvious form gathers every source whole into memory, runs the op over the full length, and holds the whole result, so a three-source AND on 256 MiB bitmaps needs about a gigabyte resident at the peak. The form the slice ships streams: it walks the sources chunk by chunk, applies the word kernel to one chunk from each source, writes that result chunk to the destination owner, and moves on, so at most (sources + 1) chunks are live no matter how long the bitmaps are. That is the L11 discharge and it is the whole point of the memory bar: aki must hold less than a rival that materializes, and the peak VmHWM is what the bar counts.

The question: how flat does the streaming peak stay as the bitmap length grows, and does streaming per chunk cost anything in throughput to buy that flat peak.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. `andWords`/`orWords`/`xorWords`/`notWords` are byte-for-byte the kernels in `engine/f3/store/bitop.go`. `applyStreaming` walks (sources + 1) chunk buffers and hands each result chunk to a sink, the model of the `SetRange` write to the destination owner that leaves the working set at once; `applyMaterialize` gathers each source whole and holds the whole result, the strawman a naive cross-shard coordinator would take. The sweep runs a three-source AND over 1 MiB, 16 MiB, and 256 MiB bitmaps and reports each form's ns/op, the streaming peak resident model, the measured `TotalAlloc` delta over a run, and the peak ratio.

`go run .` runs the whole sweep; `-quick` cuts the iteration counts for the shared runner. `main_test.go` carries `TestFormsAgree` (the streaming form, the materialize form, and the byte oracle must agree bit for bit over every op and lengths that straddle the chunk boundary, so the memory numbers compare one answer) plus `BenchmarkStreaming` and `BenchmarkMaterialize` for a hand run. CI drives the test.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process, `-quick`.

AND over 3 sources, result length = source length:

| size | stream ns | mat ns | stream MiB | streamAlloc MiB | matAlloc MiB | peak ratio |
|---|---|---|---|---|---|---|
| 1MiB | 292375 | 854250 | 0.25 | 0.25 | 4.01 | 16x |
| 16MiB | 5348958 | 10224458 | 0.25 | 0.25 | 64.01 | 256x |
| 256MiB | 118879084 | 427273583 | 0.25 | 0.25 | 1024.00 | 4096x |

The streaming peak holds flat at 0.25 MiB (four 64 KiB buffers, three sources plus the result) across a 256x growth in bitmap length, while the materialize peak climbs to a full gigabyte at 256 MiB. The peak ratio is the memory bar: aki holds 4096x less than the gather form at the top of the sweep.

Streaming is also the faster wall-clock at every size, roughly 3x here, because its chunk buffers stay hot in cache across the whole scan where the materialize form drags full multi-hundred-MiB buffers through DRAM. The memory win comes with a throughput win, not a trade.

## Verdict

Both forms do the same word arithmetic, but the streaming form's peak resident stays flat at (sources + 1) * 64 KiB while the materialize form's grows with the bitmap length, a 4096x gap at 256 MiB and three sources, and streaming is faster on top because it stays cache-resident. This is why aki holds less than a rival that gathers whole bitmaps, the memory bar the BITOP slice is built to clear. The cross-shard slice carries the same (sources + 1) residency into the F17 hop coordinator.
