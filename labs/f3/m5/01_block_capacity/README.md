# Lab 01: block byte budget and entry cap for the stream append log

Part of issue #547, the M5 stream milestone, lab 01, the block-geometry decision (doc 14 section 12.5 lab decision 1). This is the lab the first slice (entry chunks and the ID codec) depends on: it settles the block byte budget and entry cap before the slice bakes them into the block layout, per the labs-per-perf-change rule.

## Question

A native stream is an append log of entry blocks (doc 14 section 3.2): a block is one arena allocation with a 48-byte header holding a run of consecutive entries, closed when it fills a byte budget or an entry cap, whichever binds first. Entries pack with the master-delta encoding (section 3.3): the first entry in a block is the master, stored whole with its field names; every later same-schema entry stores only a flags byte, ID deltas against the block firstID, and its values, the names implied by the master. A 32-byte directory leaf indexes each block.

Doc 14 section 12.5 fixes the default at 4096 bytes / 128 entries (Redis's stream-node knobs) and pre-registers the sweep: 1KiB to 16KiB budgets and 64 to 256 entry caps, scored on XADD throughput, XRANGE decode, cold-read size, and directory RSS. So the question: what block geometry holds the 64B fixed-schema entry inside the section 10.1 memory bar (6 to 8 bytes of overhead per entry over payload, stretch 8, ceiling 10) at the best cold-read granularity, and does the same geometry serve the fat-value band?

## Method

In-process, no server, no wire, no engine import. The block here is lab-local code that models doc 14's structure so the geometry can be priced before the slice writes it. A block carries its entries in a byte blob after a 48-byte header, encoded exactly as section 3.3 lays out: the master whole, same-schema entries as a flags byte, the two ID deltas, and value frames. The bytes are real (varints written, values zero-filled) so an XADD is a real memcpy-class encode and an XRANGE is a real varint decode walk. Resident cost counts the 48-byte header and the 32-byte directory leaf per block plus the entry bytes, so header and directory slack are on the ledger the way F14 requires.

IDs are dense auto-IDs at a settable entries-per-millisecond rate, the benchmark-shaped case the memory rows lean on, advanced like the section 3.6 allocator (seq increments within a millisecond, rolls to a fresh millisecond at the rate). The seq delta is zigzag-coded: a block that spans a millisecond boundary carries entries whose seq restarted below the block firstSeq, a negative delta, exactly the signed listpack integer Redis stores. Without the zigzag the plain uvarint underflows to a 10-byte value and the sparse-ID overhead reads falsely high; Sweep C is the arm that catches that.

Overhead is measured over payload, where payload is the value bytes only: field names are overhead (collapsed ~100x by mastering, per section 10.1), so mastered names, flags, ID deltas, value-length varints, the header, and the directory leaf all count against the bar.

`go run .` runs the whole sweep. `-quick` is accepted for the shared runner. `TestDefaultGeometryMeetsMemoryBar`, `TestSameSchemaCompressesAgainstMaster`, `TestSeqDeltaSmallAcrossMillisecondBoundary`, `TestIDDensityDoesNotBlowOverhead`, and `TestColdWindowPreadsMinimalAtDefault` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process. Block header 48B, directory leaf 32B. `arm` is byteBudget/entryCap.

Sweep A, block geometry per entry band (dense IDs, 1000/ms):

| arm | band | ent/blk | xaddNs | xrngNs | dirB/e | ovhd/e |
|---|---|---|---|---|---|---|
| 1024/128 | 3x8B | 31.0 | 28.6 | 5.84 | 1.033 | 9.50 |
| 2048/128 | 3x8B | 64.9 | 24.8 | 8.45 | 0.493 | 7.71 |
| 4096/128 | 3x8B | 128.0 | 25.4 | 7.01 | 0.250 | 7.36 |
| 8192/128 | 3x8B | 128.0 | 28.5 | 7.68 | 0.250 | 7.36 |
| 16384/128 | 3x8B | 128.0 | 31.8 | 9.22 | 0.250 | 7.36 |
| 4096/64 | 3x8B | 64.0 | 31.9 | 6.63 | 0.500 | 7.72 |
| 4096/256 | 3x8B | 130.9 | 24.2 | 6.72 | 0.244 | 7.35 |
| 4096/128 | 1KiB | 3.0 | 124.6 | 15.2 | 10.667 | 35.00 |
| 8192/128 | 1KiB | 7.0 | 110.3 | 29.6 | 4.572 | 17.86 |
| 16384/128 | 1KiB | 15.0 | 63.4 | 17.1 | 2.134 | 11.01 |

(1024/128 and 2048/128 at the 1KiB band are marked `solo`: the block cannot hold two 1KiB entries, so a fat entry goes to a solo block per section 3.7, not a geometry point.)

Sweep B, cold read of a COUNT-100 window, 3x8B entries:

| arm | ent/blk | preads | bytesRead | amp |
|---|---|---|---|---|
| 1024/128 | 31.0 | 5 | 5,030 | 2.10 |
| 2048/128 | 64.9 | 3 | 6,081 | 2.53 |
| 4096/128 | 127.8 | 2 | 7,951 | 3.31 |
| 8192/128 | 127.8 | 2 | 7,951 | 3.31 |
| 16384/128 | 127.8 | 2 | 7,951 | 3.31 |
| 4096/64 | 64.0 | 3 | 5,993 | 2.50 |
| 4096/256 | 130.7 | 2 | 8,132 | 3.39 |

Sweep C, ID density vs overhead at 4096/128, 3x8B entries:

| rate (ent/ms) | ent/blk | ovhd/e |
|---|---|---|
| 1000 | 128.0 | 7.36 |
| 100 | 128.0 | 6.97 |
| 10 | 128.0 | 6.84 |
| 1 | 128.0 | 6.84 |

## Reading the sweep

The 64B fixed-schema band is the memory bar, and 4096/128 sits on the knee. The 128 entry cap and the 4096 byte budget bind together for this shape: 128 entries of ~30 encoded bytes fill ~3900 bytes, just under the budget, so the block closes on the cap at exactly 128 entries with almost no slack. That is why the default matches Redis's stream-node knobs, and the sweep shows why the number is not arbitrary. Below 4096 the byte budget binds first and the block holds fewer entries (64.9 at 2048, 31 at 1024), so the 48+32 byte header and directory amortize over fewer entries and the overhead climbs (7.71, then 9.50). Above 4096 the cap still binds at 128, so a bigger budget is inert for this band: 8192 and 16384 hold the same 128 entries at the same 7.36 overhead and the same 2-pread cold window, buying nothing while wasting arena on a wider tail block.

XADD and XRANGE throughput are a wash across the geometries, which is the expected and honest result: both are per-entry byte work (a memcpy-class encode, a varint decode walk) and the block size changes amortization and cold-read granularity, not the per-entry cost. XADD holds around 25 to 30 ns/entry and XRANGE decodes at 5 to 9 ns/entry regardless of budget, so the geometry decision is a memory and cold-read decision, not a throughput one. The XRANGE figure straddles the P2 prediction of under 5 ns/entry; the lab models the decode in interpreted Go with a general varint reader, and the real path with a specialized decoder and the value bytes hot should land under it, which the gate run pins (PRED-F3-M5-XADD and P2, absolute ns deferred there).

The cold-read sweep is the tension the byte budget actually resolves. A COUNT-100 window costs one pread per spanned block (section 9.4), and a mid-block window spans the minimum two blocks only when the block holds at least ~100 entries. At 4096/128 that is 2 preads for 7,951 bytes; a smaller 1024/128 block reads fewer total bytes (5,030) but costs 5 preads, and on cold storage the pread count is the latency term, not the byte count. So the byte budget wants to be large enough that a typical window is one or two preads (4096 clears this) but no larger, since past that point the extra bytes per pread are pure slack read for a small window (16384 reads the same 2 blocks but each is 4x the bytes it needs). 4096 is the smallest budget that buys the 2-pread minimum for the 100-window, which is the knee.

Sweep C is the honesty check on the compression claim. Every ID density holds inside the 6 to 8 bar (7.36 dense down to 6.84 sparse), so the memory numbers are not an artifact of benchmark-dense IDs. The sparse arms are actually slightly cheaper because a sparse stream rolls the millisecond more often, and after the zigzag fix a cross-boundary seq delta is a 1 to 2 byte signed varint, not a blowup. This arm is why the encoding zigzags the seq delta: the first cut used a plain uvarint and the underflow on the millisecond rollover reported rate-100 overhead at 11.84, a false alarm that would have argued for a design change the math did not need.

The fat-value band tells the design where its limit is, and the answer is the separation rule, not a bigger block. A 1KiB entry fits 3 per 4096 block, so the header and directory amortize over 3 entries and the overhead reads 35 B/entry. That looks alarming next to the 8 bar but it is 35 bytes over a 1024-byte payload, 3.4% overhead, and the F14 bar of 6 to 8 is defined for the 64B fixed-schema shape (section 10.1), not for a value-dominated entry. A bigger block improves the fat-band amortization (11 B/entry at 16384) but at the cost of the 64B band's cold-read granularity, so the doc does not chase it with a wider default: section 3.7 sends an entry whose encoded size exceeds the byte budget to a solo block sized to the entry, and an individual value past the F5 threshold to the value log behind a ref. So the fixed 4096/128 serves the common band at the bar, and fat entries take the separation path rather than reshaping the block for everyone.

## Darwin caveat

The overhead, entries-per-block, directory-bytes-per-entry, and cold-read columns are structural byte and block counts, so they do not depend on the box: the 6-to-8 memory verdict and the 2-pread cold window hold on the Linux gate box by construction. The XADD and XRANGE ns/entry columns are darwin/arm64 timings of an interpreted model and are indicative only; the absolute throughput and the P1/P2 predictions (XADD in the SET cost class, XRANGE under 5 ns/entry) land at the M5 gate run, and the rival RSS comparison at PRED-F3-M5-STREAMMEM there. What is frozen here is the geometry: 4096/128.

## Verdict

Frozen for the M5 stream milestone: block byte budget 4096, entry cap 128, matching Redis's stream-node knobs, with the section 3.7 separation path for fat entries.

- **4096/128 sits on the knee for the 64B fixed-schema band.** The cap and the byte budget bind together at 128 entries (~3900 bytes), so the block closes with almost no slack, the header and directory amortize to 7.36 B/entry overhead (inside the 6-to-8 bar), and the directory is 0.25 B/entry, the section 10.1 target. Below 4096 the overhead climbs (7.71 at 2048, 9.50 at 1024); above 4096 the cap still binds so a bigger budget buys nothing for this band and wastes tail-block arena.
- **Throughput is geometry-insensitive; the decision is memory and cold read.** XADD (~25 ns/entry) and XRANGE (~7 ns/entry) are a wash across budgets, so the byte budget is chosen for the cold-read window: 4096 is the smallest budget that reads a COUNT-100 window in the minimum 2 preads, where a smaller block costs more preads (the cold-storage latency term) and a larger one reads pure slack.
- **The compression holds across ID density.** Every rate stays inside the bar (6.84 to 7.36), so the memory numbers are not benchmark-dense-ID artifacts; the seq delta is zigzag-coded so a millisecond-boundary rollover stays a 1-to-2 byte signed varint.
- **Fat entries take the separation path, not a wider block.** A 1KiB entry's 35 B/entry overhead is 3.4% of payload and outside the fixed-schema bar's scope; section 3.7's solo-block and value-log separation serves the fat band without reshaping the block geometry the 64B band depends on.

No perf constant beyond the geometry is baked; the slice writes the 48-byte header, the 32-byte directory leaf, and the master-delta encoding with the zigzag seq delta this lab settled.
