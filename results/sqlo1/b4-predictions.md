# B4 predictions, filed before the G3 measurement

Milestone B4 (tamnd/aki#724); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the run.
The G3 measurement itself (bytes-per-key table across the type suites' datasets against the Redis 8.8 RDB file and the f3 disk footprint, plus the core-suite rerun with compression on) waits on the gate box; this note files the two milestone predictions with the software-side evidence that is already in.

## What changed under these predictions

Slices 1 through 6 built the cascade but saved no disk: groups sat in fixed 4 KiB slots and the fit rule worked off the raw projection, so a frame that compressed 10x still held four records like a raw group.
Slice 7 (#1306) is the lever the disk prediction leans on: AppendPacked certifies at accept time that the compressed image fits the slot, and both 950 byte reference shapes (constant and json) now pack 67 records per group against 4 raw, pinned by the 64 KiB ulen cap rather than the compressor.

## PRED-SQLO1-B4-DISK

On-disk bytes per key on the mixed-type G3 corpus lands under 0.7x the Redis 8.8 RDB file for the same dataset.
Reasoning on record: the cascade lab (#1295) put the per-shape ratios on record at extent-stream windows, json bodies 0.19, timestamps 0.20 under forpack, low-cardinality values under dict at the cheapest decode, with uuids at 0.55 the worst real shape, and packing now converts those ratios into slot occupancy instead of leaving them theoretical.
Redis RDB compresses strings over 20 bytes with LZF, which is consistently weaker than the zstd and dictionary schemes on every shape the lab measured, and RDB pays nothing back on integer shapes where forpack lands 5x.
The comparison is not free for sqlo1b: the fixed bill is the chunk index at ~12 B per entry over a 0.465 B per key directory (measured at 1e8 in the b2 chunkindex lab), the record envelope around every value, the 2 byte slot table entry, and whatever slack the last open group in each extent strands.
The 0.7x target prices that overhead in; the raw ratios alone would suggest far lower.
Miss conditions, on record: a corpus dominated by short values where the envelope plus index bill exceeds the compression win; a corpus dominated by high-entropy cores (uuid-class, session tokens) where zstd holds only 0.55 and the boxed FSST stretch is the answer; or live-fraction effects where dead records inflate the file before compaction has walked the extents, which the measurement must control by checkpointing and letting the debt controller settle before the size is read.

## PRED-SQLO1-B4-THROUGHPUT

Compression on costs under 10 percent throughput on the 1x arm and wins outright on the 16x arm.
Reasoning on record for the 1x arm: the write path pays nothing new until the raw projection is out of room, and past that each accepted packed append re-trials only the winning scheme, with the full selector running only on the first pack or when the winner stops fitting.
The read path pays the frame decode, 555 ns cold dict and 6.2 us cold zstd against 8.8 ns cached (#1304 bench on the M4), and the FrameCache amortizes clustered reads so a one-group batch pays exactly one decode.
The 10 percent budget prices the cold-read tax on scattered point reads plus the compaction-side trial encodes.
Reasoning on record for the 16x arm: when the dataset is 16x the memory budget the bill is IO, and packed groups move up to 16.7x fewer group reads per record for compressible shapes, so fewer bytes moved beats the decode cost exactly as doc 13 frames it.
Miss conditions, on record: a point-read-heavy mix with no locality, where every read is a cold decode and the 8-entry FIFO never hits, would push the 1x arm past 10 percent, and the counter-move is sizing the FrameCache, not weakening certification; a compaction-bound write mix could surface the per-append trial cost, and the counter-move there is trialing every k-th append with a safety margin.

## Bookkeeping

Filed before any G3 rep has run on the gate box.
The software half of B4 is complete as of this note: slices 1 through 7 merged (#1297, #1298, #1300, #1301, #1303, #1304, #1306), the boxed stretch (FSST, trained-dictionary zstd) stays closed unless the G3 table opens it, per the zdict verdict (#1296).
The measurement lands in its own results note with the corpus recipe per type suite, the RDB provenance (redis 8.8, rdbcompression yes, one SAVE after load), the f3 column, the core-suite delta table both arms, and the crash-suite row with compressed groups; these predictions get their verdicts there.
