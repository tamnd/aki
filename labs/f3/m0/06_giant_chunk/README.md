# Lab: giant-value chunk threshold and chunk size

Spec 2064/f3/09 sections 2 and 5, M0 lab 6.

## The question

Doc 09 chunks values at and above `str_chunk_min` = 64KiB into `str_chunk_size` = 64KiB chunks behind a 16B-per-chunk directory, so a windowed write (SETRANGE, APPEND) costs the touched chunks and never the value, which is the L11 bound. Chunking is not free: full reads and writes pay a directory walk and per-chunk dispatch, and the chunk size sets the price of one windowed write (the COW copies a whole chunk to apply a 100-byte window). Before the giant-value band bakes the constants in: what do full-value write and read throughput, the windowed-write cost, and the directory overhead actually look like per chunk size, and where does the unchunked shape stop being tolerable?

## Method

`go run .` sweeps values 64KiB, 256KiB, 1MiB, 4MiB, 8MiB against chunk sizes 16KiB, 64KiB, 256KiB, 1MiB (chunk below value), plus a whole-run row per value size, the single unchunked run a higher chunk threshold would keep. Chunk bytes live in a lab-side bump slab standing in for the value log; the real `engine/f3/store` holds one directory record per key (16B per chunk: run offset, length, CRC slot), and the engine is not modified. Per cell, over 256MiB of values: a full write pass (chunk copies in, directory built, one Set), a full read pass in shuffled key order streaming every chunk through one reusable chunk-sized buffer (the F19 shape, the value is never assembled), and up to 256 SETRANGE-shaped 100B windowed writes at random offsets (COW of the touched chunk plus a directory republish; the whole-run row rewrites the full value, which is the point of the row). The slab is pre-faulted so page zeroing is not charged to a pass, and Go heap allocations across the read pass come from runtime.MemStats.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, single thread. Two runs, first shown; throughput spread was 10 to 25 percent between runs (these are memory-bandwidth cells on a shared laptop LLC), but the ordering and the window-cost column were stable.

| value | chunk | write GB/s | read GB/s | 100B window us | dir B/value | read allocs |
|---|---|---|---|---|---|---|
| 64KiB | 16KiB | 18.5 | 30.8 | 1.3 | 64 | 0 |
| 64KiB | whole | 27.8 | 30.4 | 2.9 | 16 | 0 |
| 256KiB | 16KiB | 18.5 | 41.1 | 1.2 | 256 | 0 |
| 256KiB | 64KiB | 37.0 | 38.7 | 2.2 | 64 | 0 |
| 256KiB | whole | 48.2 | 37.0 | 6.9 | 16 | 0 |
| 1MiB | 16KiB | 24.7 | 41.6 | 1.1 | 1024 | 0 |
| 1MiB | 64KiB | 38.3 | 40.5 | 2.2 | 256 | 0 |
| 1MiB | 256KiB | 38.5 | 37.5 | 9.3 | 64 | 0 |
| 1MiB | whole | 53.0 | 38.5 | 41.5 | 16 | 0 |
| 4MiB | 16KiB | 23.0 | 48.5 | 1.3 | 4096 | 0 |
| 4MiB | 64KiB | 36.5 | 47.5 | 2.2 | 1024 | 0 |
| 4MiB | 256KiB | 50.4 | 49.8 | 6.8 | 256 | 0 |
| 4MiB | 1MiB | 43.3 | 37.1 | 27.2 | 64 | 0 |
| 4MiB | whole | 45.8 | 36.4 | 113.1 | 16 | 0 |
| 8MiB | 16KiB | 21.6 | 50.5 | 2.1 | 8192 | 0 |
| 8MiB | 64KiB | 35.6 | 44.2 | 2.7 | 2048 | 0 |
| 8MiB | 256KiB | 32.9 | 35.5 | 9.1 | 512 | 0 |
| 8MiB | 1MiB | 38.4 | 32.4 | 38.5 | 128 | 0 |
| 8MiB | whole | 43.9 | 25.4 | 244.7 | 16 | 0 |

Notes on the shape:

- Writes: throughput rises with chunk size because per-chunk dispatch amortizes away. 16KiB chunks pay roughly half the whole-run write rate; 64KiB chunks recover to 70 to 80 percent of it and the remaining gap keeps shrinking upward. The cost of chunking a full write is real but bounded and flat in value size.
- Reads: chunk size barely matters. Streaming through one reusable 16KiB or 64KiB buffer reads as fast as (at 4MiB and 8MiB, faster than) walking one contiguous multi-megabyte run, because the reused buffer stays cache-resident while the whole-run walk streams cold. The F19 streaming shape costs nothing on this axis.
- The windowed write is the decisive column. The COW cost is one chunk plus a directory republish: 1 to 2us at 16KiB, 2 to 3us at 64KiB, 7 to 9us at 256KiB, 27 to 39us at 1MiB, and the whole-run row grows linearly with the value, 42us at 1MiB up to 245us at 8MiB and unbounded beyond. That linear row is L11 in miniature and is exactly what the chunk threshold exists to cut off.
- Directory overhead is noise everywhere: 8KiB of directory on an 8MiB value at 16KiB chunks is 0.1 percent. Zero heap allocations on the read path in every cell.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. `str_chunk_size` = 64KiB holds: it keeps the 100B windowed write at 2 to 3us flat across value sizes, gives up 20 to 30 percent of full-write throughput against the unchunked shape (16KiB gives up half), and matches or beats every other size on reads. 16KiB buys another microsecond on windows but pays double on every full write, which is the wrong trade for a band whose gate rows include full-value SET; 256KiB and up let the window cost grow 3 to 15x for no reliable throughput win. `str_chunk_min` = 64KiB is contract-bound (F5) and this sweep shows it costs nothing to honor: a 64KiB value at 64KiB chunks is one chunk plus one directory entry, indistinguishable from the whole-run shape, while the whole-run window cost is already 7us at 256KiB and climbing linearly right above it. The gate box rerun should confirm the write-throughput gap at 64KiB chunks on Zen 4 bandwidth and rerun the window column with the real value log (NVMe-backed cold runs) before the constants freeze.
