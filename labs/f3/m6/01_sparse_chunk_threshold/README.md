# Lab 01: sparse-bitmap resident cost, chunk-directory holes vs a full extent

Part of issue #548, the M6 bitmap milestone, lab 01, the sparse-chunk decision (doc 15 section 2.3). This is the lab the sparse-chunk slice depends on: it settles that an all-zero chunk should stay a directory hole before the slice bakes hole entries into the chunked band, per the labs-per-perf-change rule and the M6 memory bar (aki holds less RAM than the rivals, ideally half).

## Question

A bitmap in aki rides the chunked string band (doc 09). A value at or past the 64 KiB chunk threshold splits into chunks located by a directory of 16-byte run pointers, one per chunk, and the record holds one pointer to that directory. Section 2.3 lets an all-zero chunk stay a hole: its directory entry carries the chunk length with a nil run word, and the chunk consumes no run bytes. A SETBIT at a high offset then stores only the chunks that hold a set bit plus the directory that spans the extent.

Redis and Valkey store a bitmap as one contiguous SDS string of the whole extent, so `SETBIT k 4294967295 1` allocates the full 512 MiB. The claim the slice bakes in is that aki's resident cost tracks the live chunks, not the logical extent, so a sparse bitmap uses far less memory. The questions: how far under the rival does a single high bit land, at what fill density do enough chunks fill that holes stop helping (the sparse-chunk threshold), and what does the directory cost when there is nothing to save.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. The chunk geometry (64 KiB chunk, 16-byte directory pointer, a 16-byte arena run header per live chunk) and the record overhead match the store's chunked band, so the resident figures are the store's, not a stand-in. `akiBytes` charges the record, the full directory (one pointer per chunk in the extent, holes included), and one run per live chunk; a hole costs only its pointer. `rivalBytes` is the conservative whole-extent SDS, the extent bytes plus a string header, without leaning on any allocator size-class rounding, which only ever adds to the rival's side. A layout is built from a set of set-bit offsets: the extent is the covering byte of the highest bit, the live chunks are the distinct chunk indices the offsets touch.

`go run .` runs the whole sweep. `-quick` shrinks the density-sweep counts for the shared runner. `TestSingleHighBitIsOneChunk`, `TestGapChunksCostOnlyDirectory`, `TestDensityCrossover`, `TestFullDensityOverheadNegligible`, `TestCrossoverMatchesModel`, and `TestLayoutLiveChunksMatchDistinct` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process. 64 KiB chunk, 16-byte directory pointer, 16-byte run header.

Sweep A, one set bit at a high offset (the `SETBIT k <big> 1` case):

| maxBitOffset | extent | akiBytes | rivalBytes | aki/rival |
|---|---|---|---|---|
| 1048576 | 128.00KiB | 64.11KiB | 128.02KiB | 0.5008 |
| 16777216 | 2.00MiB | 64.58KiB | 2.00MiB | 0.0315 |
| 268435456 | 32.00MiB | 72.08KiB | 32.00MiB | 0.0022 |
| 4294967295 | 512.00MiB | 192.06KiB | 512.00MiB | 0.0004 |

A single high bit holds one 64 KiB chunk plus the directory that spans the extent (8192 pointers = 128 KiB at the offset cap). The rival holds the whole extent. At the offset cap aki is 192 KiB against 512 MiB, 0.0004x.

Sweep B, density crossover at a 512 MiB extent (8192 chunks), rising set-bit counts:

| setBits | liveChk | coverage | akiBytes | aki/rival | verdict |
|---|---|---|---|---|---|
| 1000 | 943 | 11.5% | 59.08MiB | 0.1155 | aki< |
| 10000 | 5800 | 70.8% | 362.71MiB | 0.7084 | aki< |
| 100000 | 8192 | 100.0% | 512.25MiB | 1.0005 | tie/over |
| 1000000 | 8192 | 100.0% | 512.25MiB | 1.0005 | tie/over |
| 4000000 | 8192 | 100.0% | 512.25MiB | 1.0005 | tie/over |

Scattered bits fill distinct chunks by the coupon-collector curve: 1000 bits touch 943 chunks (11.5% coverage, 0.12x), 10000 touch 5800 (70.8%, 0.71x), and by 100000 every chunk holds a bit (100%, break-even plus the directory tax). aki wins across the whole sparse regime and only reaches the rival once the bitmap is effectively dense.

Sweep C, full-density directory overhead (every chunk live):

| extent | chunks | akiBytes | rivalBytes | overhead |
|---|---|---|---|---|
| 1.00MiB | 16 | 1.00MiB | 1.00MiB | 0.052% |
| 16.00MiB | 256 | 16.01MiB | 16.00MiB | 0.049% |
| 256.00MiB | 4096 | 256.13MiB | 256.00MiB | 0.049% |
| 512.00MiB | 8192 | 512.25MiB | 512.00MiB | 0.049% |

When there is nothing to save, the hole scheme costs one directory pointer plus one run header per 64 KiB chunk: 32 bytes over 65536, under a twentieth of a percent, flat with the extent.

Threshold, from the per-chunk model: aki holds under half the rival below 50.0% chunk coverage and crosses break-even at 99.9%.

## Verdict

An all-zero chunk stays a directory hole. The resident cost tracks the live chunks, so:

- A high-offset bitmap, the `SETBIT k <big> 1` pathology Redis and Valkey allocate the full extent for, costs one chunk plus the directory in aki: 192 KiB against 512 MiB at the offset cap, 0.0004x. This is the M6 memory bar met with room to spare.
- The sparse-chunk threshold is 50% chunk coverage for the half-the-rival bar and ~100% for break-even. Below half the chunks touched aki holds under half the memory; a bitmap only reaches the rival's footprint once it is effectively dense, where the directory tax is the only difference.
- That tax is 0.05%, flat with the extent, so the hole scheme never meaningfully costs even when it cannot help.

The bit-kernel throughput of BITCOUNT and BITOP over these chunks (doc 15 sections 3 and 5) is orthogonal to this resident decision and lands with its own lab: this one settles only that the store holds a sparse bitmap in live chunks, not the extent.
