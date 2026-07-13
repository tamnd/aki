# Lab 02: bit-kernel throughput, word-at-a-time vs byte-at-a-time

Part of issue #548, the M6 bitmap milestone, lab 02, the BITCOUNT/BITPOS kernel decision (doc 15 section 3). This is the lab the bit-kernel slice ships with, per the labs-per-perf-change rule: it prices the word-at-a-time kernel BITCOUNT and BITPOS run on against the byte-at-a-time form, so the algorithm choice is measured, not asserted.

## Question

BITCOUNT and BITPOS scan a byte range. The naive form works a byte at a time: one OnesCount8 per byte for the count, one non-zero test per byte for the position. The kernel the slice ships reads eight bytes at a time as a 64-bit word: BITCOUNT folds each word with one OnesCount64 (a single POPCNT), eight chains over four accumulators so the core's one POPCNT port is not the ceiling; BITPOS tests a whole word against zero and skips eight bytes at once over a clear run. Go has no stable SIMD, so this word-at-a-time math is the whole lever.

The questions: how much does the word form win on the command-path sliver and the in-LLC range where the arithmetic dominates, and how much of that edge survives once the range is large. Doc 15 section 3 splits BITCOUNT into three regimes; this lab prices the aki word-vs-byte gap in each.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. `popcountWord8` and `firstSetWord` are byte-for-byte the kernel inner loops in `engine/f3/store/bitkernel.go`; `popcountNaive` and `firstSetNaive` are the per-byte forms. The sweep times each over three size bands, a 64-byte command-path sliver, a 256 KiB in-LLC range, and a 64 MiB DRAM range, with a fixed iteration budget and a live sink so the loop cannot fold away, and reports ns/op, GiB/s, and the speedup.

`go run .` runs the whole sweep; `-quick` cuts the iteration counts for the shared runner. `main_test.go` carries `TestPopcountFormsAgree` and `TestFirstSetFormsAgree` (the word and byte forms must agree bit for bit over every length class and fill, so the throughput compares one answer) plus `BenchmarkPopcount{Naive,Word8}{Small,LLC,DRAM}` for a hand run. CI drives the tests.

Note the scope: this lab measures aki's own word kernel against aki's own byte form, both scalar Go. It does not measure aki against a rival's AVX2 POPCNT. The DRAM-band tie against a rival that doc 15 section 3 predicts is the aki-vs-rival regime on the gate box (i9-13900K, AVX-512 fused off, rivals on AVX2), a different comparison settled there, not here.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process, `-quick`.

BITCOUNT, dense buffer (0x55 fill), count all set bits:

| band | naive ns | word8 ns | naive GiB/s | word8 GiB/s | speedup |
|---|---|---|---|---|---|
| small 64B (command path) | 27.0 | 5.0 | 2.21 | 11.92 | 5.40x |
| LLC 256KiB | 71705.0 | 12537.0 | 3.40 | 19.47 | 5.72x |
| DRAM 64MiB | 18201715.0 | 3224069.0 | 3.43 | 19.39 | 5.65x |

BITPOS, sparse buffer (one set bit near the end), clear-run skip:

| band | naive ns | word ns | naive GiB/s | word GiB/s | speedup |
|---|---|---|---|---|---|
| small 64B (command path) | 17.0 | 6.0 | 3.51 | 9.93 | 2.83x |
| LLC 256KiB | 65529.0 | 16400.0 | 3.73 | 14.89 | 4.00x |
| DRAM 64MiB | 18710270.0 | 4748486.0 | 3.34 | 13.16 | 3.94x |

The word kernel wins in every band. The edge does not fade at 64 MiB, because neither scalar loop saturates the M4 memory bus: the byte form tops out near 3.4 GiB/s (POPCNT-port-bound, not memory-bound) and the word form near 19 GiB/s, both well under the box's bandwidth, so both stay compute-bound and the eight-way POPCNT keeps its ~5.6x for the count and ~3.9x for the position scan across the whole sweep. BITPOS's edge is smaller than BITCOUNT's because the clear-run skip does one compare per word against OnesCount64's one POPCNT plus fold per word, but the word form still nearly quadruples the byte form on a sparse bitmap.

## Verdict

The word-at-a-time kernel is the right inner loop for BITCOUNT and BITPOS: 5.4x to 5.7x over the byte form for the count and 2.8x to 4.0x for the position scan on this box, holding across the command-path, LLC, and DRAM bands because a scalar POPCNT loop is compute-bound well below the memory bus. The eight-way unroll over four accumulators is what keeps the count near 19 GiB/s where a single-chain POPCNT would stall on its one port.

This settles only the aki word-vs-byte choice. The three-regime aki-vs-rival story, where the small band wins on the command path, the LLC band trades scalar POPCNT against a rival's AVX2, and the DRAM band ties on the memory bus, is the gate-box comparison in doc 15 section 3 and lands with the gate run, not this lab.
