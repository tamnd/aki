# Lab: APPEND growth multiplier

Spec 2064/f3/09 section 2, M0 lab 7.

## The question

When an APPEND outgrows a record's reserved capacity the write republishes: fresh arena record, full copy, entry repoint, old bytes charged dead. Doc 09 doubles the embedded band toward `str_inline_max` and grows separated runs at 1.5x; SDS grows at 2x then fixed 1MB steps. The multiplier is a straight trade: a bigger factor means fewer republishes (less copying, less arena churn) but more reserved slack, and F14 charges every reserved byte forever. Before the APPEND slice bakes the constant in: what do 1.25x, 1.5x, 2.0x, and a fixed 4KiB step actually cost on this substrate?

## Method

`go run .` runs appends against the real `engine/f3/store`. Every growth republishes for real: a Set with the value padded to the new reserved capacity, which is the engine's actual republish path (the padding means the copy is the new capacity rather than the new length, a small overstatement of copy cost that is largest for the fixed step and noted below). Appends that fit the reservation are the engine's in-place tail write, emulated as the tail memcpy, identical for every policy. The engine is not modified. Two workloads: a one-key build of 16B appends growing a single key to 63KiB (just under the store's value width) over 400 rounds, and a mixed run of 10k keys with appends of 1 to 256B growing each key to its own target between 256B and 8KiB, ops interleaved in random key order, the identical op sequence replayed for every policy. Write amplification is physical bytes copied per logical byte appended; peak overallocation is reserved capacity over logical bytes, the F14 exposure; arena churn is what the arena handed out (live plus dead) per logical byte.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, single thread. Two runs, second shown; the one-key ns column moved up to 2x between runs (the tail memcpy rounds are sub-2ns and cache-state sensitive) but its ordering held, and every other column is deterministic replay and did not move.

One-key build, 16B appends to 63KiB, 400 rounds:

| policy | ns/append | republishes/round | write amp | peak cap/len |
|---|---|---|---|---|
| x1.25 | 2.6 | 35.0 | 6.78 | 1.25x |
| x1.5 | 1.9 | 21.0 | 4.34 | 1.50x |
| x2.0 | 1.5 | 13.0 | 3.03 | 2.00x |
| +4KiB | 2.9 | 17.0 | 9.64 | 128.50x |

Mixed, 10k keys, targets 256B to 8KiB (40MiB logical), 335803 appends of 1 to 256B:

| policy | ns/append | republishes/key | write amp | peak slack | arena churn |
|---|---|---|---|---|---|
| x1.25 | 135.7 | 12.45 | 6.21 | 1.12x | 5.31x |
| x1.5 | 109.6 | 8.57 | 4.57 | 1.23x | 3.64x |
| x2.0 | 70.2 | 6.06 | 3.83 | 1.45x | 2.88x |
| +4KiB | 66.5 | 2.50 | 3.01 | 8.39x | 2.03x |

Notes on the shape:

- The factor ladder behaves exactly as the arithmetic says it should: geometric growth at factor f costs about f/(f-1) copies of the final value, so 1.25x pays 60 percent more copying and 45 to 90 percent more time than 1.5x to shave 10 to 30 points of slack, and 2.0x saves another third of the copying by carrying up to 2x live slack.
- The fixed step is the fastest and cheapest on churn, and disqualifies itself anyway: its first growth on a 16B value reserves 4KiB, a 128x overallocation, and the mixed run's peak slack of 8.39x is the whole keyspace sitting on step-rounded reservations while values are small. A step policy only makes sense once values are large relative to the step, which is why SDS switches to steps at 1MB rather than starting there.
- The fixed step's write amp (9.64 one-key) is inflated by the padded-copy emulation, since a republish here copies the 4KiB-rounded capacity where the engine would copy the length; the factor rows overstate by at most their own factor margin. The republish counts and slack columns are exact.
- Mixed ns/append is dominated by republish cost (each one is a real store Set of kilobytes), which is why its spread across policies is wider than the one-key column where 16B tail writes drown everything.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. 1.5x holds as the growth multiplier for separated runs: it sits at the knee of the curve, within 1.5x of doubling on time and copying while keeping peak live slack at 1.23x against doubling's 1.45x, and F14 makes that slack column the binding one. 1.25x buys too little slack for what it pays, and the fixed step is rejected as a general policy on the 128x small-value reservation alone. Doubling stays right for the embedded band because its slack is capped by `str_inline_max` at 1KiB per key, exactly the doc 09 split: double while small and bounded, 1.5x once the value is its own run. The gate box should rerun the mixed workload with the real value-band republish (copy length, not padded capacity) once the separated band exists, and confirm the 1.5x-vs-2.0x gap survives the arena compaction charge that dead republish bytes eventually cost there.
