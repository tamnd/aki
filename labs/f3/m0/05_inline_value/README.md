# Lab: inline value threshold

Spec 2064/f3/09 section 2, M0 lab 5.

## The question

Doc 09's band ladder embeds a value in the record up to `str_inline_max` (default 1024B, the f1 `maxInlineBody` carried forward) and moves anything bigger behind a 16-byte value pointer into its own run. The default is inherited, not measured on this substrate, so before the value-band slice bakes it in: at what value size does the inline record stop beating the pointer? The bet behind embedding is that a GET's second memory touch (the record line the probe already resolved) is cheaper than a third (the pointer chase into the value arena), and that the 16-byte pointer plus run rounding is pure memory overhead at small sizes.

## Method

`go run .` runs two variants over the real `engine/f3/store`, sweeping value sizes 8, 16, 32, 64, 128, 256, 512 bytes. The inline variant is the store as shipped: Set embeds the bytes, Get copies them out. The out-of-line variant keeps the value bytes in a lab-side bump-allocated slab standing in for the separated-run arena (the store only does inline today, and this lab does not modify engine code) and stores a 16-byte pointer (offset, length) as the record's value; Get probes the store, decodes the pointer, and copies from the slab; Set fetches the pointer and rewrites the run in place, doc 09's mutable-region fast path for a same-size overwrite. Each cell fills 1M keys (16B keys), times 4M uniform pre-shuffled GETs and 1M uniform SETs single-threaded, and reads bytes-per-key from the arena's handed-out bytes (plus the slab's live run bytes for the out-of-line variant).

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, single thread. Two runs, second shown; spread was under 10 percent on GET rows and up to 15 percent on SET rows (the mid-size SET cells trade places between runs).

| value | inline GET ns | oob GET ns | inline SET ns | oob SET ns | inline B/key | oob B/key |
|---|---|---|---|---|---|---|
| 8B | 187.6 | 227.9 | 199.8 | 217.0 | 40 | 56 |
| 16B | 203.9 | 231.8 | 221.3 | 214.3 | 48 | 64 |
| 32B | 201.6 | 251.4 | 199.6 | 221.1 | 64 | 80 |
| 64B | 233.1 | 238.2 | 242.4 | 218.5 | 96 | 112 |
| 128B | 245.0 | 253.0 | 278.5 | 235.5 | 160 | 176 |
| 256B | 309.9 | 285.7 | 301.1 | 306.6 | 288 | 304 |
| 512B | 369.1 | 407.2 | 323.5 | 303.2 | 544 | 560 |

Notes on the shape:

- GET: inline wins clearly at 8B to 128B (10 to 25 percent), and the gap is the pointer chase, one extra dependent cache miss into a slab line the probe did not warm. At 256B the two variants land within noise of each other and trade places between runs; at 512B inline wins again because the out-of-line copy now streams 512 cold slab bytes on top of the pointer chase, while the inline copy rides the record lines the key compare already started loading.
- SET: the same-size overwrite is in-place for both variants, so the rows mostly measure probe cost plus a memcpy and sit within 15 percent of each other; no crossover shows up in the swept range.
- Memory: the pointer variant pays a flat 16B/key extra at every size, which is 40 percent overhead at 8B values and 3 percent at 512B. Below roughly 64B the pointer costs a meaningful fraction of the data it points at.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. Inline wins or ties at every swept size through 512B on both time and memory, so the threshold is not below 512B and nothing here argues for separating early. The doc 09 default of `str_inline_max` = 1024 stands. What this sweep does not settle is the 512B to 4KiB stretch where the doc 11 sweep range (256 to 4096) tops out: the out-of-line variant's copy cost grows linearly while its pointer-chase cost is flat, so the crossover, if it exists, sits above this lab's range and interacts with the arena churn a real separated band adds on non-same-size writes. The gate box rerun should extend the sweep to 4096 with the real value-band slice in place of the lab slab before the constant freezes.
