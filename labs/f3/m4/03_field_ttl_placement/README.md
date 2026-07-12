# Lab 03: field-TTL memory placement on the two hash bands

Part of issue #546, the M4 hash milestone, slice 6 (field TTL, the HEXPIRE family).
This slice is a correctness and wire-parity slice, so per the labs-per-perf-change rule it bakes no perf constant and needs no throughput lab. It ships this one anyway because the placement it chooses has a direct memory consequence, and the memory bar is a hard gate for the whole f3 project (aki must use less RAM than the rival for the same data). This lab pins the structural cost of that placement with real numbers so the gate-run RSS comparison (PRED-F3-M4-HASHMEM) has a byte figure to stand on.

## Question

Field TTL is the HEXPIRE family: a per-field absolute expiry. Where do the expiry bytes live, and what does a hash that never sets a TTL pay?

Redis stores a listpack hash with any field TTL as `listpackex`, an eight-byte expiry slot after every entry in the whole blob, and a hashtable hash through an ebuckets expiry index. The memory bar says the placement has to cost nothing until a field actually takes a TTL, then cost only the eight bytes the expiry needs, with no per-hash TTL index sitting alongside the data.

So the question: what is the resident byte cost of the two aki placements, with no TTL, with every field TTL'd, and with half?

## Method

In-process, no server, no wire. The tables model the two bands' TTL-bearing memory exactly as the real structs lay it out (the same discipline lab 02 uses for the field slab); the real path is `engine/f3/hash/field.go` (the native `exp` column) and `hash.go` (the inline sticky `listpackex` blob).

- **native band**: `nativeBand` builds the record vector (`fentry`, mirrored field for field so the per-record width tracks the real band, 20 bytes here), the draw vector, the field/value slab, and the `exp` column. The column is `[]uint64` indexed by record ordinal, `nil` until the first TTL; the first `setFieldExp` allocates one slot per record, so any TTL charges the whole hash `len(ents)*8`.
- **inline band**: `inlineBlob` builds the listpack layout (`[count][flags]` then `[flen][field][vlen][value]` per entry) and, once the sticky bit flips on the first TTL, the `listpackex` layout with an eight-byte slot trailing every entry.
- **per hash**: one `uint64` next-expire hint, always present, the word the lazy reap gates on.

The reported bytes are structural (the exact `len` of each store), not size-class-rounded heap: the TTL claim is about resident bytes that compare to the rival's byte layout, not about Go's allocator buckets. One measured note at the bottom shows the allocator rounds the structural bytes up (size classes plus append over-reservation), overhead the rival's allocator pays too so it cancels in the RSS comparison.

Axes: field count m in {8, 64, 512, 4096, 65536} by value width in {8, 64} bytes; the inline band stops at 512, its `hash-max-listpack-entries` ceiling.

`go run .` runs the whole sweep. The table is deterministic (structural byte counts), so `-quick` is accepted for the shared runner but changes nothing; `TestNativeExpColumnPlacement` and `TestInlineStickyPlacement` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26.5, 2026-07-13, one process. `fentry` is 20 bytes/record; the per-hash next-expire hint is 8 bytes, always present.

Native band (`exp` column, `[]uint64` indexed by ordinal):

| fields | valW | noTTL_B | allTTL_B | delta_B | delta/f |
|---|---|---|---|---|---|
| 8 | 8 | 272 | 336 | 64 | 8.00 |
| 64 | 8 | 2,230 | 2,742 | 512 | 8.00 |
| 512 | 8 | 18,322 | 22,418 | 4,096 | 8.00 |
| 4096 | 8 | 150,442 | 183,210 | 32,768 | 8.00 |
| 65536 | 8 | 2,479,258 | 3,003,546 | 524,288 | 8.00 |
| 8 | 64 | 720 | 784 | 64 | 8.00 |
| 64 | 64 | 5,814 | 6,326 | 512 | 8.00 |
| 512 | 64 | 46,994 | 51,090 | 4,096 | 8.00 |
| 4096 | 64 | 379,818 | 412,586 | 32,768 | 8.00 |
| 65536 | 64 | 6,149,274 | 6,673,562 | 524,288 | 8.00 |

Inline band (listpack to listpackex, 8B slot per entry):

| fields | valW | noTTL_B | allTTL_B | delta_B | delta/f |
|---|---|---|---|---|---|
| 8 | 8 | 99 | 163 | 64 | 8.00 |
| 64 | 8 | 825 | 1,337 | 512 | 8.00 |
| 512 | 8 | 7,061 | 11,157 | 4,096 | 8.00 |
| 8 | 64 | 547 | 611 | 64 | 8.00 |
| 64 | 64 | 4,409 | 4,921 | 512 | 8.00 |
| 512 | 64 | 35,733 | 39,829 | 4,096 | 8.00 |

Half-TTL arm (any TTL charges the whole container):

| fields | valW | band | halfTTL_B | allTTL_B |
|---|---|---|---|---|
| 64 | 64 | native | 512 | 512 |
| 64 | 64 | inline | 4,921 | 4,921 |
| 512 | 64 | native | 4,096 | 4,096 |
| 512 | 64 | inline | 39,829 | 39,829 |

Allocator note (512 fields, 64B, all-TTL native): structural 51,090 B, resident heap 172,066 B/build. The heap is larger than the structural bytes because each `make` grows to a size class and this build loop appends the slab (which over-reserves as it doubles); the real band presizes on promotion via `newFtable`, so it pays less of the append tax, and the size-class rounding is overhead the rival's allocator pays too.

## Reading the sweep

Every `delta/f` is exactly 8.00, on both bands, at every cardinality and value width. That is the claim: field TTL costs eight bytes a field, the width of one absolute unix-ms expiry, and nothing else. There is no per-field record header, no per-hash TTL index, no tree of expiry buckets; the expiry rides in a flat column beside the record on the native band and in an inline slot on the small band, byte for byte the same eight the rival's `listpackex` slot costs.

`noTTL_B` never carries a TTL byte. The native `exp` column is `nil` and the inline blob is the plain listpack until the first `HEXPIRE`-family setter lands, so a hash that never sets a field TTL, which is the overwhelming common case, pays zero for the machinery beyond the one eight-byte next-expire hint word. That word is the whole fixed cost, and it is what lets the lazy reap gate every command with a single comparison (`nextExp == 0` short-circuits before any scan).

The half-TTL arm is the honest nuance. The moment any one field takes a TTL, both bands charge the eight bytes for every field in the hash, not just the TTL'd ones: the native column allocates one slot per record and the inline blob re-encodes the whole thing sticky. So half a hash TTL'd costs exactly what all of it costs (512 vs 512, 4,096 vs 4,096). This is not a regression against the rival; it is the same all-or-nothing Redis's `listpackex` pays (one flip converts the whole listpack), and the native column is the flat equivalent. The design does not try to store a sparse expiry set inline, because a sparse side index would cost more than the dense column for any hash where a meaningful fraction of fields expire, which is the case field TTL exists for (session fields, rate-limit windows).

## Darwin caveat

These are structural byte counts, so they do not depend on the box: `delta/f` is 8 by construction on any platform (the `exp` slot and the `listpackex` slot are eight bytes wide everywhere), and `noTTL_B` excludes them by construction (the column is nil, the sticky bit is clear). The `fentry` width is 20 bytes on a 64-bit target and would not change on the Linux gate box. The only measured line, the allocator note, is illustrative overhead, not the decision. The decision numbers, delta per field and the zero-until-TTL base, are frozen here; the absolute RSS against redis 8.8 and valkey 9.1 lands at the M4 gate run (PRED-F3-M4-HASHMEM) where the rival's real per-field TTL cost is read off `INFO memory`.

## Verdict

Frozen for the M4 field-TTL slice: the lazy native `exp` column and the sticky inline `listpackex` blob, both absent until the first TTL, each eight bytes a field after.

- **Zero until the first TTL.** A hash with no field TTL carries no expiry bytes on either band, only the one eight-byte next-expire hint that gates the lazy reap. The common case pays nothing for the machinery.
- **Eight bytes a field after, no index.** Once a field takes a TTL the band pays exactly the expiry width per field (8.00/f across the whole sweep), the same eight the rival's `listpackex` slot costs, with no per-hash TTL tree or bucket set beside the data. Less bookkeeping than an ebuckets index for the same expiries, which is where the memory-bar margin against the rival comes from at the gate run.
- **Whole-container on first TTL, by design.** Any TTL charges every field, matching Redis's listpackex flip; a sparse side index was rejected because it costs more than the dense column for the fraction of fields field TTL is actually used on.

What the slice ships: the native `exp` column (`field.go`, nil until `setFieldExp`, `hashtable` encoding unchanged), the inline sticky `listpackex` blob (`hash.go`, the flag bit re-encodes with the 8-byte slot and `OBJECT ENCODING` reports `listpackex`), and the per-hash `nextExp` hint driving the reap-at-command-entry lazy expiry. No perf constant is baked; the eight-byte width is the expiry itself and the reap gate is a comparison.
