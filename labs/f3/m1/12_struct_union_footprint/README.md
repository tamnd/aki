# M1 lab 12: the set struct union footprint

The per-collection memory-bar slim (spec 2064/f3 doc 11 section 3). A set holds
exactly one of two mutually exclusive inline representations at a time, the
intset-class sorted `[]int64` or the listpack-class packed `[]byte`. The original
struct carried a separate field for each, so every set paid a dead 24-byte slice
header for the shape it was not, whatever its own shape. Unioning the two into a
single `data []byte` (the intset now packs its members as sorted little-endian
int64 lanes, the listpack keeps its byte format) removes one slice header.

The struct size drops from 104 to 80 bytes. The number that moves the memory bar
is not that size, it is the heap size class the Go allocator rounds the struct up
to: 104 bytes rounds to the 112 class, 80 bytes lands exactly on the 80 class.
This lab prices the real per-set heap footprint of both shapes so the class
crossover is measured, not inferred.

## Why the single-member set

The single-member set is the exact cell that fails the memory bar worst. The
collection-point-read gate (2026-07-18) held 1M single-member sets at about 4x
the rivals' peak, because there is no member data to amortize the fixed per-set
cost against, so the struct's own size class is nearly the whole footprint. This
lab weighs that cell: `count` single-member listpack sets (`SADD set hello`), old
struct shape versus unioned shape, `HeapAlloc` delta per set with GC pinned so
the figure is the live structs and their backing blobs alone.

## Sweep

```
go run ./labs/f3/m1/12_struct_union_footprint
```

single-member listpack sets, member "hello"
sizeof oldSet = 104 bytes, sizeof newSet = 80 bytes

|     count | old B/set | new B/set | saved | saved (count) |
|---|---|---|---|---|
|   100000 | 128.0 | 96.0 | 32.0 |  3.1 MB |
|   500000 | 128.0 | 96.0 | 32.0 | 15.3 MB |
|  1000000 | 128.0 | 96.0 | 32.0 | 30.5 MB |
|  2000000 | 128.0 | 96.0 | 32.0 | 61.0 MB |

The allocator charges 128 B/set before the union and 96 B/set after, flat across
the sweep. Each figure decomposes cleanly: the old 128 is the 112 struct class
plus the 16-byte class the 7-byte listpack blob rounds up to; the new 96 is the
80 struct class plus the same 16-byte blob. The entire 32-byte saving is the
struct crossing 112 to 80. The backing blob is untouched, as it must be, since
the union changes the struct's field set, not the member data.

## Verdict

The union returns exactly 32 bytes to every set, whatever its shape, which is
30.5 MB per 1M sets. On the single-member cell that fails the bar worst that is a
25 percent cut to the struct's own class (112 to 80) and a 25 percent cut to the
whole 128 B/set charge. It does not close the 4x gap alone, that gap is the sum
of the struct, the registry map entry, the `*set` indirection, and the
connection-fabric peak, but it is the first and cleanest of those terms to move,
it moves for every set at once, and it costs nothing on the hot path: the intset
lane helpers answer the same binary search and shift-insert the `[]int64` did,
and the gate's string-member sets are listpack, which never touch the intset
path. The same union carries to the hash and zset structs next, where the dead
representation headers are the same shape.
