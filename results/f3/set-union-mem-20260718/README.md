# Set struct union: peak VmHWM A/B on the gate box (2026-07-18)

PR #1156 unioned the set's two mutually exclusive inline representation fields
(`ints []int64` and `blob []byte`) into a single `data []byte`, dropping the set
struct from 104 to 80 bytes and crossing the Go allocator size class from 112 to
80. Lab `labs/f3/m1/12_struct_union_footprint` measured the effect in process at
a flat 32 B/set. This run confirms the effect end to end on the gate box, in peak
VmHWM under the real workload, which is the number the memory bar gates on.

The question the run answers: does a 32 B/set struct saving actually surface in
peak VmHWM, or is it masked by the connection-fabric and GC-headroom peak that
dominates the process high water mark? If struct slimming does not move VmHWM,
the arc is not worth continuing; if it does, it is a real lever.

## Protocol

Same-box A/B, two f3srv binaries built on the GamingPC under WSL2 (32 logical
cores, Go 1.26.0):

- `f3srv-preunion` at `7a8e866`, the commit before the union.
- `f3srv` at `7cf13d3` (PR #1156 on main), the union.

Each binary is launched pinned to cores 4-17, `GOMAXPROCS=14 -shards 8 -arena-mib
512 -batch-data-cap 1024 -reply-ring 128 -free-list-cap 8 -net goroutine`, the M0
gate's server flags. The load is 1M distinct single-member sets, `SADD
set:__rand_int__ hello` with `-r 1000000 -n 5000000 -c 50` from a generator pinned
to cores 18-31, so about 993k of the 1M keyspace is populated (the rest is
birthday collisions in the random draw). Peak VmHWM is read from
`/proc/<pid>/status` after the load settles. Three reps per binary, median
reported.

This is the single-member cell that fails the memory bar worst (the
collection-point-read gate held 1M single-member sets at about 4x the rivals'
peak), because there is no member data to amortize the fixed per-collection cost
against, so the struct's own size class is nearly the whole footprint. The load
runs at c50, not the gate's c512, so the connection fabric is small and the
per-collection struct delta shows cleanly rather than buried under the fabric
peak; the struct delta is per-collection and conn-count independent, so it
carries to c512 unchanged.

## Result

Peak VmHWM, 1M single-member sets, three reps each.

| binary | rep1 | rep2 | rep3 | median |
|---|---|---|---|---|
| preunion (7a8e866) | 231892 | 238308 | 238172 | 238172 kB |
| union (7cf13d3) | 192604 | 191980 | 198500 | 192604 kB |

- Median drop: 238172 - 192604 = **45568 kB, about 44.5 MB**.
- Conservative floor (leanest preunion vs fattest union): 231892 - 198500 =
  33392 kB, about **32.6 MB**, which lands exactly on the lab's deterministic 32
  B/set (32.6 MB for 993k sets).

The saving surfaces in peak VmHWM, and the median is larger than the 32 MB the
struct shrink alone accounts for: the smaller struct also cuts the live heap the
Go GC keeps headroom over, so the 32 MB of live-data reduction pulls another 10
MB or so of GC headroom down with it. That is the amplification the lab could not
see and the box does.

`used_memory` read 65824 bytes on both binaries, the known reporting gap: the
string store's `UsedMemory` ledger does not count the collection registries, so
the server's own accounting omits the member data entirely (an M9 item, tracked
separately). VmHWM is the true figure and the one the bar gates on.

## Verdict

The set struct union drops peak VmHWM about 44.5 MB (median, floor 32.6 MB
matching the lab) on 1M single-member sets, confirmed on the gate box, median of
three. Struct and allocation slimming is a real memory lever: it moves the
process peak and is not masked by the connection fabric at this scale. The direction
the arc committed to is validated.

It does not close the gap alone. The union binary still holds the 1M sets at
192.6 MB against the rivals' roughly 92 MB, about 2x, because the per-collection
footprint is the sum of the struct (now slimmed), the registry map entry, the
`*set` heap indirection, the arena record, and the shared conn-fabric and
GC-headroom peak. The struct was the first and cleanest of those terms to move,
and it is now moved for every set; the remaining terms are the registry map and
indirection (per-collection, addressable by allocation shape) and the conn-fabric
floor (structural goroutine-per-conn, shared with the M0 string gate). The set
struct is the only one of the three collection structs with the dual-header
overhead: hash and zset already sit at 64 bytes with a single inline blob each,
so this union has no counterpart to replicate to them.
