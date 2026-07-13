# Lab 03: PEL slab layout, one shared record with an owner field vs the dual-PEL duplicate

Part of issue #547, the M5 stream milestone, lab 03, the pending-entries-list layout decision (doc 14 section 7.4). This is the lab the consumer-group delivery slice depends on: it prices the PEL index geometry before the slice bakes it into `pel.go`, per the labs-per-perf-change rule. It is the lab that overrode the spec: the measured numbers rejected the hash the spec called for, so the shipped PEL is tree-only.

## Question

A consumer group's pending-entries list (doc 14 section 7.4) records the entries the group delivered but has not yet acknowledged, owner-local, hung off the group header. Each pending entry is a 32-byte slab record: the 16-byte ID, an 8-byte delivery time (the idle clock), a 2-byte delivery count (RETRYCOUNT), and a 2/4-byte consumer ordinal, the owner. The access pattern wants two shapes: point ops by ID (XACK, the XCLAIM owner lookup) and ID-range scans (XPENDING, XAUTOCLAIM). Section 7.4 spec'd a Go map hash id->ordinal beside a counted tree over the shared slab, the hash for O(1) point ops and the tree for the range scans, with the owner in the slab so an ack never needs a second id-keyed index.

Redis instead stores each pending entry twice: a `streamNACK` in the group PEL rax and the same NACK pointer in the owning consumer's PEL rax, both keyed by the 16-byte ID. That buys range-by-consumer (XPENDING with a consumer, XAUTOCLAIM) with no owner filter, at the cost of a second id-keyed ordered structure per pending and a two-tree acknowledge.

The memory bar PRED-F3-M5-STREAMMEM, and the standing rule that aki holds the same data in less RAM than the rivals, turns on this choice. So the questions: what does the hash actually cost per pending over a shared slab, does it pay for itself against the tree the design already carries, and is Redis's dual PEL worth its second tree? This lab measures the three layouts head to head before the slice commits one.

## Method

In-process, over the real engine counted tree (`engine/f3/struct`), so the per-pending tree and hash cost is measured, not modeled. Three arms share the identical 32-byte slab arena, addressed by ordinal, so the record cost is held fixed and only the index geometry moves:

- **A** shared slab + counted tree + hash: the spec's layout, O(1) point via the map, O(log p) range via the tree.
- **B** shared slab + counted tree only: drop the hash, point ops go through the tree's delete, which already returns the slab ordinal.
- **C** shared slab + group tree + per-consumer tree: Redis's dual PEL, the id key in two ordered structures.

Resident cost is a live-heap delta: build N pending, force GC, read `HeapAlloc` over the ids-only floor. Each arm is measured in its own subprocess (`-mem`), because in one process a prior arm's freed structures leave residue GC has not returned to the OS accounting, which reads a later arm below its own slab floor; one arm per process removes the contamination. The point-op and range timings run on the built structures in the parent process. Ownership is spread round-robin over 8 consumers, the shape XPENDING-by-consumer scans.

`go run .` runs the whole sweep. `-quick` shrinks the pending counts for the shared runner. IDs are dense auto-IDs at 1000 per millisecond. `TestArmsAgreeOnAck`, `TestTreeOnlyServesPointAck`, `TestWalkOwnerMatches`, and `TestSlabReuse` are what CI drives: they hold the equivalence that makes dropping the hash safe, since the resident-byte and ns/op figures the verdict rests on are measured, not asserted.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13. Dense IDs 1000/ms, ownership round-robin over 8 consumers.

Sweep A, resident bytes per pending, each arm isolated over the ids-only floor. The slab column is the 32-byte record floor (with append slack), the denominator the index columns add to:

| pending | slab | A slab+tree+hash | B slab+tree | C dual tree |
|---|---|---|---|---|
| 100000 | 32.0 | 107.2 | 72.2 | 111.5 |
| 1000000 | 32.0 | 131.9 | 76.1 | 114.0 |

The counted tree is not free: over the shared slab it adds ~44 B/pending, so B is 76 B at a million, the 32-byte slab plus the tree's leaves, its branch separators, and the 4-byte reference per entry. The Go map hash adds ~56 B/pending on top of that, so A is 132 B, the largest index of the three. Redis's second tree adds ~38 B/pending, so C is 114 B, less than the hash, because a second dense counted tree packs tighter than a Go map, but still ~38 B over the single tree B carries.

Sweep B, XACK ns/op, the point retire, whole set acked:

| layout | ns/op |
|---|---|
| A hash | 177 |
| B tree | 137 |
| C dual tree | 266 |

B is fastest. The tree delete alone locates the record and returns its ordinal, so B pays one descent. A pays a map probe on top of that same descent, since it deletes from the tree too, so it is ~1.3x B. C deletes from two trees, ~1.9x B. The hash does not replace the tree delete, it adds to it, because the tree still has to drop the entry to keep the range scan correct.

Sweep C, XPENDING-by-consumer ns per owned entry, one consumer's full list of 12500:

| layout | ns/owned-entry | |
|---|---|---|
| A/B filter | 16.5 | shared tree, owner-filtered walk |
| C direct | 2.3 | per-consumer tree, no filter |

This is the one path the dual PEL wins, and it wins it clearly. The per-consumer tree walk touches only the ~12500 entries the consumer owns, while the shared-tree walk scans all 100000 and skips the 7 in 8 it does not own. The skip is a single slab-field compare, but at an 8-to-1 ownership ratio that is ~7x the work, so C leads ~7x here. This is XPENDING-with-a-consumer and XAUTOCLAIM, the introspection and claim paths, not the hot ack.

## Verdict

The PEL ships as arm B: one counted tree over the shared slab, the owner in the slab record, no hash. The spec's hash (arm A) is rejected on memory first:

- It adds ~56 B/pending of pure map overhead, so A is 132 B against B's 76 B, the largest index of the three, for point ops the tree already serves. Against the memory bar, where the product pitch is holding the same data in less RAM, that is the wrong default.
- It does not even buy speed on the ack it exists for. The tree delete must run regardless to keep the range scan correct, so the map probe is added work on top of the same descent, and A is ~1.3x slower than B on XACK, not faster.

Redis's dual PEL (arm C) is rejected too. It costs ~38 B/pending for a second tree and a two-tree ack (~1.9x B on XACK) to win only the by-consumer scan (~7x on XPENDING-with-a-consumer and XAUTOCLAIM). The common unfiltered XPENDING is identical to B, the hot ack is nearly 2x worse, and the memory stays ~38 B/pending higher. The by-consumer scan on the shared tree is O(scanned) not O(owned), 16 ns per owned entry with a single field compare per skip, fast enough in absolute terms for the introspection and claim paths. Keeping the PEL to one tree is the memory-minimal choice and the one that holds the bar.

So the shipped layout: a 32-byte slab arena addressed by ordinal, one counted tree keyed (id.ms score, id.seq member) with the slab ordinal as the stored reference, and the owner carried in the slab so an ack reads it from the record the tree delete returned and a claim (a later slice) rewrites it in place. This supersedes doc 14 section 7.4's hash-plus-tree; the spec doc is to be reconciled to the tree-only PEL this lab froze.
