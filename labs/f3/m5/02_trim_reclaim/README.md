# Lab 02: XTRIM reclaim granularity, whole-block drop vs per-entry tombstone

Part of issue #547, the M5 stream milestone, lab 02, the trim-reclaim decision (doc 14 section 6.6). This is the lab the XTRIM slice depends on: it settles that the block is the right front-reclaim unit before the slice bakes the whole-block drop into the trim path, per the labs-per-perf-change rule.

## Question

A native stream is an append log of entry blocks (doc 14 section 3.2): a sealed block is append-frozen and dropped only whole. XTRIM removes entries from the front, the oldest, and section 6.6 fixes the reclaim unit as the whole block. Approximate (`~`) drops whole front blocks while the result stays at or above the threshold, each drop one directory delete plus the freeing of one block, so removing 10k entries that live in ~80 blocks is ~80 directory deletes, not 10k entry deletes. Exact (`=`) adds tombstoning the overshoot inside the boundary block, paying the deleted-flag writes now and reclaiming the bytes only when that block later empties or gc-rewrites. The design rejects a per-entry front reclaim: a sealed block cannot shrink without a full re-encode, so splicing one entry off the front would re-encode a block on the command path of a point-ish command.

So the questions: does a whole-block drop actually reclaim a removed entry's full resident cost, how much cheaper is it than a per-entry front reclaim, and how much does the approximate mode overshoot the threshold, the memory `~` trades for never re-encoding?

## Method

In-process, no server, no wire, no engine import, the same lab-local model lab 01 uses. Blocks carry real byte blobs encoded as section 3.3 lays out (master whole, same-schema entries as a flags byte, the two ID deltas against the block firstID, and value frames), so a whole-block drop frees real bytes and an exact-mode boundary tombstone flips a real deleted flag by walking the blob. IDs are dense auto-IDs at 1000 entries per millisecond, the benchmark-shaped case. Resident cost counts the 48-byte block header and the 32-byte directory leaf per block plus the entry bytes, matching lab 01, so the reclaim figure is the full footprint a dropped block returns.

The stream carries the base offset of section 6.6: dropping front blocks reslices to a fresh slice and advances `base` (the logical index of `blocks[0]`), so surviving blocks keep their directory references without a renumber. Three strategies trim the same built stream: `trimApprox` (whole-block drops only), `trimExact` (drops plus boundary tombstone), and `trimPerEntry`, the rejected design that tombstones one entry at a time and charges one directory-class operation per entry so the comparison is apples to apples on both reclaim and cost.

`go run .` runs the whole sweep. `-quick` shrinks the op counts for the shared runner. `TestWholeBlockReclaimsFullEntryBytes`, `TestBlockDropOpsBeatPerEntry`, `TestApproxOvershootBoundedByOneBlock`, `TestExactReachesThreshold`, and `TestBaseOffsetKeepsDirectoryRefsValid` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-13, one process. 4096/128 block geometry, 3x8B fixed-schema entries (24-byte payload), dense IDs 1000/ms.

Sweep A, reclaim per strategy over keep fractions (B/e is bytes reclaimed immediately per entry removed):

| keep% | removed | blocks | ~reclB/e | =reclB/e | peReclB/e | ~over |
|---|---|---|---|---|---|---|
| 90 | 20000 | 156 | 31.36 | 31.31 | 0.00 | 32 |
| 50 | 100000 | 781 | 31.36 | 31.35 | 0.00 | 32 |
| 10 | 180000 | 1406 | 31.36 | 31.35 | 0.00 | 32 |
| 1 | 198000 | 1546 | 31.36 | 31.34 | 0.00 | 112 |

A removed entry costs 24 payload + 7.36 overhead (lab 01) = ~31.36 resident bytes; the whole-block drop frees all of it. The per-entry tombstone frees 0.00: the sealed block keeps every byte, only the flag flips.

Sweep B, directory operations to keep 10 percent, O(blocks) vs O(entries):

| entries | removed | blkDrops | peOps | opsRatio |
|---|---|---|---|---|
| 10000 | 8960 | 70 | 9000 | 128.6 |
| 100000 | 89984 | 703 | 90000 | 128.0 |
| 1000000 | 899968 | 7031 | 900000 | 128.0 |

The per-entry cost is the entries-per-block multiple, ~128 at the default geometry: a million-entry trim is ~7k block drops, not ~900k entry deletes.

Sweep C, approximate overshoot vs a 1000-entry threshold, by entry cap:

| cap | ent/blk | left | over% |
|---|---|---|---|
| 32 | 32.0 | 1024 | 2.4 |
| 64 | 64.0 | 1024 | 2.4 |
| 128 | 128.0 | 1088 | 8.8 |
| 256 | 130.9 | 1081 | 8.1 |

The overshoot is bounded by one block: at most the boundary block's live entries stay above the threshold, so a larger block overshoots a small threshold more. Against a threshold in the thousands or above, the target XTRIM case, one block of overshoot is a low-single-digit percent.

Sweep D, trim latency to keep 10 percent:

| entries | approxNs | exactNs |
|---|---|---|
| 10000 | 313 | 713 |
| 100000 | 3779 | 3404 |
| 1000000 | 40308 | 35675 |

Both are dominated by the O(blocks) drop and its slice copy; exact adds a bounded boundary tombstone that decodes only the overshoot entries, so at scale the two converge (the difference falls into timing noise). Trimming a million entries takes tens of microseconds, LIMIT-cappable when even that must not monopolize a batch.

## Verdict

The block is the front-reclaim unit. Whole-block drop frees a removed entry's entire ~31 B resident footprint at O(blocks) directory work; the rejected per-entry tombstone frees nothing until a block empties and costs O(entries), ~128x more operations. So:

- XTRIM `~` and the XADD `~` clause drop whole front blocks, one directory delete each, and reclaim immediately. This is the memory-efficient default and matches Redis's `~` whole-node semantics.
- XTRIM `=` adds the boundary tombstone for an exact count, a bounded add over the same drops; its overshoot bytes are reclaimed later when the boundary block empties or the section 6.5 gc rewrite (its own lab) rewrites it.
- Approximate overshoot is bounded by one block, a low-single-digit percent of any threshold a real trim targets, the price paid to never re-encode a sealed block on the command path.

The gc-ratio rewrite of a partially-tombstoned block (section 6.5, `stream-block-gc-ratio` default 0.5) is orthogonal to this decision and lands with its own lab: it reclaims the bytes the exact-mode boundary tombstones leave, on the owner's background step, not on the trim path this lab prices.
