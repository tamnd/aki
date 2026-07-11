# Lab 01: counted B+ tree node size

Part of issue #544, the M2 zset milestone.
This lab lands before the counted-tree slice so the slice bakes a settled node size, not a guess.

## Question

Doc 12 (12-zset-model.md) replaces v1's order-statistic skiplist with an owner-local counted B+ tree per key: keyed by (score, member), subtree counts in every interior node, leaves singly linked for range walks, all in the shard arena as offset-addressed bytes (F7).
Section 2.3 sizes every node at 256 bytes, four cache lines, giving arity 16 and a 15-entry leaf, but says in the same breath that the lab sweeps 128/256/512 before freezing (section 11 exit 1).
This lab runs that sweep and freezes the node size and any leaf-versus-branch asymmetry the numbers ask for.

The bar is PRED-F3-M2-ZSETMEM, which F14 states as 2 to 3 bytes of tree overhead per entry for the native band, the Dragonfly-class figure their counted B+ tree hit when it replaced the skiplist (doc 12 section 2.2).
Section 9 pins that bar precisely: the leaf entry is 16 bytes, of which 8 is the score payload and 8 is bookkeeping, and "amortized node headers, interior nodes and 0.9-fill slack add ~2.5B/entry at arity 16" (section 9).
That 0.9-fill structural term is exactly the bar, and it is exactly what the node size sets, so this lab measures it against the arity choice.
Over 5 bytes per entry blocks the milestone.

## Method

In-process, no server, no wire, no engine import.
The tree here is lab-local code that models the doc's structure so the choice can be priced before the slice writes it.
Two fixed-size arenas hold the nodes: a leaf arena and a branch arena, each a flat byte slice addressed by a 4-byte ordinal, no Go pointers.
Because the two arenas share the ordinal range, the node kind is decided by tree level (level 1 is the leaf level), not by probing a tag byte, which is the arena discipline the slice inherits.
Interior nodes carry a separator, a child ordinal and a 4-byte subtree count per child; leaves carry the 16-byte (score, member-offset) entries after an 8-byte header.

Five arms sweep the size, with the arm named by branch bytes then leaf bytes when they differ:
128 (arity 8, 7-entry leaf), 256 (arity 16, 15-entry leaf, the doc's shape), 512 (arity 32, 31-entry leaf), 256b/512l (arity 16 branch, 31-entry leaf), and 256b/1024l (arity 16 branch, 63-entry leaf).
The last two hold the branch at the doc's 256 bytes and widen only the leaf, so the sweep separates the branch (descent, fanout) axis from the leaf (memory, range walk) axis instead of moving both at once.

Cardinalities are 1k, 10k, 100k, 1M, 4M, spanning L2-resident to well past it.
Reads per arm: point insert ns (ZADD shape), delete ns (ZREM shape), rank ns (ZRANK, the position-of-member descent), select ns (ZRANGE-by-rank, the member-at-rank descent), in-order range-walk ns per emitted entry (ZRANGE, seek once then follow leaf links), and bytes of tree overhead per entry beyond the 16-byte entry.
Two memory columns: bpeBulk is a right-edge bulk load at the doc's 0.9 fill, which is the F14 bar term, and bpeRand is random single-key insertion, which settles at the ln2 ~0.7 steady-state fill that a churned zset holds.
The bulk loader distributes keys evenly across each level so the whole level sits at a true 0.9 fill rather than a run of full nodes and one near-empty tail.

`go run .` runs the whole sweep; `-quick` shrinks the op counts for a fast check.

## What the doc predicts, and what this lab tests

- 256-byte nodes, arity 16, 15-entry leaf (section 2.3). Tested by the sweep; confirmed for the branch, revised for the leaf.
- Tree overhead ~2.5B per entry at arity 16 and 0.9 fill (section 9). Tested by the bpeBulk column; the 256-byte leaf lands above it and a wider leaf is needed to reach it.
- Steady-state leaves average ~0.7 fill under random insertion (section 9). Tested by the bpeRand column, which reproduces ~0.67.
- Descent in a handful of cache-line reads, O(log n) rank and select (section 2.2). Tested by the rank and select ns columns against tree height.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process.
ns columns are nanoseconds per op; the range column is ns per emitted entry.
bpe columns are bytes of tree overhead per entry beyond the 16-byte leaf entry, so they read directly against the 2-3B bar.

Tree overhead per entry, structural and near constant across cardinality:

| arm | arity | leaf cap | bpeBulk (0.9 fill) | bpeRand (0.7 fill) |
|---|---|---|---|---|
| 128 | 8 | 7 | 7.6 | 15.4 |
| 256 | 16 | 15 | 4.4 | 10.6 |
| 512 | 32 | 31 | 3.0 | 8.7 |
| 256b/512l | 16 | 31 | 3.0 | 8.8 |
| 256b/1024l | 16 | 63 | 2.4 | 7.9 |

Descent and walk ns at 1M and 4M, past L2 and DRAM-bound:

| arm | lvl@4M | rankNs 1M/4M | selNs 1M/4M | rngNs 1M/4M |
|---|---|---|---|---|
| 128 | 9 | 794 / 1025 | 566 / 681 | 37.7 / 48.8 |
| 256 | 7 | 679 / 1115 | 344 / 553 | 14.4 / 19.2 |
| 512 | 5 | 604 / 1290 | 246 / 537 | 8.3 / 17.5 |
| 256b/512l | 6 | 718 / 1011 | 295 / 530 | 11.7 / 19.7 |
| 256b/1024l | 6 | 735 / 996 | 303 / 439 | 8.0 / 11.9 |

Write ns at 1M and 4M:

| arm | insNs 1M/4M | delNs 1M/4M |
|---|---|---|
| 128 | 1028 / 995 | 793 / 1078 |
| 256 | 670 / 1096 | 514 / 970 |
| 512 | 640 / 1974 | 654 / 1076 |
| 256b/512l | 707 / 1152 | 646 / 1232 |
| 256b/1024l | 764 / 1134 | 710 / 1360 |

The ns figures wander run to run from cache and TLB noise, so the ordering and the level count are the signal, not the last digit; the bpe columns are structural and stable.

## Reading the sweep

The 128 arm is out on every axis and disqualifies itself first.
Arity 8 builds a 9-level tree at 4M against 5 to 7 levels for the wider arms, so its rank and select pay the most dependent node reads, its range walk is 37 to 49 ns per entry against 8 to 20 for the rest, and its 7.6B bulk overhead is over the 5B block line before random churn even starts.
A small node saves nothing and costs everything here, exactly because the tree amortizes one header and one count array across a node, and a 7-entry node amortizes across almost nothing.

Memory is a leaf-capacity story and the branch barely enters it.
The 512 arm (arity 32) and the 256b/512l arm (arity 16) share a 31-entry leaf and land on the same 3.0B bulk and ~8.7B random, so the interior term is a small fraction of the total and the leaf fill dominates.
This is why the two memory columns split the way they do: bulk load at 0.9 fill leaves ~1.8B of slack per entry against ~7B at the 0.7 random steady state, and that slack is set by fill, not node size.
The doc's own 2.5B figure at arity 16 (section 9) is the bulk term, and the sweep reproduces its shape but not its value at the 256-byte leaf: the 15-entry leaf lands at 4.4B bulk, over the 3B ceiling, because a 15-entry leaf still pays too large a header and interior share.
Widening the leaf to 31 entries (512 bytes) brings it to 3.0B, and to 63 entries (1024 bytes) to 2.4B, which is the first arm inside the bar with margin.

The branch wants to stay at 256 bytes even though memory does not care.
The pure 512 arm (arity 32) reads well on select and range but spikes point insert to 1974 ns at 4M against ~1100 for every 256-byte-branch arm, because an 8-cache-line interior node pays a larger memmove on every split and reads a full 512 bytes per descent step for a separator binary search that only needs five compares.
Holding the branch at 256 bytes (arity 16) and widening only the leaf keeps insert flat and takes the range-walk and select win: 256b/1024l walks range at 8.0 and 11.9 ns against the 256 leaf's 14.4 and 19.2, and selects at 439 ns at 4M against 553, for a ~3 percent slower insert at 4M and ~14 percent slower at 1M from the wider leaf memmove.
Since ZADD already passes its class in v1 and ZRANGE and ZRANK are the open cells the doc is trying to fix (section 0), trading a little insert headroom for the range, select and memory win is the right direction.

## Bytes per entry against the bar

The F14 bar is the 0.9-fill structural term, the bpeBulk column, and the sweep places the arms cleanly against it.
128 at 7.6B is over the 5B block line and out.
256, the doc's shape, is 4.4B, inside the block line but over the 2-3B target.
512 and 256b/512l are 3.0B, at the ceiling.
256b/1024l is 2.4B, inside the target with room.
The random column is the separate 16B/0.7 leaf-slack term that section 9 folds into the ~35B total per member, not the tree bar, so its 8 to 11B is expected and budgeted; a bulk-built or compacted zset sits at the bulk number and a churn-heavy one trends toward the random number, both accounted.
The tree side of the ledger meets the 2-3B bar once the leaf is 512 bytes or wider, and the hash slot at ~18B remains the dominant term the section 9 total is really about, not the tree.

## Darwin caveat

These constants are measured on the darwin/arm64 box because the GamingPC gate box is busy with another campaign.
The node-size decision rests on the structural bpe columns, which are deterministic and platform-independent, on the tree height, which is arithmetic, and on ns orderings wide enough to survive a platform change (the 128 loss and the 512-branch insert spike are both large).
The absolute ns and the exact insert crossover between the 256-byte and 1024-byte leaf get their Linux confirmation at the M2 gate run on GamingPC before the gate rows are read, and that run decides whether the leaf freezes at 512 or moves to 1024.

## Verdict

Frozen for the counted-tree slice:

- Branch node: 256 bytes, arity 16, exactly as doc 12 section 2.3 states. It is the descent sweet spot: four cache lines per interior read, a 16-way fanout that keeps a 4M-member tree at 6 to 7 levels, and stable point insert. 128 (arity 8) is rejected outright: deepest tree, slowest on every op, and 7.6B bulk overhead over the block line. 512-byte branch (arity 32) is rejected: no descent win over 256 and a point-insert spike to 1974 ns at 4M from the 8-cache-line interior memmove.
- Leaf node: widen to 512 bytes (31 entries) as the leaf-versus-branch asymmetry, the smallest leaf that clears the 2-3B bar (3.0B bulk against the 256-byte leaf's 4.4B) while keeping insert flat. The 1024-byte leaf (63 entries) is better on every read axis (2.4B bulk, range walk 8.0/11.9 ns, best select) at a ~14 percent insert cost at 1M, and is the recommended upgrade for the gate box to confirm on Linux before committing past 512.
- Bytes per entry: the tree bar is met at a 512-byte-or-wider leaf; the doc's uniform 256-byte node lands at 4.4B, inside the block line but over the 2-3B target, so the asymmetry is what buys the bar. Memory is set by leaf fill, not branch arity, so the bulk-load path and the merge policy that hold fill up are the real levers, and this sweep confirms the branch arity is free to stay at the cache-friendly 16.

What the slice should bake in: 256-byte interior nodes at arity 16 with 4-byte subtree counts, a 512-byte leaf at 31 entries (with 1024 flagged for the gate box), node kind decided by level not tag, and the right-edge 0.9-fill bulk loader for band promotion and STORE outputs so bulk-built trees start at the bar.
The counted tree built here is the shared package M5's directory and M6's geo reuse (issue #544), so the node size is frozen once against these numbers.

What the fuzz exit (section 11) must assert, surfaced by this lab's own property test: after every insert and delete, every interior count equals the live entry count of its child subtree, separators stay sorted, rank and select agree with a sorted-slice model, and the range walk emits exactly the model window, on all node sizes and down to a drained-empty tree, plus the bulk loader produces a tree that passes the same count and rank checks.
