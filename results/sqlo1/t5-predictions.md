# T5 predictions and invariant map, filed before the exit-gate run

Milestone T5 (tamnd/aki#722); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The suite half of the exit gate (the lqueue depth sweep with the redis 8.8 and valkey 9.1 arms, the capped feed, pagination walks, adversarial middle inserts, the per-operator table) waits on the gate box; this note files the two milestone predictions and the L-I1 through L-I6 test map, whose evidence is complete software-side, including the new list kill matrix in cmd/sqlo1crash.

## PRED-SQLO1-T5-QUEUE

Queue p99 flat in depth from 10 to 10^7 at steady LPUSH plus RPOP, with flat meaning the 10^7 point lands within 1.25x of the depth-10 point on p99, and steady queue traffic writes a few node-sized images per drain window regardless of op count.
Reasoning on record: a queue op is an edge amendment, one node rewrite plus the root frame whatever the length (L-I1), and depth only decides which rung the key sits on, not how many records the edge touches.
On the paged rung the edge path is root, one fence page, one node, and the one-page cache pinned by l.pi keeps queue-shaped traffic on the same page across ops, so the added record read amortizes away rather than scaling with depth.
The lnode lab billed the queue mix at 1.05 frames and ~2.2 KB per op with WA 0.1 at the baked 4032/128 cuts (#1072), and the lqueue harness's local self-proof already walked the flat shape across three decades: 120k, 113k, and 108k ops/s at depth 10, 100, and 1000 with p99 172 to 189 us and zero oracle misses (#1080).
The write half of the claim is the same bill read from the drain side: the edge node coalesces in the hot tier between flushes, so a drain window carries a handful of node images and root frames set by cadence, not by how many pushes and pops the window absorbed.
If flatness breaks it breaks in the fence page cache on the 10^7 arm or in the box's cold tail, not in the IO count, and the per-depth table the suite prints will show which.

## PRED-SQLO1-T5-FEED

The LPUSH plus LTRIM capped feed costs O(1) amortized per op, with data-file bytes per op bounded by node size over cap length.
Reasoning on record: the feed's steady state pushes at the head and trims whole nodes off the tail, and LTRIM's bill is edge rewrites plus fence cuts only (L-I3), so each element is written into a head-node image on the way in and leaves in a whole-node cut that costs a delete, never a rewrite of surviving elements.
The lnode lab priced the feed pair at 2.33 frames and ~4.3 KB (#1072), and the same lab's feed knee is why node_max is 4032 at all: at 2016 a cap-1000 feed pages its fence, so the baked constant keeps realistic caps on the flat fence where the trim cut is a root-only edit.
Amortized over the cap, the data-file bill per op is one node's bytes spread across the elements that node held, which is the node-size-over-cap-length bound the milestone row states.
A miss means the trim is rewriting interior nodes or the fence edit is billing page rewrites per op, both visible in the put and delete counters the suite records.

## L-I1 through L-I6, each mapped to passing tests

- L-I1 (pushes, pops, LINDEX, LSET, and LMOVE touch O(1) nodes at any length): TestListInlineDequeOps and TestListNodedDeque pin the edge paths on both rungs, TestListNodedPushBatching the batch-by-batch cut bill, TestListPositionalOracle the fence prefix-sum seek that makes LINDEX and LSET one node read, TestListMoveEdgeIO the move bill at 4 puts edge-amending and 3 puts 1 del node-draining, and TestListPagedLadder the cold point path at root, page, node on the paged rung.
- L-I2 (LLEN O(1) exact, crash-inclusive): the root carries the exact count, and TestListKillMatrix plus TestListCleanControl in cmd/sqlo1crash hold every recovered image to LLEN equals the walked length equals per-position point reads under real SIGKILLs, plus repeatability across a second open; in-engine, TestListMoveTornTail and TestListPagedTornTail pin the count at every WAL prefix.
- L-I3 (LTRIM cost is edge rewrites plus fence cuts): TestListTrimOracle pins the window grammar across the rungs and TestListTrimEdgeIO the bill itself, head and tail cuts at exactly 2 puts 0 dels and an interior cut of ~12 nodes at most 3 puts with deletes equal to dropped nodes; the paged ladder in TestListPagedLadder covers trim dropping whole pages while rewriting only the two edges.
- L-I4 (LRANGE streams with bounded RAM): the prefetch loop materializes one LookupBatch round ahead of the writer by construction (#1082); TestListPositionalOracle drives thirteen window shapes against a reference slice through cold reopens, and TestServerListPositionalNoded crosses two prefetch rounds on the wire.
- L-I5 (LINSERT, LREM, LPOS cost O(scanned) with directional early exit): TestListScanOracle pins all three against the reference walk, TestListScanEdgeIO the near-end bill at root plus a node or two, TestListInsertGrowth the split-only-past-thresholds rule, and TestListRemMerge the merge_max 2016 counterweight that holds the lmid decimation adversary at 1.003x nodes and 0.490 occupancy (#1073).
- L-I6 (fence order is the single source of list order, scrubbed against node headers): TestListVerifyHealthy, TestListVerifyPaged, TestListVerifyCatchesDivergence, and TestListVerifySample pin the new Verify cross-check with injected divergences caught by name, a node header disagreeing with its fence count flat and paged, a missing node, and an aliased fence; TestListRungPreflight runs it healthy on every rung and TestListKillMatrix runs it on every SIGKILLed image.

## The list kill matrix, on record

New in cmd/sqlo1crash alongside this note: listrig_test.go and listmatrix_test.go, the type-rig matrix on the list ladder with an exact-state oracle on top.
The keyset spans 4 inline queues, a noded queue, a noded middle-op key, and a paged queue of 10500 elements whose 68 B entries cut ~178 nodes, past the 167-entry flat fence cap, plus the capped-feed destination; all populated and flushed before any kill window opens.
Steady state runs the queues at 40 percent LPUSH, 40 percent RPOP, and 20 percent LINDEX probes inside floor and ceiling bands, the middle-op key at 25 percent LSET, 20 percent LINSERT, 20 percent LREM alternating scan directions, and 15/10/10 edge and probe traffic, and every 192 ops the feed cadence moves the noded queue's tail onto the destination's head and trims it to 100, the PRED-SQLO1-T5-FEED shape under kills, with the move-before-trim intermediate state a legal recovery point.
Elements are self-describing (key, sequence number, seeded filler, xxhash tail), so the parent classifies any recovered element with no journal.
The oracle is stricter than the zset arm's membership rule because list recovery is command-boundary exact per key: the parent replays the stream and digests every key's content at every post-population op count, and a recovered key must sit at exactly one of those states, with Verify first, then LLEN, the LRANGE walk, and per-position LINDEX point reads all in agreement, then a second open landing on the same state.
Pops stay in the crash stream here, unlike the zset matrix's excluded ZPOPMIN: a deque pop names its end, so the parent replays the choice deterministically and there is no engine-chosen victim to lose.
Local runs: preflight green (11660 population ops, 4 inline, 2 noded, 1 paged, Verify green), 6 kill iterations and the 25000-op clean control green at defaults, then 25 kill iterations and a 60000-op clean control green, every recovered key at an exact stream state, destination included.
On record for the box: 100 kill iterations at SQLO1_LIST_KILL_ITERS=100, zero drift.

## Bookkeeping

Filed before any T5 suite rep has run on the gate box.
The suite run lands in its own results note with the depth sweep, the rival arms, the per-operator table, VmHWM capture, and provenance; these predictions get their verdicts there, and the L map above carries into that note verbatim.
