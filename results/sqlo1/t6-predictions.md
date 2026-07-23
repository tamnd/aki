# T6 predictions and invariant map, filed before the exit-gate run

Milestone T6 (tamnd/aki#723); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The suite half of the exit gate (the append mix, the trimmed feed, consumer-group fan-out at 1/10/100 consumers, the cold catch-up concurrent with a hot tail, the per-operator table against the redis 8.8 and valkey 9.1 arms) waits on the gate box; this note files the three milestone predictions and the X-I1 through X-I5 test map, whose evidence is complete software-side, including the eleven-phase stream torn-tail matrix.

## PRED-SQLO1-T6-XADD

XADD throughput lands within 1.2x of plain SET at equal payload on the append mix.
Reasoning on record: an XADD is a tail amendment, one run image patched through the shared entry encoder plus the root frame, so the marginal op writes the same one-value-one-root shape a SET does and the run encode cost amortizes over the entries the run holds (X-I1, a sealed run is never rewritten).
The xadd lab billed steady 100 B XADDs at 1.03 frames and ~2.2 KB WAL per op, the same bill the T5 queue mix paid, with the encode ratio at 0.70 and ~21 ns per entry at the baked 4032/128 cuts (#1114), and fan-out held WA at 1.0 to 1.2 from 10 to 1000 streams, so the feed bill does not degrade when the mix spreads.
The 1.2x headroom covers what SET does not pay: the name-table and varint ID encode, the amendment read-modify-write on the hot tail image, and the occasional run cut.
A miss would show as per-op frames drifting above ~1.05 or as run cuts billing more than their share, both visible in the WAL byte counters the suite records.

## PRED-SQLO1-T6-CATCHUP

A cold 10^7-entry catch-up leaves the hot working set intact, and foreground point-op p99 moves under 20 percent during the replay.
Reasoning on record: catch-up ranges decode only boundary runs and stream through a fixed 16-run prefetch window (streamRangeBatchRuns, baked by the 4/16/64 A/B in #1145), so the replay's resident footprint is bounded by the window, not the stream, and the xcatchup lab held RSS at 120 to 130 MiB over a 1.1 GB file at 1.74M entries per second.
The prediction split locally and the split goes on record with it: the working-set half held, with after-replay probes within 12 percent of baseline, while the p99 half failed locally at 3.9x in the worst window even though p50 stayed at baseline, which reads as dispatch burst on a loaded laptop rather than cache damage.
The gate box decides the p99 half on quiet hardware; if it fails there too, the lever is scheduling fairness in the dispatch loop, not the storage path, and the working-set evidence stays standing either way.
This is the one T6 prediction filed with a known local counterexample, per the doc 13 rule that the number goes on record anyway.

## PRED-SQLO1-T6-PEL

XACK cost is independent of stream length: an ack touches only PEL segments, the group record, and the root, never entry runs.
Reasoning on record: the PEL lives in kind 5 segments keyed by (rooth, segid) with its own fence in the group record, so delivery and ack churn is structurally disjoint from the entry side (X-I3), and stream length never enters the ack path's IO bill.
The xpel lab put floors on record at the baked 4096/1024 segment cuts: FIFO ack 146 ns and random ack 904 ns in-memory, XPENDING walks at 1.4 ns per entry, XAUTOCLAIM at 11 ns per claimed entry, with the codec at 16.07 B per pending entry (#1270).
The known cost that does scale is pending count, not stream length: the inline PEL fence rebills the group record per delivery and ack batch, which is why the same lab's verdict routes the fence inline-then-paged as the scoped follow-up; at 10^5 to 10^6 pending the paged shape flattens to ~680 B per delivery and ~663 B per ack.
A miss would show as ack latency tracking the entry-side arm of the sweep, and the per-operator table's XACK row against stream depth is the direct check.

## X-I1 through X-I5, each mapped to passing tests

- X-I1 (XADD O(1) amortized, never rewrites a sealed run): TestStreamAddInvariants asserts the tail amendment is byte-equal to a from-scratch re-encode after every add, TestXaddWire pins the ID grammar and refusal ladder, TestStreamFenceTransition and TestStreamPagedLadder pin the run cut and page spill paths, and the xadd lab's frame counter held 1.03 frames per op (#1114).
- X-I2 (XLEN, XINFO STREAM summary, XPENDING summary O(1)): the root carries count, entries-added, last-generated-ID, and max-deleted-ID, and the group record carries the pending count and fence bounds; TestXsetidXinfoWire, TestStreamSetIDOracle, and TestStreamSetIDPagedTop pin the root fields through moves and paging, TestXdelWire pins max-deleted-ID advancing to the largest deleted ID and never back, TestXpendingWire and TestStreamPendingSummaryExt pin the summary form, and TestStreamPagedTornTail renders the accounting line at every WAL cut.
- X-I3 (PEL churn never touches entry runs, entry reads never touch PEL segments): kind 5 segments and kind 3 runs are disjoint keyspaces by construction; TestStreamPelDeliverAckOracle and TestStreamPelHistoryOracle drive delivery and ack churn with entry-side state asserted unchanged, TestStreamPelDelConsumerSweep and TestXdelWire pin the crossings that are allowed (a consumer sweep, a delete leaving the PEL exact with its nil history row), and TestXinfoFullPelWire renders real PEL tables without entry-run reads.
- X-I4 (approximate trim cuts whole runs, exact trim rewrites at most one run): TestStreamTrimEdgeIO pins the bill directly, whole-run cuts at fence-edit cost and one boundary rewrite at most, TestStreamTrimOracle pins the grammar across MAXLEN and MINID on both rungs, and TestStreamTrimPaged covers pages dropping whole while only edges rewrite.
- X-I5 (group state count-exact under W1-W4 with crash-exact PEL contents): TestStreamPagedTornTail phases 8 through 11 hold every WAL prefix to a command boundary with the full group table and full pending table rendered at every cut, so a delivered-but-unacked entry survives the cut as pending exactly once, fresh segments flush before the records that reference them, and a destroy's compaction plus segment sweep lands whole or not at all; TestStreamGroupOracle, TestStreamCGLag, and TestStreamPelEntriesReadRepair pin the count and lag arithmetic live against 8.8.

## The stream crash rows, on record

The torn-tail matrix (TestStreamPagedTornTail) runs at dialed caps (three flat runs, two runs per page, four index slots, 64 B PEL segments) with the WAL cut after every frame across eleven phases: append and transition, tail rewrite, page spill, trim with whole-run and whole-page drops, XSETID root moves, the kind 4 group ladder including destroy compaction, kind 5 delivery and ack with cross-segment sweeps, the pending surface with claims and FORCE mints, and the XDEL phase's tomb flips, page death, deletes of pending entries, delete to empty, and a destroy over a live multi-segment PEL.
Every snapshot carries the root accounting line, so last-generated-ID monotonicity is asserted at every cut, and the full pending table, so PEL exactness is too; those are the two named exit-gate crash rows, and both hold at every prefix.
Streams have no cmd/sqlo1crash rig: the type ladder there exercises the shared tiered machinery under real SIGKILLs, and the stream-specific atomicity claims are all frame-group claims the WAL-prefix matrix checks strictly tighter, cut by cut, than a timed kill can.

## Bookkeeping

Filed before any T6 suite rep has run on the gate box.
The compat corpus closed the slice list one PR back (#1286, 196 STREAM rows green on the first replay), so the software half of the milestone is complete as of this note.
The suite run lands in its own results note with the mixes, the rival arms, the per-operator table, VmHWM capture, and provenance; these predictions get their verdicts there, and the X map above carries into that note verbatim.
