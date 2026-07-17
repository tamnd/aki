# T4 predictions and invariant map, filed before the exit-gate run

Milestone T4 (tamnd/aki#721); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The suite half of the exit gate (leaderboard mix, range analytics, ZREMRANGE trims, the geo radii sweep with parity and over-read) waits on the gate box; this note files the three milestone predictions and the Z-I1 through Z-I6 test map, whose evidence is complete software-side, including the new zset kill matrix in cmd/sqlo1crash.

## PRED-SQLO1-T4-RANK

ZRANK p99 flat from 10^3 to 10^9 members at a fixed memory cap, with flat meaning the 10^9 point lands within 1.25x of the 10^3 point on both quantiles.
Reasoning on record: rank arithmetic is a fence-count descent bounded by structure, not size (Z-I3), so a rank read costs the member-side score lookup (root, at most one fence page, one segment) plus the score-side descent (the root's page index, at most one fence page, one run), and no term in that sum grows with cardinality once the two-level 250/250 fence pages hold.
The zrank lab already walked this curve in memory: 708 ns p99 across six decades with the root at 3.7 KB for 10^9 members (#1014), so the gate-box question is only whether the cold-read multiplier stays flat too, and the IO count argument says it must: the descent reads the same three records at 10^9 as at 10^5.
If flatness breaks it breaks in the root page index search or the box's cold-read tail, not in the IO count, and the per-decade table the suite prints will show which.
The 10^9 point is a separate large run queued behind the standard suite.

## PRED-SQLO1-T4-COLDBOARD

Cold 10^8-member leaderboard top-100 ZRANGE lands under 5 ms to first result on the gate box.
Reasoning on record: the top-100 window is a rank walk that seeks by fence-count descent and reads no run before the window (the #1036 contract), so the cold cost is the root, the score fence's page index, one fence page, and the two or three runs the window spans, roughly five or six cold record reads end to end.
At the box's NVMe latencies through the sqlo1b read path that is a few hundred microseconds of device time; 5 ms leaves an order of magnitude for the directory walk, checksum verifies, and a cache-cold open, which is the honest margin for a first-result number rather than a steady-state one.
A miss past 5 ms means the descent is reading runs before the window or the open path is walking more than it must, both visible in the read counters the suite records.

## PRED-SQLO1-T4-WALZ

WAL bytes per score-moving ZADD on the gate-box leaderboard mix land within 15 percent of the hsegz lab's 2.40 post-image frames and 6.7 KB per board move, with the paged 10^9 arm under 8 frames and 25 KB per move.
Reasoning on record: a score move rewrites the member's segment, the old run, the new run, and the root in one command frame group, and on the paged rung the touched fence pages of both planes bill in the same group, which is where the lab's flat 2.40-frame average grows to the restated ~22.4 KB and up-to-8-frame bill at 10^9 (#955, #1014).
The milestone's provisional bound (under 1.5 frames average, under 3 worst case) is on record as re-priced by measurement, not recalled away: the labs showed the honest average is 2.40 because most board moves cross runs, and the wal-delta lever that would buy the difference stays a priced v2 candidate rather than a T4 dependency.
What the gate box adds is the mix at real device speeds; a result past the 15 percent band means frames are billing outside their command group, which the frame accounting in the suite output would surface.

## Z-I1 through Z-I6, each mapped to passing tests

- Z-I1 (ZSCORE and ZADD touch at most 1 member segment and 2 runs at any zset size): TestZAddInline, TestZAddSegFlags, and TestZDualMutationLadder pin the point paths across the rungs; the member side inherits T2's H-I1 bounded cold read through TestZSetMemberLadder and TestZSetPagedMembers, and the score side routes to exactly one run through TestZRunLadder and TestZRunEqualScoreChain, with the old-run-plus-new-run worst case being the move TestZDualMutationLadder drives; TestZScoreImageStability holds the read path to leaving no writes behind.
- Z-I2 (ZCARD O(1) exact, crash-inclusive): TestZsetKillMatrix and TestZsetCleanControl in cmd/sqlo1crash hold every recovered image to ZCARD equals the walked member set equals per-member point reads, across inline, segmented, and paged rungs, under real SIGKILLs with a ZRANGESTORE cadence cutting bulk-build commits, plus repeatability across a second open; in-engine, TestZSetTornTailMatrix and TestZSetPagedTornTailMatrix pin the count at every WAL prefix.
- Z-I3 (rank arithmetic never scans more than one fence page index, one fence page, and one run): TestZRankLadder and TestZRankEqualScoreChain pin the descent on the flat fence, TestZFencePagedLadder and TestZFenceChainAcrossPages on the two-level pages with strict parent cross-checks, and TestZFenceThirdLevel pins the cap's error door; the zrank lab's 708 ns p99 across six decades is the measured shape of the same bound.
- Z-I4 (the two sides never disagree, crash included): TestZVerifyHealthy, TestZVerifyPaged, TestZVerifyCatchesDivergence, and TestZVerifySample pin the scrub cross-check itself with injected single-side divergences caught by name; TestZsetKillMatrix runs ZVerify on every SIGKILLed image plus a score-legality oracle that calls any recovered score the stream never assigned a torn move; and the torn-tail matrices cut every WAL prefix through dual moves (TestZSetTornTailMatrix, TestZSetPagedTornTailMatrix), pops (TestZPopTornTail), trims (TestZRemRangeTornTail), and the store builders (TestZRangeStoreTornTail, TestZAlgebraStoreTornTail).
- Z-I5 (range reads stream; no operator materializes more than the IO batch window): satisfied by construction since #1036, with the rank walk reading no run before the window; TestZRangeStoreShapes and TestZRangeStorePagedDest pin the streaming builder, TestZRemRangeRungs the trim's single forward window, TestZUnionSegmented and TestZInterPaged the algebra's one-IO-round-per-cursor merge and window walk, and TestZRandMemberRungs the rank-descent sampler that never loads the plane.
- Z-I6 (geo results bit-identical to Redis's encoding and filter semantics): TestGeoCodecPinned holds the 52-bit codec to live-captured Redis bits, TestGeoSearchOracle the cover-and-filter walk to the brute-force oracle, the geo lab's parity arm measured 6000 scores bit-identical and 200/200 searches set-identical against live Redis 8.8.0 (#1053), and the compat GEO section's ZSCORE readback rows pin the cross-family claim on the wire (#1064).

## The zset kill matrix, on record

New in cmd/sqlo1crash alongside this note: zsetrig_test.go and zsetmatrix_test.go, the set matrix's shape on the zset ladder with the dual-plane oracle on top.
The keyset spans 4 inline, 2 segmented, and 1 paged zset (~180 member segments and ~180 score runs, past both the 128-segment member fence wall and the 100-run flat cap), populated and flushed before any kill window opens; steady state is score-move heavy on purpose, 50 percent ZADD with a fresh score (the dual-plane move on a live member), 20 percent ZINCRBY, 15 percent ZREM, 15 percent ZSCORE probes, plus a ZRANGESTORE cadence alternating a real 400-rank window build with an empty window that exercises the destination delete.
Scores live on the quarter grid so every sum is exact in float64 and the shadow compares with plain equality; members are self-describing (key, index, seeded filler, xxhash tail), so the parent classifies any recovered member with no journal.
The oracle runs ZVerify on every recovered key first, then holds the ZSCAN walk, ZCARD, and per-member ZSCORE point reads to agreement, and the kill arm adds the score-legality rule: a member the bounded stream never removed must survive, and a present member's score must be one the stream actually assigned it, so a half-applied move surfaces as a named failure whichever plane it tore on.
Local run: preflight green (10160 population ops, all rungs verified, ZVerify green), 6 kill iterations green, clean control at 25000 ops exact to the score, destination included.
On record for the box: 100 kill iterations at SQLO1_ZSET_KILL_ITERS=100, zero drift.
Pops and ZRANDMEMBER stay out of the crash stream deliberately: the engine picks the victim, so a killed parent cannot replay the choice; their crash story is TestZPopTornTail and the removal path ZREM covers.

## Bookkeeping

Filed before any T4 suite rep has run on the gate box.
The suite run lands in its own results note with the per-operator table, the geo radii sweep, VmHWM capture, and provenance; these predictions get their verdicts there, and the Z map above carries into that note verbatim.
