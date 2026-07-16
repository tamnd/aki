# T3 predictions and invariant map, filed before the exit-gate run

Milestone T3 (tamnd/aki#720); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The suite half of the exit gate (membership mix, algebra arms at skewed ratios, SPOP drains, the per-operator table) waits on the gate box; this note files the two milestone predictions and the SE-I1 through SE-I4 test map, whose evidence is complete software-side, including the new set kill matrix in cmd/sqlo1crash.

## PRED-SQLO1-T3-FLAT

SISMEMBER p50 and p99 flat from 10^2 to 10^9 members at a fixed memory cap, with flat meaning the 10^9 point lands within 1.25x of the 10^2 point on both quantiles.
Reasoning on record: the set rides the hash ladder with the valueless codec dimension on, so a cold membership probe is bounded by structure, not size, exactly as T2's flatness prediction argued for HGET. Inline: root read, one record. Segmented: root, fence binary search in the root bytes, one segment, two records. Paged: root page index, one fence page, one segment, three records.
Set size only widens the fence search and the page index, neither of which adds an IO, so the curve's slope should be indistinguishable from noise once the cap fixes the hit ratio.
The set-specific wrinkle versus T2 is density: valueless entries pack roughly 60 members per 4 KiB segment at 64 B members, so 10^9 members is ~17M segments and a wider page index than any hash the T2 run built; if flatness breaks it breaks there, in the root page index search, not in the IO count.
The 10^9 point is a separate large run queued behind the standard suite.

## PRED-SQLO1-T3-ALGEBRA

Cold SINTER of a 10^4-member driver against a 10^8-member set lands within 3x of the same call hot.
Reasoning on record: the algebra drives from the smallest set and probes the big one in whole IO rounds (the #924 gather window), so the cold call is bounded by the driver's segment walk plus one batched probe round per window against the big set's fence, not by the big set's size.
The probe batching is the whole bet: 10^4 driver members hash-partition across at most ~64 windows of grouped segment probes, each one LookupBatch round, so the cold-over-hot ratio is the cold-read multiplier on a bounded number of rounds rather than 10^4 point reads.
A miss past 3x means the routing groups are not coalescing probes the way the salgebra lab said they would, and the fix is window sizing, not a new design; that is what the number is for.

## SE-I1 through SE-I4, each mapped to passing tests

- SE-I1 (SADD, SREM, SISMEMBER, SMOVE touch exactly one segment per member at any set size): TestSetUpgradeToSegments and TestSetInlinePointOps pin the inline and segmented point paths on the shared ladder, whose per-op IO bound is T2's H-I1 evidence (TestHashPagedTransition's exact 3-round cold read) inherited byte for byte through the valueless codec (TestSetEntryCodec); TestSMoveSemantics, TestSMoveSegmented, and TestSMoveDirtyGuard pin SMOVE's two-key frame group.
- SE-I2 (SCARD O(1) exact, crash-inclusive): TestSetKillMatrix and TestSetCleanControl in cmd/sqlo1crash hold every recovered image to SCARD equals begin(count) equals the walked member set equals per-member point reads, across inline, segmented, and paged rungs, under real SIGKILLs with a STORE cadence cutting bulk-build commits, plus repeatability across a second open; TestSetReconcileAcceptance covers the W3 count repair path in-engine, and TestSMoveCrashPrefix plus TestSPopCrashPrefixEdit and TestSPopCrashPrefixRebuild pin the multi-frame commands at every WAL prefix.
- SE-I3 (algebra streams with bounded RAM): satisfied by construction since #934. The intersection and difference hold one driver IO round (TestSInterPaged pins the gather window against a 16000-member paged set); the union's k-way merge holds one IO round per source cursor with an owned previous-emit copy as the entire dedupe state (TestSUnion, TestSUnionStore), and the digest table the spec's spill path existed for is deleted. The STORE builder buffers only the pending segment and the fence, capped by the format's third-level bound (TestSetStorePaged).
- SE-I4 (SPOP uniform, never rewrites more segments than it empties or edits): uniformity is the spop lab's chi-square verdict (#912, position allocator uniform); the removal bound is TestSPopCountSegmentedEdit (in-place edits), TestSPopWholeSegment (whole-segment removal touches only emptied segments), and TestSPopCountRebuild plus TestSPopRebuildPaged (the rebuild threshold at count >= 8x fence length, the lab's verdict baked).

## The set kill matrix, on record

New in cmd/sqlo1crash alongside this note: setrig_test.go and setmatrix_test.go, the hash matrix's shape on the set ladder.
The keyset spans 4 inline, 2 segmented, and 2 paged sets (~150 segments each, past the 128-segment fence-page boundary), populated and flushed before any kill window opens; steady state is 55/30/15 SADD/SREM/SISMEMBER plus a STORE cadence that alternates a real SUNIONSTORE bulk build with an empty-by-construction SINTERSTORE, so kills cut segment writes, fence pages, root PUT commits, and the empty-result delete.
Members are self-describing (key, index, seeded filler, xxhash tail), so the parent classifies any recovered member with no journal; a member the bounded stream never removed must survive (population is durable at READY), and the clean arm demands the exact final state, destination included.
Local run: preflight green (10392 population ops, all three rungs verified), 6 kill iterations green, clean control at 25000 ops exact.
On record for the box: 100 kill iterations at SQLO1_SET_KILL_ITERS=100, zero count drift.
SPOP stays out of the crash stream deliberately: the engine picks the victim, so a killed parent cannot replay the choice; its crash story is the removal path SREM covers plus the in-engine WAL-prefix tests named under SE-I2.

## Bookkeeping

Filed before any T3 suite rep has run on the gate box.
The suite run lands in its own results note with the per-operator table, VmHWM capture, and provenance; these predictions get their verdicts there, and the SE map above carries into that note verbatim.
