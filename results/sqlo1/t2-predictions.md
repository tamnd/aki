# T2 predictions and invariant map, filed before the exit-gate run

Milestone T2 (tamnd/aki#715); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The suite half of the exit gate (hash session-store mix, wide-hash point ops, HGETALL streams, field-TTL churn, the hfence curve) waits on the gate box; this note files the three milestone predictions and the H-I1 through H-I6 test map, whose evidence is complete software-side.

## PRED-SQLO1-T2-FLAT

HGET and HSET p50 and p99 flat from 10^2 to 10^9 fields at a fixed memory cap, with flat meaning the 10^9 point lands within 1.25x of the 10^2 point on both quantiles.
Reasoning on record: a cold point op is bounded by structure, not size. Flat root: root read, one record. Segmented: root read, fence binary search in the root bytes, one segment read, two records. Paged: root page index, one fence page, one segment, three records, and TestHashPagedTransition pins exactly 3 IO rounds cold.
Size only moves the fence search width (log of segment count, in RAM once the root is resident) and the page index width, neither of which touches the IO count, so the curve's slope should be indistinguishable from noise once the cap fixes the hit ratio.
The riskiest cell is warm HSET at 10^9: the write path rewrites a whole segment image per landing and drains fence-page frames with it, so if flatness breaks it breaks on write p99 at the paged rung, not on reads.
The 10^9 point is a separate ~56 GB run queued behind the standard suite.

## PRED-SQLO1-T2-HLEN

Cold HLEN is one record read at any size, and it is crash-exact against the oracle after every torn-tail case.
The software half is already green and on record here: TestHashTornTailMatrix recovers every WAL frame prefix of a nine-phase ladder scenario and demands HLEN equal the reachable field count at each of the ~200 cuts, and the cmd/sqlo1crash hash matrix does the same against real SIGKILLs over inline, segmented, and paged keysets (TestHashKillMatrix, TestHashCleanControl).
On record for the box: 100 kill iterations, zero count drift, and the clean control exact to the byte.

## PRED-SQLO1-T2-WAL

WAL bytes per HSET under zipfian field reuse land below 1.5x the segment post-image size, point estimate 1.2x.
Reasoning on record: rule W4 coalesces the root frame to one per drain batch, so the per-op cost is dominated by segment post-images, and zipfian reuse concentrates repeated fields into the same segments inside one drain window, so a segment image carrying k landed ops amortizes to 1/k images per op.
The adders are the root and fence-page frames on paged hashes and the entry overhead on fat values; the 0.3x headroom covers them.
A miss past 1.5x triggers the doc 06 W4 v2 lever ticket, which is the point of the number.

## H-I1 through H-I6, each mapped to passing tests

- H-I1 (point ops touch one segment plus one fence page at any size): TestHashPagedTransition asserts the cold paged HGET pays exactly 3 IO rounds (root, fence page, segment); TestHashSegPointOps covers the segmented rung; TestHashInlinePointOps the flat root.
- H-I2 (HLEN O(1) and exact, including across crash recovery, rule W3): TestHashTornTailMatrix at every WAL frame prefix; TestHashKillMatrix and TestHashCleanControl in cmd/sqlo1crash against real SIGKILLs, with the count oracle holding begin(count), the walked set, HLEN, and per-field point reads to exact agreement on every recovered image.
- H-I3 (full iteration streams with bounded RAM): TestHIterateSegmented asserts the cold walk pays segments in fixed prefetch batches (IO rounds equal the batch count, never one round holding everything); TestHIterateInline covers the flat root.
- H-I4 (DEL of any hash is O(1) foreground): TestHashSegmentedTakeover retires a segmented plane through the root DEL plus genbump path with no segment touches; TestHashPagedTransition does the same over a paged root; TestHashFieldTTLReapSegmentedDeath retires through the reap.
- H-I5 (HSCAN full-iteration contract under concurrent splits and merges): TestHScanCoverageUnderSplits; TestHScanResumeCold pins cursor resume over a cold reopen.
- H-I6 (expired fields never returned, never resurrect; min_expire stale-early only): TestHashFieldTTLLazyExpiry, TestHashFieldTTLIteratePurges, TestHashFieldTTLRandNeverDead, and TestHashFieldTTLReapStaleEarlyMin for the deleted-min-holder case; the reap rung tests (TestHashFieldTTLReapInline, TestHashFieldTTLReapSegmented, TestHashFieldTTLReapPaged) each re-check after a cold reopen.

## Bookkeeping

Filed before any T2 suite rep has run on the gate box.
The suite run lands in its own results note with the per-operator table, the hfence curve, VmHWM capture, and provenance; these predictions get their verdicts there, and the H-I map above carries into that note verbatim.
