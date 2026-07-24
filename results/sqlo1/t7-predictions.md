# T7 predictions and invariant map, filed before the exit-gate run

Milestone T7 (tamnd/aki#726); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The suite half of the exit gate (churn mixes at several TTL distributions, lazy-expiry correctness on every type's read path, the per-operator table against the redis 8.8 and valkey 9.1 arms, the crash rows) waits on the gate box; this note files the three milestone predictions and the E-I1 through E-I6 map, whose software-side evidence is complete as of the compat PR (#1346).

## PRED-SQLO1-T7-DIEINRAM

At least 80 percent of keys with TTL under the drain window expire without a disk write on the short-TTL burst mix.
Reasoning on record: the dieinram lab (#1336) showed the baseline free win is only ~2 percent, because dirty entries cannot evict and the drainer writes unconditionally, and FIFO drain lag sits at one full interval.
The claim therefore rides entirely on slice 4 (#1338): reap-cancel converts an expired plain put to a tombstone at collect time, which alone recovered ~97 percent of any sub-interval TTL in the lab, and volatile-near deferral parks records for up to two extra windows, lifting the one-to-two-interval band from 45 to 98 percent on the uniform mix.
The 80 percent bar leaves room for the band above one interval where deferral is the only lever and its cap is two laps by design.
A miss would show as drained-dead counts staying near the baseline in TieredStats (ReapCancels low, VolDefers high but not converting), which the suite records per mix.

## PRED-SQLO1-T7-REAPWA

Reaping adds under 0.3 to end-to-end WA on the churn mix, and total WA stays under 1.3 over the B3 baseline component.
Reasoning on record: the reaper's own writes are tombstone frames that batch at 256 or ride the drain cycle (#1334, sized by the reaper lab's flat ~4ms fsync bill), and the pass itself is time-boxed at ~100us per tick, so reaping adds frames, never relocations.
The reclamation side is the slice 2 credit machinery (#1330): near-class bytes book per extent with the latest deadline at placement, and the debt picker realizes all of it only once the max deadline passes, so a pure-TTL extent compacts with zero live bytes to move and the WA term for reclamation approaches the doc's near-1 ideal.
The known tension is mixed-deadline extents, where credit waits for the slowest record and garbage from overwrites has to carry the pick; the lab could not price that mix end to end, which is what the suite's WA breakdown is for.
A miss would show in the CompactStats.ExpiredBytes to relocated-bytes ratio and in DebtStats.ExpiredDrops staying low while garbage picks dominate.

## PRED-SQLO1-T7-DISK

Disk usage converges within 2x live-data size on the 100 percent-TTL churn mix.
Reasoning on record: on that mix every extent eventually holds only expired bytes, the slice 2 credit fires at its max deadline with zero booked garbage needed, and compaction plus the checkpoint release returns the extent to free, so the steady state is bounded by live data plus the extents still inside their deadline window plus the write frontier.
The file never shrinks, so the 2x reads against the high-water mark of a converged run, the same protocol the B4 G3 ladder used (#1309).
The drain interval and the TTL distribution set the in-window slack; 2x holds when the churn writes roughly one live-data volume per TTL window, which is how the mix is defined.
A miss would show as the free-extent gauge trending down across windows while ExpiredBytes credit sits unrealized, which separates a picker bug from an in-window slack undercount.

## E-I1 through E-I6, mapped to the software-side evidence

- E-I1 (an expired key is never visible to any reader, regardless of reaper progress): both lazy doors treat expired as absent, the tiered read bail and the type layers' metaOf filter; TestTieredExpiryAcrossTheTiers walks visibility across hot, drained, and cold, TestReapStepOverB pins reads during reaper progress, TestExpireFamilySurface and the EXPIRY compat rows pin the wire view, and the per-type read-path arm of the suite extends this to every operator on the gate box.
- E-I2 (expiry edits never rewrite values, WAL op 3 only): NOT met as specified, on record since the T7 audit; the op 3 codec exists with no emitter and replay rejects it, so an expiry edit re-dirties the hot record or pulls a cold one, and the value bytes ride the next drain again. The suite's WA counters price this honestly; if the churn mixes show EXPIRE-heavy traffic paying materially, the op 3 emitter is the named residual lever, deferred not deleted.
- E-I3 (no expiry structure scales with total keyspace): met by a different mechanism than the doc's wheel, which stayed dead code; the shipped shape is a 2-bit class in existing index meta plus a per-extent credit map plus a time-boxed sampling pass, so there is no per-key expiry structure at all. TestExpClassFor, TestSampleExpiryClasses, TestSampleExpiryBudget, and the reaper lab's exact accuracy at 1M keys (#1332) carry the bound.
- E-I4 (TTL reclamation WA bounded by the picker's expired-fraction term, target under 1.3): this is PRED-SQLO1-T7-REAPWA; software-side, TestExpiredCreditFiresCompaction and TestNearCreditScope pin the credit term feeding the picker, and the verdict lands with the suite's measured breakdown.
- E-I5 (reaping emits the same frames as explicit deletion; replayed reaps are idempotent): ReapStep routes through the Str.Del contract, tombstone plus genbump in one drain batch, and reap-cancel converts to a tombstone rather than a drop precisely so a stale cold predecessor cannot resurrect the key; TestReapStepOverB covers reopen and replay, TestDrainReapCancel and TestDrainDeferredRecordCancels pin the cancel shape, TestTieredReapCancelStats the counters.
- E-I6 (no policy destroys data unless --hard-evict is set): TestPoliciesNeverDeleteData walks all eight policy names over a dirty set, TestHardEvictUnarmedNeverDeletes and TestHardEvictArmedDeletes pin the opt-in boundary, and the armed path deletes through the command path so roots retire their planes.

## The expiry crash row, on record

The named exit-gate row is that no expired key resurrects after recovery.
Software-side the mechanism is already strict: a reaped or cancelled key leaves a tombstone, never a gap, so replay lands on the tombstone rather than the stale cold record, and both lazy doors would refuse the resurrected read anyway since the deadline itself replays with the record.
TestReapStepOverB reopens mid-reap, and the accounting tests reopen both with and without a checkpoint so the WAL-rebuild path is the one under test.
The gate-box half is the timed-kill run of cmd/sqlo1crash with the reaper armed, which exercises the same claims under real SIGKILLs concurrent with drain, compaction, and eviction.

## Bookkeeping

Filed before any T7 suite rep has run on the gate box.
The compat corpus closed the slice list one PR back (#1346, 109 EXPIRY rows green on the first replay), so the software half of the milestone is complete as of this note.
The suite run lands in its own results note with the TTL distributions, the rival arms, the per-operator table, the WA breakdown, VmHWM capture, and provenance; these predictions get their verdicts there, and the E map above carries into that note verbatim.
