# B3 predictions, filed before the measured runs

Milestone B3 (tamnd/aki#719); spec 2064/sqlo1 doc 13 discipline: the number goes on record before the lab runs, so the lab can only confirm or embarrass it, never shape it.
The lab predictions here cover labs/sqlo1/b3/01_batchdrain (batchdrain-b) and labs/sqlo1/b3/02_hotclock (hotclock-b); the suite-level predictions (PRED-SQLO1-B3-BEATA, -WA, -COLD) file separately before the exit-gate suite run.
Both labs run on the gate box; A2's batchdrain has not run yet either, so the two verdicts land together and the A-vs-B comparison is free.

## PRED-SQLO1-B3-DRAINCOST

A Track B drain cycle prices under 1200 ns per drained row on the gate box at the doc constants (8 MiB window, 1024 cap, uniform), at least 2x cheaper than the same row through sqlo1a on the same hardware.
Reasoning on record: the A-side floor measured at A2 development time was 2229 ns per row (SQL transaction, statement rebinds, B-tree page writes); the B-side cycle is one WAL append run, one group-buffer memcpy, and one chunk upsert per row, whose micro costs (envelope encode ~100 ns, appendVlog copy ~50 ns, chunk probe and insert ~200 ns, WAL frame ~150 ns plus one fsync amortized over the batch) sum well under half the A-side number.
If B does not beat A by 2x here, the whole doc 04 batching thesis is suspect and the drain slice does not bake constants until the story is written.

## PRED-SQLO1-B3-CAPREADER

Reader p99 rises monotonically with the op cap, and at cap 4096 under zipf it is at least 2x the cap-256 p99 at the same window.
Reasoning on record: one ApplyBatch holds the store mutex for the whole RAM apply, so the cap is literally the lock-hold knob; 4096 rows at several hundred ns each is a millisecond-class hold that uniform readers must eat, while 256-row holds hide under scheduler noise.
The verdict keeps cap 1024 unless the 4096 arm shows a writer-side win over 10% at acceptable reader p99, which the reasoning above says it will not.

## PRED-SQLO1-B3-WINDOWFLAT

Writer throughput moves under 10% across the window sweep (1 to 32 MiB) at fixed cap, and drain lag p50 scales roughly linearly with the window, so the window verdict stays 8 MiB on lag grounds, not throughput grounds.
Reasoning on record: on Track B the cycle cost is per-row dominated (no per-transaction fsync tax to amortize away with bigger windows; the WAL syncs per ApplyBatch regardless), so the window only sets how much dirty data queues up, which is a staleness and RAM question, not a speed one.
This is the constant most likely to differ from Track A, where bigger windows should genuinely help amortize transaction overhead.

## PRED-SQLO1-B3-CKPTSTALL

Checkpoint stalls, not drain cycles, own the max reader latency: in every configuration where at least one checkpoint fires, max reader latency is at least 5x the reader p99, and the ckpt row's max duration exceeds 100 ms at the 256 MiB trigger with 500k keys.
Reasoning on record: a checkpoint under this write load rewrites every dirty bucket's chunks plus the whole one-group directory and syncs the data file, seconds of work at v0 (no incremental FlushIndex), all under the store mutex readers need.
This prediction is the on-record motivation for the B3 drain slice's group-buffer handoff and for keeping checkpoint cadence off the owner loop in the runtime; if the stall measures small, that pressure drops.

## PRED-SQLO1-B3-DSTANDS

The S1 verdict D=0.125 survives the nanosecond metric: on every trace, amortized cold time per point read at D=0.125 is within 10 percent of the best D in the sweep, and D=1.0 is at least 15 percent worse than D=0.125 on the scan-mix traces.
Reasoning on record: cold cost per miss is roughly constant (three group preads, page-cache warm), so amortized cold time is miss ratio times a constant, and the S1 hit-ratio ordering carries over unless the ghost-ring insert path or the tier bookkeeping has a hidden per-promotion cost big enough to show at 128 B values, which it should not.
This is the boring outcome and it is the predicted one; the lab exists because "should carry over" is not a number.

## PRED-SQLO1-B3-KSTANDS

K stays flat in nanoseconds as it was in ratio: no K in {16..256} moves amortized cold time by more than 5 percent on any trace, so K=64 survives on eviction-cost grounds alone.
Reasoning on record: K only changes which entries leave, and at S1 the ratio spread across K was under 1 percentage point; a constant cold cost cannot amplify a flat ratio into a nanosecond gap.

## PRED-SQLO1-B3-COLDREAD

The measured cold read through the checkpointed sqlo1b index prices between 1 and 4 microseconds on the gate box (page-cache resident, 1M keys, 128 B values), and stays flat across the sweep configurations, confirming the read path has no hidden hot-set dependence.
Reasoning on record: the read-path slice pinned exactly 3 group reads cold and the store-level A2-era floor for a hot point read was 2208 ns through sqlo1a; sqlo1b replaces SQL with two pread-backed group loads plus one directory page hit, each a microsecond-class page-cache copy plus decode.
Under 1 us would mean the OS absorbed a read we think we issue; over 4 us would mean decode or locking costs the doc 03 ledger does not carry, and either way the hot-tier slice waits for the story.

## Falsification terms

Predictions are measured by labs/sqlo1/b3/01_batchdrain and labs/sqlo1/b3/02_hotclock on the gate box before the drain slice bakes the window or cap and before the hot-tier slice bakes D or K.
A failed prediction does not get re-run until the causal story is written down next to the failing number in this directory.
