# B3 suite predictions, filed before the exit-gate run

Milestone B3 (tamnd/aki#719); spec 2064/sqlo1 doc 13 discipline: the number goes on record before the suite runs, so the run can only confirm or embarrass it, never shape it.
These are the three suite-level predictions the milestone requires before the exit-gate core-suite run; the lab-level predictions filed earlier in b3-predictions.md.
The core suite is the doc 13 matrix: GET-heavy 90/10, mixed 50/50, write-heavy 10/90, values 16/128/512/4096 B, uniform and zipf theta 0.99, datasets at 1x, 4x, and 16x of the memory cap, Track B vs Track A vs f3 vs rivals with VmHWM capture.

## PRED-SQLO1-B3-BEATA

Track B beats Track A on every core-suite row at equal memory budgets.
Reasoning on record: the write path is the widest gap, because the A2 floors put a sqlo1a drained row at 2229 ns (SQL transaction, statement rebinds, B-tree page writes) while the B cycle is one WAL append run plus one group memcpy plus one chunk upsert, predicted under 1200 ns per row in PRED-SQLO1-B3-DRAINCOST, so every mix with a write component inherits at least that margin.
The read path should win too: a cold B read is a directory probe, a chunk probe, and one group pread, while a cold A read is a full B-tree descent through SQLite's page cache, and at equal budgets A pays its page cache out of the same cap that B spends on the hot tier and chunk cache.
The thinnest rows will be GET-heavy zipf at 1x, where both tracks serve almost everything from hot RAM and the storage engine barely matters; if any row falls to A it will be one of those, and a loss there would say the Tiered composite's per-read overhead is too fat rather than that the format is wrong.
On record: no row falls, but the 1x GET-heavy margins land under 1.3x while the 16x and write-heavy margins land over 2x.

## PRED-SQLO1-B3-WA

End-to-end write amplification stays under 2 sustained on the write-heavy 16x arm, and the point estimate on record is 1.5.
Reasoning on record: the doc 04 budget allows 2 and the F2 ancestry measured 1.23 for a coalescing write-behind log, so the bound has slack; the B-side adders over that ancestry are WAL frame overhead (small against 512 B and up values, visible at 16 B), group padding now that the open group carries across batches (slice 2 made small drains stop padding a group each), and compaction relocation, which the debt controller caps at 3 live bytes per garbage byte but only triggers past a quarter-capacity garbage threshold.
Write-heavy uniform at 16x is the honest worst case because coalescing earns nothing and the overwrite stream manufactures garbage at full rate, so compaction runs continuously at the 75 percent utilization target; 1 for the WAL, the padding tax, and a steady-state relocation share is a 1.4 to 1.6 story, not a 2.
If the measured number crosses 2, the doc 14 kill-table row triggers and the debt controller gets reworked before B4; that consequence is the point of putting 1.5 on record now.

## PRED-SQLO1-B3-COLD

The 16x arm retains at least 36 percent of the 1x-arm throughput on every core-suite mix, which is the floor of the F2 shape of winning (36 to 83 percent retained at tiny budgets while rivals fall off the cliff).
Reasoning on record: zipf rows will sit high in the band because theta 0.99 concentrates the working set and the hot tier plus the S1 clock (D=0.125, K=64, re-verdicted in hotclock-b) keeps the hit ratio close to the 1x number, so the retained fraction is mostly the hit ratio carried over.
Uniform 16x is the floor case: nearly every read is a cold miss at three preads, so retention there is the ratio of the cold read path to the RAM read path, and the hotclock-b cold-read pricing says that ratio clears 36 percent for GET-heavy at 128 B and up.
The riskiest cell on record is uniform GET-heavy at 16 B values, where per-record overheads are proportionally largest; if any cell breaks the floor it is that one, and the fallback story is the doc 04 batching of misses through BatchGet, which the suite exercises but the labs did not.

## Bookkeeping

Filed before any exit-gate suite rep has run on the gate box.
The suite run lands in its own results note with provenance and the budget-table reconciliation; these three predictions get their confirmed-or-embarrassed verdicts there.
