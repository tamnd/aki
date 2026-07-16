# A3 predictions, filed before the measurement run

Milestone A3 (tamnd/aki#716); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
A3 is a measurement and decision milestone with no new machinery; everything below gets its verdict from one full run on the gate box (core suite plus str and hash suites, Track A vs Redis 8.8, Valkey 9.1, and f3, both arms, VmHWM, disk footprint), and the decision is minuted against the doc 14 kill table.

## PRED-SQLO1-A3-POSTURE

Track A lands 0.3-0.7x Redis on data-bearing point workloads, with streams-adjacent and range rows the strongest families.
Reasoning on record, from doc 02 section 8 and the drivershoot and abatch labs: tuned point reads through the frozen sqlo1a driver sit around 100-200k/s per file cache-hot, and drained writes batch well but stay bounded by SQLite's single writer per file.
Range families (GETRANGE over ropes, HGETALL streams, SCAN) are B-tree home turf where the per-op fixed cost amortizes over the row stream, so those rows should sit at the top of the band and may cross it.
The bottom of the band belongs to small-value point writes under concurrency, where the drain serializes on the writer lock.

The number that matters most is not the Redis ratio but the f3 ratio: the doc 14 kill line freezes Track A as a compatibility layer behind a flag if it lands under 0.75x f3 on the data-bearing suite.
Point estimate on record: Track A lands under the kill line on the point-op families and gets frozen, because f3's log-structured write path has no per-file writer bottleneck; the survival case ("Redis API over a real SQLite file", worth shipping at or above 1.0x Redis) is real but requires the batching to carry point writes further than the abatch knee suggested.

## PRED-SQLO1-A3-MEM

Equal-data VmHWM within 1.2x of Redis despite carrying the store, because the shared hot tier dominates the resident set and the SQLite page cache is pinned small by the apragma constants.
The sqlocache lab's RAM split (hot-tier-first vs page-cache-only) is the evidence base; the risk is double-caching on read-heavy arms where rows live in both the hot tier and the page cache, which is exactly what the 0.2x headroom is for.

## The A2 drift-check slice

The first A3 slice asks for re-runs of any A2 lab whose constants changed since landing.
Nothing has drifted because nothing has been baked: the pragma constants PR is still held pending the apragma verdict, which itself waits in the A2 gate-box queue.
So the drift-check slice collapses into the A2 queue: the box session runs the four A2 labs, the constants PR lands from the held branch, and A3's full run follows in the same session with those constants live.

## Bookkeeping

Filed before any A3 rep has run.
The full run, the results note with the per-row 0.75x-of-f3 callout and the shared-runtime vs store cost split, and the kill-table decision all land together after the gate-box session; whichever way the decision goes, the A3 matrix is pinned as the Track B comparison baseline.
