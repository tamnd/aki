# A2 predictions, filed before the measured runs

Milestone A2 (tamnd/aki#713); spec 2064/sqlo1 doc 13 discipline: the number goes on record before the lab runs, so the lab can only confirm or embarrass it, never shape it.
Reference numbers are the drivershoot verdict (results/sqlo1/drivershoot.md), ncruces at page_size 8192 on the gate box, and the measured runs happen on the same box.
Stack under test: the sqlo1a store as of #758 (statement catalog, crc-verified reads, ApplyBatch with the high-water mark, reaper, scrub).

## PRED-SQLO1-A2-POINT

A cache-hot point GET through the full sqlo1a stack (mutex, prepared statement, crc verification, expiry and tag gating, copy out) costs at most 2x the raw prepared read from drivershoot: at most 4416 ns per Get against the 2208 ns floor, single connection, reference cell shape (200k keys, 128 B values, uniform).

Reasoning on record: the stack adds one mutex pair, one crc32c over roughly 150 bytes (hardware Castagnoli, low tens of ns), two small copies, and gating arithmetic; that budget is nearer 1.2x than 2x, so 2x is the generous bound doc 13 wants as a stack-tax tripwire, not a stretch goal.
If this fails, the tax is structural (allocation churn or a driver interaction), and slice-5-adjacent profiling happens before any pragma tuning is allowed to paper over it.

## PRED-SQLO1-A2-DRAIN

Drained write throughput through ApplyBatch at the abatch-lab knee is at least 10x the single-statement rate, where the single-statement rate is the same upsert committed one row per transaction, same store, same box, measured in the same lab run.

Reasoning on record: drivershoot measured 2229 ns/row inside 4096-row transactions; a one-row transaction pays the commit path (WAL append plus frame sync work even at relaxed synchronous) per row, which SQLite folklore and our own drain-solo numbers put well past 10x that per-row cost.
The interesting output is not whether 10x holds but where the knee sits, because that number becomes the ApplyBatch sizing constant; if 10x somehow fails, the commit path is misconfigured (synchronous or journal pragmas not doing what slice 5 thinks) and the pragma slice reopens.

## Falsification terms

Both predictions are measured by labs/sqlo1/a2/02_abatch (drain arms) and the point arm carried in the same lab, before slice 5 bakes any pragma constants.
A failed prediction does not get re-run until the causal story is written down next to the failing number in this directory.
