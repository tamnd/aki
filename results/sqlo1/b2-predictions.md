# B2 predictions, filed before the measured runs

Milestone B2 (tamnd/aki#718); spec 2064/sqlo1 doc 13 discipline: the number goes on record before the lab runs, so the lab can only confirm or embarrass it, never shape it.
The lab is labs/sqlo1/b2/01_chunkindex, a counts-mode linear hashing simulator over the doc 03 section 8 protocol, with an exact-mode oracle and a real heap measurement of the doc 04 resident directory.
Unlike the A2 predictions this lab prices RAM and occupancy arithmetic, not disk, so the verdict run is machine-independent and happens locally.

## PRED-SQLO1-B2-INDEXRAM

The measured resident directory heap is under 1 byte per cold key at 10^8 keys with the whole directory resident, under all three split policies, and the doc policy lands in 0.45 to 0.60 B per key.
The kill line stays 2 B per key as written in the milestone.

Reasoning on record: the directory costs 16 B per chunk (8 B position, 8 B check word), a chunk holds 42 entries, so bytes per key is 16 divided by 42 times the mean fill.
Overflow-driven splitting historically settles linear hashing near 0.8 fill counting chained entries, which gives 16 / (42 * 0.8) = 0.48 B per key before page-granularity overhead, and the 4 KiB page rounding adds low single-digit percent at millions of chunks.
The lf75 arm floors fill near 0.75 and still clears 1 B with 3x headroom, so only a simulator surprise (fill collapsing under 0.19) could breach the target, and that would mean the split protocol itself is wrong.
If this fails, the constant-baking slice does not start until the causal story is written next to the failing number.

## PRED-SQLO1-B2-READPATH

A cold point read is exactly 3 group reads (directory page, chunk group, record group) at any keyspace size, 2 warm, and the lab prices the only structural threat to that ceiling: buckets with chains of 2 or more links stay under 0.1% of buckets at every sweep point under the doc policy.
Single-link chains are allowed to be common under the doc policy (the smoke run already showed tens of percent); the prediction is that lf85 cuts the single-link rate below 10% while keeping the directory under 1 B per key, which is the crossover the cold-index slice should bake if it holds.

Reasoning on record: the read-path IO count is fixed by construction (radix root resident, directory page and chunk each one read, record one read) and will be pinned by an IO-count test in the read-path slice, so the lab's job is the tail: every chain link past the base chunk is one more group read on the probes that land there.
Poisson arithmetic at fill 0.75 to 0.85 over a 42-slot bucket puts the two-link mass orders of magnitude under 0.1%, but the doc policy's overflow-driven equilibrium is exactly the regime where that arithmetic is least trustworthy, which is why the number is measured, not derived.
The falsehit arm rides along: with 16-bit fingerprints and about 35 occupied slots probed per lookup, false hits should land near 35/65536, about 0.05%, and each one costs one wasted record read resolved by a full key compare; the store-level confirmation arrives with the read-path slice.

## Falsification terms

Both predictions are measured by labs/sqlo1/b2/01_chunkindex before the cold-index slice bakes the 512 B chunk layout, the 42-entry packing, or a split policy.
A failed prediction does not get re-run until the causal story is written down next to the failing number in this directory.
