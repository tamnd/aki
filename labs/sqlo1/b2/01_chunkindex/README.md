# chunkindex: RAM per cold key

Milestone B2 lab 01 (spec 2064/sqlo1 doc 03 section 8).

## Question

Does the cold index really cost under 1 byte of RAM per cold key at 10^6 to 10^9 keys, measured, with directory bytes accounted separately?
The ledger says 16 B of directory per 42-entry chunk, about 0.38 B per key at full chunks, but the divisor is the occupancy that overflow-driven linear hashing actually settles at, and the kill line is 2 B per key.
The same run answers the two structural riders: how often buckets chain (every link is an extra group read on probe, chains past 2 links must stay under 0.1% of buckets) and whether splits arrive in storms.
The falsehit arm measures the 16-bit fingerprint false-hit rate against the predicted 0.06% order.

## Method

The simulator is the doc 8.5 protocol at count level: chunk number from bits 47..0 under (L, S), an insert that would push a bucket past capacity chains it (base 41 plus pointer once chained, links 41, last link 42) and advances one split at the split pointer.
Counts mode redistributes a splitting bucket with a fair coin, which is identical in distribution to the real bit-L partition for uniform hashes; exact mode stores every hash and partitions on the real xxhash bit, and the oracle test holds the two within 3% so the 10^9 arm can run in 32 MiB of counter state.
The policy arms are the decision: doc splits only on overflow, lf75 and lf85 also split whenever the load factor crosses their target, buying lower chain rates with more, emptier chunks.
RAM per key is not arithmetic here: measureDirHeap builds the doc 04 resident directory (4 KiB pages of 256 full pointers plus a radix root) at the bucket count the sim actually produced and takes the Go heap delta across GC.
The falsehit arm probes present and absent keys against the exact-mode table; every fingerprint match that is not the probed key is a false hit the read path resolves with a record read and a key compare.

Read the sweep as: heap_dir_b_per_key is the headline against the 1 B target, chained_pct prices the probe tail against the 3-group-read ceiling, chain2_pct is the red-flag line, and max_split_window says whether split cost needs pacing.
The crossover the verdict needs is the policy where directory bytes stay sub-byte while the chain rate stops taxing the read path.

## Run

    ./run.sh            # {1e6, 1e7, 1e8, 1e9} x {doc, lf75, lf85} counts mode, exact cross-check at 1e7, falsehit at 1e7
    go run . -quick     # smoke
    go test ./...       # oracle: layout arithmetic, LH invariants, counts-vs-exact, heap floor

The lab is CPU and RAM only, no disk in the loop, so the verdict can run anywhere; predictions are on record in results/sqlo1/b2-predictions.md before the verdict run.

## Results

Full sweep 2026-07-16 on the mac (machine-independent arm), seed 1, raw CSV in results/sqlo1/b2-chunkindex.csv.

| n | policy | fill | chained % | chain2 % | heap B/key | max split window |
|---|--------|------|-----------|----------|------------|------------------|
| 1e6 | doc | 0.955 | 32.5 | 0.128 | 0.404 | 3438 |
| 1e6 | lf75 | 0.750 | 5.0 | 0.009 | 0.515 | 2525 |
| 1e6 | lf85 | 0.850 | 18.3 | 0.043 | 0.453 | 2921 |
| 1e7 | doc | 0.967 | 28.6 | 1.316 | 0.398 | 3770 |
| 1e7 | lf75 | 0.744 | 15.1 | 0.000 | 0.516 | 2766 |
| 1e7 | lf85 | 0.821 | 19.1 | 0.000 | 0.467 | 3235 |
| 1e8 | doc | 0.825 | 34.9 | 0.000 | 0.465 | 3829 |
| 1e8 | lf75 | 0.735 | 22.8 | 0.000 | 0.521 | 2831 |
| 1e8 | lf85 | 0.769 | 27.4 | 0.000 | 0.499 | 3235 |
| 1e9 | doc | 0.938 | 32.8 | 0.054 | 0.409 | 3832 |
| 1e9 | lf75 | 0.750 | 6.9 | 0.009 | 0.511 | 2848 |
| 1e9 | lf85 | 0.850 | 20.7 | 0.034 | 0.451 | 3290 |

Exact-mode cross-check at 1e7 doc: buckets within 0.1%, fill 0.9677 vs 0.9668, chain2 1.3152 vs 1.3156, so counts mode is validated and the 1e9 arms are trustworthy.
Falsehit at 1e7 with 2M probes: absent 0.0625%, present 0.0661%, predicted 0.0620%.

## Verdict

RAM confirmed: every policy at every scale lands between 0.40 and 0.52 heap B per cold key, 4x under the kill line and about 2x under the target, and the exact-mode oracle says the numbers are real.
The structural finding is that overflow-driven fill has no steady state: it sweeps 0.82 to 0.97 across each doubling cycle, and the chain tail rides that sweep, so the doc policy crosses the 0.1% chain2 red line by 13x at cycle top (1.32% at the 1e7 point).
lf85 is the bake: chain2 stays at or under 0.043% at every sweep point through the whole cycle for at most 0.05 B/key over the doc policy.
Single-link chains are structural under every policy (5 to 35%), so the cold-index slice prices them instead of chasing them: probe the base chunk first, read the link only on a base miss with the chain flag set, and the read-path IO-count test asserts exactly 3 group reads unchained and exactly 4 on a forced chain.
Split storms need no pacing: the worst 65536-insert window splits 5.8% of inserts, each a local CoW of two or three chunks.
PRED-SQLO1-B2-INDEXRAM confirmed, PRED-SQLO1-B2-READPATH failed as filed; the causal stories are next to the failing numbers in results/sqlo1/b2-predictions.md and the full verdict is results/sqlo1/b2-chunkindex.md.
