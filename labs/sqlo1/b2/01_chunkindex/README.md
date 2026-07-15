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

Pending.

## Verdict

Pending.
