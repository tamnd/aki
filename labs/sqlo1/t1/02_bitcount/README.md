# bitcount: popcount cache on and off

Milestone T1 lab 02 (spec 2064/sqlo1 doc 05 sections 3.2 and 8).

## Question

Does the popcount cache make cold BITCOUNT cost the cache instead of the bitmap, and does the Track A pc column need to sit ahead of the blob to deliver it?
Doc 05 claims a full BITCOUNT reads cache state, not chunks, and the T1 prediction wants at least 100x fewer bytes than a scan on 512 MiB; slice 7 bakes whatever this lab verdicts.

## Method

One bitmap key stored as rope chunks over the real Track A shape, random bytes so counts are unpredictable, pc computed per chunk at preload exactly as a drain would.
Two arms per range shape: cache is sum(pc) over interior chunks plus the one or two edge chunks read raw and trimmed to the addressed bytes; scan reads every overlapping blob.
Shapes are the whole key, a 64 KiB span in the middle, and an unaligned middle half.
Cold puts every rep behind a fresh open (the apragma caveat applies: that drops the SQLite cache, not the OS cache, so cross-arm ratios are the read); hot times repeats on one warm connection.
Correctness is asserted while the clock runs: both arms and every rep must agree per shape, and the full count must equal the popcount accumulated at preload, so a cache that answers fast but wrong cannot win.
An oracle test pins the range decomposition against a flat in-RAM reference over hundreds of random byte ranges.

The layout sweep is the Track A twist: SQLite stores record bytes in declared order, so with the shipped chunk (k, cid, v, pc) the pc value sits at the end of each 32 KiB blob's overflow chain and sum(pc) may walk the whole chain per row, while (k, cid, pc, v) keeps it in the leaf-local prefix.
If pcfirst beats pclast materially on the cold cache arm, the verdict is a schema column reorder before T1 slice 7 lands the cache maintenance.

Read the sweep as: the cold cache rows across 1/16/128/512 MiB are the curve (they must scale like row visits, not bytes); the cold scan rows are the O(bytes) baseline; the full-cache pclast/pcfirst delta is the layout verdict.

## Run

    ./run.sh            # {pclast, pcfirst} x {1, 16, 128, 512 MiB}, gate box
    go run . -quick     # smoke
    go test ./...       # both-layout smoke plus the range oracle

## Results

Pending: runs on the gate box after the A2 queue frees it.

## Verdict

Pending.
