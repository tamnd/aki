# bitcount: popcount cache on and off

Milestone T1 lab 02 (spec 2064/sqlo1 doc 05 sections 3.2 and 8).

## Question

Does the popcount cache make cold BITCOUNT cost the cache instead of the bitmap, and does the Track A pc column need to sit ahead of the blob to deliver it?
Doc 05 claims a full BITCOUNT reads cache state, not chunks, and the T1 prediction wants at least 100x fewer bytes than a scan on 512 MiB; slice 7 bakes whatever this lab verdicts.
Since B3 the suite runs on both backends: -store a is the SQLite schema below, -store b the same chunks over sqlo1b records, and the cold-curve verdict reads from the arm that will actually carry the cache.

## Method

One bitmap key stored as rope chunks over the real Track A shape, random bytes so counts are unpredictable, pc computed per chunk at preload exactly as a drain would.
Two arms per range shape: cache is sum(pc) over interior chunks plus the one or two edge chunks read raw and trimmed to the addressed bytes; scan reads every overlapping blob.
Shapes are the whole key, a 64 KiB span in the middle, and an unaligned middle half.
Cold puts every rep behind a fresh open (the apragma caveat applies: that drops the SQLite cache, not the OS cache, so cross-arm ratios are the read); hot times repeats on one warm connection.
Correctness is asserted while the clock runs: both arms and every rep must agree per shape, and the full count must equal the popcount accumulated at preload, so a cache that answers fast but wrong cannot win.
An oracle test pins the range decomposition on both arms against a flat in-RAM reference over hundreds of random byte ranges, reading stored state through the store surface alone.

On the b arm chunks are segment subkey records under a minted rooth and the popcount cache is not a column: doc 05 section 3.2 kind 2 cache segments hold one little-endian u32 per chunk, 1024 chunks to a segment, written in the same DrainBatch as the chunks they cover.
Cache mode there reads the covering cache segments plus the two edge chunk records, scan mode reads every chunk record, and cold closes and reopens the store after the load checkpoint so each rep rebuilds from the settled file instead of reading the writer's dirty RAM.
The layout sweep stays SQLite-only, so -store b accepts only the default pclast and run.sh sweeps pcfirst on the a arm alone.

The layout sweep is the Track A twist: SQLite stores record bytes in declared order, so with the shipped chunk (k, cid, v, pc) the pc value sits at the end of each 32 KiB blob's overflow chain and sum(pc) may walk the whole chain per row, while (k, cid, pc, v) keeps it in the leaf-local prefix.
If pcfirst beats pclast materially on the cold cache arm, the verdict is a schema column reorder before T1 slice 7 lands the cache maintenance.

Read the sweep as: the cold cache rows across 1/16/128/512 MiB are the curve (they must scale like row visits, not bytes); the cold scan rows are the O(bytes) baseline; the full-cache pclast/pcfirst delta is the layout verdict.

## Run

    ./run.sh            # both arms x {1, 16, 128, 512 MiB}, plus pcfirst on arm a, gate box
    go run . -quick     # smoke (add -store b for the Track B arm)
    go test ./...       # both-arm smoke plus the range oracle

## Results

Pending: runs on the gate box after the A2 queue frees it.

## Verdict

Pending.
