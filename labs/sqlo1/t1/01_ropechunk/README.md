# ropechunk: rope chunk size sweep

Milestone T1 lab 01 (spec 2064/sqlo1 doc 05 section 1.1).

## Question

What fixed chunk size should log2chunk default to?
Doc 05 defaults to 32 KiB and this lab has to confirm or move it before T1 slice 1 bakes the constant.
One size must serve strings and bitmaps both, because Redis semantics make a bitmap a string, so the sweep covers 8/16/32/64 KiB against SETRANGE-heavy, APPEND-heavy, SETBIT-heavy, and GETRANGE-heavy mixes.
Since B3 the suite runs on both backends: -store a is the SQLite schema below, -store b the same rope over sqlo1b records, and the verdict reads from the arm that will actually carry the constant.

## Method

The rope is the doc 05 model expressed as Track A rows over the real chunk schema: root length in kv, fixed-size chunks in chunk keyed (k, cid), offset arithmetic instead of a fence table, absent chunks reading as zeros, the last row trimmed to the logical length, and the pc popcount column recomputed for dirty chunks when the bitmap mix flushes.
Writes go through a coalescing dirty-chunk overlay flushed in drain-shaped transactions at the engine's 8 MiB threshold, because production never pays per-op transactions; the overlay empties after each flush, so every fresh chunk touch pays the read-modify base from the store, which is the cold half of the trade.
An oracle test drives the model on both arms against a flat byte-slice reference (mid-stream reads, then a store-only readback with pc and row-length checks), so the sweep cannot time a rope that computes the wrong bytes.
On the b arm chunks are segment subkey records under a minted rooth, the popcount lives in the doc 05 kind 2 cache segments drained in the same batch, a flush is one DrainBatch, and the checkpoint cadence calls the store's checkpoint.

Each mix is 90/10 heavy op against the opposite reader, per-op cost timed into its class: the write row is the in-RAM mutate plus any RMW base read, the read row goes overlay then SQL, and the flush row carries the amortized IO bill (worst stall in max_ns, peak WAL, final file size, VmHWM).
Write amplification is flushed bytes over logical bytes written, so a one-bit SETBIT that drains a whole chunk shows up as the chunk-size multiple that it is, discounted by whatever coalescing the distribution earns.
The dataset is 32 keys at 8 MiB (256 MiB, past the 32 MiB page cache); the append mix preloads one chunk per key so the growth path is what gets measured.
Page size and cache are pinned at the current spec defaults (8192, 32 MiB); the apragma lab owns those knobs and the chunk-size crossover is read within a cell.

Read the sweep as: GETRANGE cost falls with chunk size (fewer row probes per span), SETRANGE/SETBIT write amplification rises with it (bigger images per drain), and the knee that balances the four mixes is the default; a zipf SETBIT arm shows how much hot-chunk coalescing pays back the bigger image.

## Run

    ./run.sh            # both arms x {8,16,32,64 KiB} x 4 mixes uniform + setbit zipf, gate box
    go run . -quick     # smoke (add -store b for the Track B arm)
    go test ./...       # smoke all mixes plus the oracle test, both arms

## Results

Local provisional run 2026-07-16 on the dev Mac, both arms, full grid: results/sqlo1/t1-ropechunk-local.csv, read in results/sqlo1/t1-ropechunk-local.md.
Gate-box confirmation still pending behind the A2 queue; it owns VmHWM and should extend the sweep with a 4 KiB arm.

## Verdict

Provisional: log2chunk defaults to 13 (8 KiB), rejecting the doc 05 prior of 15.
8 KiB won every mix on both arms with no knee in range: per-op cost and p99 rise 1.6 to 1.9x per chunk doubling, WA doubles per doubling by arithmetic, the zipf coalescing discount runs the wrong way for big chunks (35% at 8 KiB, 12% at 64 KiB), and the 4 KiB GETRANGE span never rewards bigger chunks with fewer probes.
The default stays a lab-movable constant behind the superblock field; the box run can move it back up if the 4x segment count prices the memory bar down.
