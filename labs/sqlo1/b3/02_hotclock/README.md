# hotclock-b: promotion and sampling constants against real cold reads

Milestone B3 lab 02 (spec 2064/sqlo1 doc 04 sections 4 and 8, doc 13 lab table).
Re-verdict of the S1 hotclock lab: those verdicts were hit ratios against no store at all.

## Question

The S1 lab baked promotion probability D=0.125, eviction sample K=64, and two stamps per class on hit ratios alone.
A miss now costs three group preads through the real sqlo1b cold index.
Do the constants survive when the metric is the time the runtime actually pays?

## Method

The tier model is byte-for-byte the S1 simulator: dense slot array behind a key-to-slot map, WATT-lite two-stamp scoring with writes 2x, ghost ring at 1/16 of capacity, plain clock baseline, coarse 1024-op ticks.
Ratio deltas against the S1 README are therefore model-free.

The B-side change: the whole keyspace (1M keys, 128 B) is preloaded into a real sqlo1b file and checkpointed, and every point-read miss performs an actual timed store Get.
Cells report hit-ratio/amortized-cold-ns-per-point-read; the nanoseconds decide.
A constant that moves the ratio but not the nanoseconds does not get to move the default.

Scan-burst touches update tier state through the same read path but skip the store read: the page cache is fully warm either way and the burst's job is flushing the hot set, not spending IO.
Writes stay tier-only, because the write path never reads the store.
Cold reads here land in the OS page cache (the file is ~150 MB), so the priced miss is the warm-file case; the disk-cold case is priced by the read-path IO-count pin (exactly 3 group reads) and the device's pread latency, which scale the same verdict.

Sweeps, same grid as S1: promotion D {0, 0.125, 0.25, 0.5, 0.75, 1.0}, sample K {16..256}, policy {clock, watt2, watt3}, over four traces (zipfian and scan-mix, each with a read-only arm).

## Run

    go run .            # full sweep, gate box (minutes: real reads on every miss)
    go run . -quick     # smoke
    go test ./...       # qualitative pins over a tiny real store

## Results

Pending: runs on the gate box.

## Verdict

Pending.
Predictions in results/sqlo1/b3-predictions.md, filed before the verdict run.
