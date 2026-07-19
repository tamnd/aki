# m7/05 value-log group-commit window

The readback gate row M7-G3 owes: the write-under-spill path's one in-engine knob, priced on the real store.

## The question

When a write lands past the resident cap, aki appends the value's bytes to the shard's value log.
An evicting rival at the same cap drops the value instead, so it pays no disk write at all.
G3 measured aki's raw write rate under spill at 0.12-0.33x that rival and asked whether the gap is an unbuilt lever or the sequential-write floor a durable store pays.

The one knob on that path is the pending buffer's flush window (`vlogFlushBytes`, the store field `flushAt`, tuned here through `TuneVlogFlush`).
A spilled value copies into the buffer and the buffer hits the disk in one pwrite per window.
A tiny window pwrites almost every value alone, so the syscall-plus-seek cost dominates and the rate is syscall-bound.
A large window coalesces many values into one sequential pwrite, so the syscall cost amortizes toward the disk's sequential bandwidth.
This lab sweeps the window across four orders of magnitude on the real store and finds the knee.

## Run

    go run ./labs/f3/m7/05_group_commit          # full sweep
    go run ./labs/f3/m7/05_group_commit -quick   # smaller fill

## Numbers (2026-07-19, mac local disk, cap 16 MiB, 1 KiB values, 300k keys, ~18x the cap spilled)

| flush window | sets/sec | vs 1 MiB | MB/s spill |
|---|---|---|---|
| 1 B (a pwrite per value) | 129623 | 0.14x | 128 |
| 4 KiB | 155744 | 0.16x | 153 |
| 16 KiB | 250055 | 0.26x | 246 |
| 64 KiB | 593045 | 0.63x | 584 |
| 256 KiB | 899922 | 0.95x | 886 |
| 1 MiB (shipped) | 945376 | 1.00x | 930 |
| 4 MiB | 787055 | 0.83x | 775 |
| 16 MiB | 640015 | 0.68x | 630 |

The knee sits between 64 KiB and 256 KiB.
The shipped 1 MiB window is the plateau optimum: 7.3x the byte-1 syscall-bound rate, and larger windows regress because the bigger pending buffer costs more copy and GC than the extra coalescing saves.

## Verdict (feeds M7-G3)

The coalescing is saturated at the shipped window.
Past the knee, more window buys nothing, and 1 MiB is already the optimum, so the syscall cost the window exists to hide is amortized away.
The residual write cost is the sequential write itself, the physical cost of keeping the value on disk, which an evicting rival never pays because it drops the value.

That is the G3 structural finding under the LTM fair-proof rule (spec 2064/f3, "never raw ops vs evicting rivals").
The one deeper in-engine lever, the reactor-boundary group-commit that stages a whole command group and resolves once (bands.go:127-130, an owed reactor-side follow-up that needs a hook the store cannot reach on its own), moves the pwrite off the owner thread.
That cuts owner-stall latency and overlaps the write with the next batch, but it cannot lift the sustained spill rate above the sequential-bandwidth floor this sweep already reaches.
So G3's raw-write deficit against a data-discarding cache is structural: the fair proof is the memory-and-coverage arm (M7-G1/G2), which aki wins.

The lab is real-store, not a model: it drives `engine/f3/store` Set under a live value log so the number is the write path itself. `main_test.go` pins the knee shape for CI.
