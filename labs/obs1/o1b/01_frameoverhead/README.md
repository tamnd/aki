# frameoverhead: the K1 number

Milestone O1b lab 01 (spec 2064/obs1 doc 04 sections 1 and 12, doc 11 section 4).

## Question

Doc 04 budgets the one thing obs1 adds to the owner's hot path, encoding the op frame into the group WAL buffer, at under 100ns for small ops.
K1 is the milestone kill switch: if the measured hot tax cannot sit at or under 10 percent, the milestone stops and frames get redesigned.
This lab produces the number before the op-frames slice bakes the encoder.

## Prediction (filed before the measured run)

PRED-OBS1-O1B-HOTTAX, the lab's local rung: the frame append costs under 100ns per op at 16 to 256 byte values on the M-series box, and under 5 percent of wire-inclusive per-op cost projected against the paritysmoke rates (1.7 to 2.2M ops/s pipelined means roughly 500 to 600ns of wall per op, so 100ns of encode could not exceed about 20 percent even unamortized, and the measured number should project well under 5).
In-process the percentage against a bare SetString will read much larger than the hot-gate tax, since a RAM apply is itself only a few hundred nanoseconds; the budget line is the absolute nanoseconds, and the doc 11 gate line is the projection.

## Method

In-process, no server, no wire.
The substrate is the ported store, byte-identical to f3's by the O1a port contract, so the apply-only arm is the f3 baseline on shared bytes and the index tier-tag branch (also byte-identical) taxes zero by construction.
Both arms run the same uniform SetString loop over 128k resident keys; the frame arm additionally appends a strset frame in the doc 03 section 4 byte layout with the doc 04 strset payload, into a preallocated buffer that resets at the 8 MiB flush-size default.
The slot is a mask, not a CRC16, because dispatch already computed the real slot before the owner runs.
Arms alternate every 16 batches of 4096 ops and the statistic is the median per-batch ns/op, the loaded-box lesson from the paritysmoke lab.

## Run

    ./run.sh            # full sweep into frameoverhead.csv
    go run . -quick     # tiny counts, smoke only

## Results

Full sweep in frameoverhead.csv, run 2026-07-17 on the M-series dev box, 256 batches of 4096 ops per arm per size, medians of per-batch ns/op.

| value bytes | base ns/op | frame ns/op | delta ns | in-process delta |
|-------------|-----------|-------------|----------|------------------|
| 16 | 36.5 | 38.8 | 2.2 | 6.1% |
| 64 | 36.7 | 39.6 | 2.9 | 7.8% |
| 256 | 40.0 | 50.2 | 10.2 | 25.6% |
| 1024 | 83.2 | 118.7 | 35.5 | 42.7% |

The delta scales with value size because a logical WAL copies the value bytes into the buffer once; 35.5ns for a 1 KiB copy plus the 18-byte fixed part is memcpy speed, there is no constant to tune away.
An earlier draft ran the base arm first inside every alternation round and read the frame arm as faster than base; the lead arm now alternates too, and that artifact is gone.

## Verdict

PRED-OBS1-O1B-HOTTAX scores HIT.
The budget line holds with room: 2.2 to 10.2ns at 16 to 256 byte values against the 100ns doc 04 budget, and even the 1 KiB row sits at a third of it.
The gate line projects comfortably under 5 percent: paritysmoke measured 1.7 to 2.2M ops/s wire-inclusive on this box, roughly 500 to 600ns of wall per op, so 2.9ns at 64 bytes is about half a percent, and the 1 KiB row projects around 3 to 4 percent against the proportionally slower wire path for values that size.
K1 verdict: the tax is nowhere near the 10 percent kill line, frames stay as designed, and the op-frames slice can bake this encoder shape.
The in-process percentages in the table are real but are the wrong denominator for K1, which is why the projection line exists; the binding evidence stays with the F9-class gate runs on the box.
