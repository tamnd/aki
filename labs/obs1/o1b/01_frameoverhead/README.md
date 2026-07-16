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

Pending the measured run; this section lands in the results commit.
