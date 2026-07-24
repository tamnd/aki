# Lab: die-in-RAM fractions under the baseline drainer

Question: when a short-TTL write burst runs under dirty pressure, what fraction of the volatile records could avoid a data write, and how is that recovery split between reap-cancel (drop a record that expired while queued) and drain reordering (defer volatile-near records so they get time to die)?
Spec ground: doc 11 sections 6 and 8; milestone 16-T7 lab 02; shapes slice 4.

## Setup

The harness runs the real Tiered-over-sqlo1b stack with a simulated clock the writer advances, so the drain interval is fixed by construction: the drainer fires on dirty bytes (8 MiB threshold) and the write rate converts that to time.
At 8 MB/s influx and 1 KiB values the interval is 1000 ms and one interval holds 8192 records.
A metering Store wrapper books every volatile put at the ApplyBatch door with the simulated apply time, which classifies each record into exactly one fate: drained dead (expired before its put applied, the reap-cancel win), drained alive with slack (deadline minus drain time, the reordering headroom), died in RAM (never drained, dead at run end), or pending (never drained, still alive, truncation residue).
Each arm writes 30 intervals of unique keys (245760 records), Ticks once per simulated second, and reports slack buckets at one and two intervals.

## Arms

- ratio: fixed TTL swept across {0.25, 0.5, 1, 2, 4, 8} times the drain interval.
- uniform: TTL uniform in (0, 2] intervals, the mixed-workload shape.
- vol50: every other key plain, checking the meter never misattributes non-volatile records.

## Run

    ./run.sh

Builds to /tmp, sweeps the arms, writes dieinram.csv, and prints the table to stderr.
Wall time is a few seconds total.

## Results (2026-07-24, M4 Mac, dieinram.csv)

| arm | ttl/interval | drained dead | died in RAM | drained alive | slack <= 1iv | slack <= 2iv |
|---|---|---|---|---|---|---|
| ratio | 0.25 | 97.1% | 2.1% | 0 | - | - |
| ratio | 0.5 | 97.1% | 1.2% | 0 | - | - |
| ratio | 1 | 0 | 0 | 97.1% | all | all |
| ratio | 2 | 0 | 0 | 97.1% | none | all |
| ratio | 4-8 | 0 | 0 | 97.1% | none | none |
| uniform | (0, 2] | 44.9% | 0.6% | 52.2% | 93.2% of alive | all |
| vol50 | 1 | 0 | 0 | 97.1% | all | all |

Drain lag p50 is 927 ms against the 1000 ms interval in every arm, so the FIFO queue delivers records to the store almost exactly one interval after their write, as the threshold math predicts.
The residual ~2.9% per arm is the final interval's worth of dirty records the run end truncates (pending), plus at sub-interval TTLs a small died-in-RAM slice from the same tail.

## Verdict

1. The baseline gets almost no free die-in-RAM win. Even at TTL a quarter of the interval, only 2.1% of records die in RAM; 97% drain as already-dead puts. Dirty records cannot be evicted and the drainer writes them unconditionally, so expiry during the queue wait recovers nothing today. PRED-SQLO1-T7-DIEINRAM (80% of sub-window TTLs never hit disk) is unreachable without slice 4.
2. Reap-cancel is the first-order lever and it is a cliff, not a slope. For any TTL below the drain interval it recovers ~97% of all volatile writes by itself (everything except the truncation tail), because the FIFO lag is already a full interval. It costs one expiry check per drained op and needs no queue restructuring.
3. Reordering pays only in the one-to-two-interval TTL band. At TTL exactly one interval every record drains alive with slack under one interval: one deferral window turns all of them into reap-cancels. At two intervals it takes the second deferral. Past two intervals the records genuinely outlive their RAM stay and nothing should save them.
4. On the mixed workload the split is 45% reap-cancel alone, 94% with one interval of deferral, 98% with two. So slice 4 ships both levers, with reap-cancel as the correctness-simple backstop and reordering bounded at about two windows of deferral, beyond which holding a record dirty just inflates the queue for no recovery.
5. The vol50 arm matches the all-volatile arm exactly on the volatile half, so the meter and the drain path are indifferent to interleaved plain keys.

Numbers are local (simulated clock, M4 Mac); the gate box re-run rides the T7 exit gate (#170), but the shape here is structural (FIFO lag equals one threshold of bytes) and does not depend on hardware.
