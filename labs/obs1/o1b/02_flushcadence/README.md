# flushcadence: request rate, dollars, and the knee

Milestone O1b lab 02 (spec 2064/obs1 doc 04 sections 4.1 and 12, doc 11 section 4).

## Question

What request rate, monthly request bill, and realized commit lag do the flusher triggers produce across flush-age 5/50/250/1000ms and flush-size 1/8/64 MiB under trickle, steady, and saturating ingest, and where does the size trigger take the cadence over from the age trigger?
This gates the 8 MiB / 50ms defaults and the thrift 250ms profile before the WAL slice bakes them.

## Prediction (PRED-OBS1-O1B-CADENCE, filed before the scored run)

Numbers below derive from doc 04 section 4.1 arithmetic and the sim's S3Standard latency model (PUT lognormal p50 35ms p99 260ms, mean about 51ms, so the 4-deep pipeline tops out near 4/51ms = 78 PUTs/s).

1. Defaults (50ms, 8 MiB) under trickle: 20 flushes/s, total requests at or under the doc 04 worst case of 40/s (append batching keeps appends below the PUT count), request bill at or under the doc's $520/month.
2. Thrift (250ms, 8 MiB) under trickle: about 4 flushes/s, at or under 8 req/s, about $105/month against the doc's "about $100".
3. Commit lag at defaults: p50 around 120 to 180ms, p99 in the 300 to 450ms band, under the additive bound flush-age plus PUT p99 plus append p99 = 570ms on this model. That lands ABOVE the doc 04 section 3.1 "about 250ms" sentence, and the reading there is that the sentence rides the pre-refit latency envelope (the sim's PUT tail is assumed as heavy as the GET tail until the O5 E-cloud fit); if the measured p99 confirms this, the constant that moves is the section 3.1 window sentence at refit time, not the trigger defaults.
4. The knee sits at flush-size/flush-age (160 MiB/s at defaults): the 200 MiB/s heavy rows show cadence decoupled from the age knob at ingest/flush-size = 25 flushes/s for 8 MiB, near-identical across age 50/250/1000.
5. 1 MiB flush-size cannot carry 200 MiB/s: the pipeline ceiling is about 78 PUTs/s times 1 MiB, so achieved ingest parks near 78 MiB/s with nonzero parks and nothing dropped.
6. The 5ms rows (the barrier-floor proxy: a barrier storm degenerates to age 5ms) run pipeline-bound near 75 to 85 flushes/s at $1100 to $1300/month, which is why the floor exists and why barrier demand must stay demand-driven.

Disclosure: the model was debugged with -quick smoke sweeps before this prediction was frozen; the smoke exposed an early-draft artifact (age triggers swapping while all four PUT slots were busy, queueing unbounded tiny objects the swap-and-continue flusher cannot produce) that was fixed by gating swaps on a free slot. The scored run is the full sweep below.

## Method

Virtual-time discrete-event model of the doc 04 section 4 flusher, sim-only for the clock-skew lab's reason: the quantities are trigger arithmetic and queueing, and real-time runs at 1000ms flush-age would need hours per cell.
The model is pinned to the engine's numbers, not free-floating: latency draws use sim.S3Standard.Put through the same lognormal quantile mapping (unit test pins the copy), and dollars go through sim.S3StandardPrices.Bill, so the O5 E-cloud refit moves this lab automatically.
Loads: trickle 100 ops/s at 100 B (at least one frame per flush window for every age at or above 10ms, realizing the age-trigger worst case), steady 1 MiB/s, heavy 200 MiB/s (past the defaults' knee).
Each cell runs 10s virtual warmup plus 120s measured; rates count PUTs and chain appends started in the window, lag is arrival to covering chain commit for frames arriving in the window.
The lag column is the relaxed-mode loss window realized, and exactly what a strict ack would park on.

## Run

    ./run.sh            # full sweep into flushcadence.csv
    go run . -quick     # tiny windows, smoke only

## Results

Pending the scored run; this section lands in the results commit.
