# coldlatency: first cut of the cold point-read distribution

## Question

Doc 05 section 9 holds cold point reads to p50 25 to 45 ms with the p99 within 2.5x p50 hedged, at one GET.
O1c serves cold points with no cache, no NVMe, and no hedging in front, so this lab takes the first cut on the simulator: the unhedged end-to-end distribution against block size and placement, and the I/O-pool queueing knee against offered cold-read rate.
The O4a re-verdict puts the cache and hedging in front; this lab prices what they have to buy back.

## Method

A cold point read composes as first byte plus transfer plus decode: the first byte draws from the sim envelope (sim.S3Standard.Get, pinned by test so the O5 E-cloud refit moves the lab), transfer is one block over a disclosed 64 MiB/s single-stream link assumption, and decode carries the #1097 zstd-1 cold-block rate at 2 GiB/s.
Express is the same lab assumption the strict-latency lab (#963) carried: p50 6 ms, p99 25 ms, no published tail, replaced at the O5 refit.
The keymap and directory halves are RAM in regime A and add nothing a simulator would resolve.
The point sweep draws 200000 samples per cell across placement {Standard, Express} x block {32, 64, 128, 256, 512} KiB.
The load sweep pushes Poisson arrivals through an FCFS pool of capped in-flight GETs (exact FCFS via earliest-free-server assignment), Standard at 128 KiB, cap {16, 64, 256} x utilization {0.3, 0.6, 0.8, 0.9, 0.95}, with rates derived from the measured mean service so cells are utilization bands rather than absolute-rate bets.

No smoke of the sweep ran before these predictions; the unit tests exercised the queue mechanics on deterministic toy values only.

## PRED-OBS1-O1C-COLDLAT (filed before the scored run)

1. Standard at 128 KiB unhedged: p50 21 to 24 ms, p99 145 to 165 ms, tail ratio 6 to 7.5x. The section 9 p50 band is met on its fast side, and the 2.5x p99 row is unreachable without hedging: the O4a hedge has to cut roughly a 7x ratio to 2.5x.
2. Block size is not a point-latency knob on Standard: p50 spans about 7.5 ms from 32 to 512 KiB, pure transfer arithmetic at the assumed link. On Express the same 7.5 ms is comparable to the whole p50 (roughly 6.6 to 14 ms across the sweep), so if Express placement ever matters for points, block size becomes a latency knob there; flag for O4a.
3. Express at 128 KiB: p50 7.6 to 8.4 ms, p99 26 to 29 ms, tail ratio 3.2 to 3.6x, meaning even Express needs hedging to reach the 2.5x row.
4. Queueing knee by utilization, not cap: at caps 64 and 256 the e2e p99 stays within 1.3x of the unloaded p99 through utilization 0.85, and blows past 2x unloaded by 0.95. Cap 16 loses pooling and shows wait inflation earlier: p99 above 1.3x unloaded already at 0.8. In rates, the 64-cap default carries roughly 1600 to 1800 cold reads/s per node at healthy tails (ceiling about 2000 to 2100/s from the measured mean service), and raising the cap to 256 buys about 4x the rate at equal tail for zero request dollars.
5. Decode is noise and the ceiling arithmetic closes: decode under 0.3% of the Standard p50 at 128 KiB, and the printed mean service implies a Little ceiling within 5% of cap divided by mean service.

## Run

    ./run.sh            # full sweep, writes coldlatency.csv
    go run . -quick     # smoke

## Results

Full sweep in coldlatency.csv (10 point cells, 15 load cells).

Standard at 128 KiB unhedged: p50 22.01 ms, p99 151.64 ms, tail ratio 6.89x.
Standard p50 across 32 to 512 KiB: 20.52 to 28.01 ms, a 7.49 ms transfer-arithmetic span; Express p50 spans 6.51 to 14.04 ms over the same blocks.
Express at 128 KiB: p50 8.01 ms, p99 27.11 ms, tail ratio 3.39x.
Load cells at 128 KiB Standard, measured mean service 31.08 ms (Little ceilings 515, 2059, and 8236 per second at caps 16, 64, 256): cap 64 holds e2e p99 within 1.04x of unloaded through utilization 0.95 (1956/s) and p50 within 1.27x; cap 256 is flat everywhere; cap 16 reaches p99 218.74 ms (1.44x unloaded) and p50 49.04 ms (2.2x) at 0.95.
Decode is 0.28% of the Standard p50.

Scoring: predictions 1, 3, and 5 HIT on every band; prediction 2 HIT (the Express p50 floor came in at 6.51 ms against the roughly-6.6 edge, inside the stated roughness).
Prediction 4 MIXED: the rate and ceiling claims hit (the 64-cap default carries 1850/s at healthy tails, ceiling 2059/s, cap 256 buys 4x for zero request dollars), but the predicted knee shape is wrong.
The e2e p99 never blows past 2x unloaded at any cap or utilization swept, because the service tail dominates the p99 until waits reach the same 150 ms scale; the in-flight cap taxes the MEDIAN first (cap 16 at 0.95: p50 2.2x, p99 1.44x), and the tail ratio actually compresses under load (6.89x down to 4.46x) as mean-scale waits pad the p50.

## Verdict

The unhedged first cut lands where doc 05 needs it: cold point p50 22 ms at the default block, on the fast side of the section 9 band, one GET, and decode and transfer are noise against the first-byte envelope.
The 2.5x p99 row is unreachable without hedging from either placement: the O4a hedge has to close 6.9x on Standard and 3.4x even on Express, which quantifies exactly what the section 6 machinery must buy.
Block size stays settled by the block-size lab's dollar argument on Standard (2 ms of p50 between 32 and 128 KiB is immaterial), but on Express block size is a genuine latency knob (p50 6.5 to 14 ms across the sweep), so an Express deployment that cares about cold p50 wants the small end; flagged for O4a.
The pool-cap finding for the async-cold-read slice: saturation shows up as median inflation and wait growth, not p99 blowup, so pool health monitoring should watch p50 drift and wait quantiles rather than e2e p99, and the 64-cap default is comfortable at design cold-read rates (1850/s per node) with cap 256 a free 4x if a workload ever needs it.
