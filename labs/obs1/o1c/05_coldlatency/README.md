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

Pending the scored run.

## Verdict

Pending the scored run.
