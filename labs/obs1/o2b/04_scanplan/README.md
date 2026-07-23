# scanplan

## Question

Doc 05 section 3 makes three planner choices this lab gates before the scan slice lands: full-collection scans coalesce adjacent blocks into 8 to 16 MiB range GETs instead of per-block reads (a 10 GiB collection in about 640 GETs), they fan out up to scan-fan parallel GETs with a default of 8, and scan plus readahead blocks are admission-exempt from the RAM block cache so scans do not pollute it.

## Method

Four cell families:

- scan_*: a real 512 MiB cold object on the counting sim, streamed whole three ways (per 128 KiB block, 8 MiB ranges, 16 MiB ranges), each arm checksum-verified against the object.
- plan_*: the directory arithmetic for the doc 05 examples, 1 GiB at both range sizes and 10 GiB at 16 MiB.
- fan_*: an analytic wall-clock model over the 10 GiB 16 MiB plan at fan 1 to 32; ranges round-robin onto lanes, each range pays first-byte plus transfer, and the client NIC divides across lanes once they outrun it.
- adm_*: a compact S3-FIFO (probationary 10 percent, main, ghost at half of main's entries, lazy promotion) under a warm skewed point workload, then a full cold scan interleaved with point reads; arms are S3-FIFO with the scan exemption, S3-FIFO admitting scan blocks, and a naive LRU admitting everything. The score is point hit rate during the scan window and GETs bought.

## Envelope disclosure

The scan transfers and admission misses are real counting-sim traffic; the fan model and the cache are lab-local and the scan slice plus the O3 cache milestone replace them with landed planes.
The fan model constants are a disclosed fit to the doc 01 section 2.2 envelope: 20 ms first byte (GET p50 10 to 30 ms), 100 MB/s per connection (AWS guidance), 10 Gbps client NIC; the E-sim refit at O5 replaces them with measured distributions.
S3 serves one contiguous range per GET (doc 05 verbatim), which is why coalescing is a planner choice and not a request option.

## Prediction (PRED-OBS1-O2B-SCANPLAN)

Filed before the scored run.

1. The 512 MiB scan bills exactly 4096 per-block GETs vs 64 at 8 MiB and 32 at 16 MiB, all three arms checksum-exact, all transferring exactly 512 MiB; the plan rows land exactly at 128 and 64 for 1 GiB and 640 for 10 GiB.
2. Fan 1 to 8 speeds up 7.5x to 8.1x (near-linear, both first-byte and transfer parallelize under the NIC cap); the 8 to 16 doubling is the first to fall below 85 percent efficiency (at most 1.7x), and 16 to 32 buys at most 1.1x, so 8 is the last near-linear fan and the default is confirmed as the knee of diminishing returns.
3. The exemption never loses: the exempt arm's point hit rate is at least the admitting arm's, with the S3-FIFO delta small (0 to 2 points, the doorkeeper is scan-resistant by design and the residual damage is small-queue and ghost churn); the naive LRU reference arm lands at least 4 points below the exempt arm and buys at least 1.3x its point-miss GETs, which is what the doorkeeper plus exemption is worth over admit-everything.

Kill line: a scan GET count off the exact plan or a checksum mismatch, a fan curve where 8 to 16 is still near-linear (above 90 percent efficiency, which would argue the default should be 16), or an admitting arm that beats the exempt arm.

## Calibration disclosure

A quick 32 MiB configuration (64 cache slots, 512 scan blocks) shaped the harness before this prediction was filed: per-block 256 vs coalesce16 2 GETs, plans exact, fan 1 to 8 exactly 8.0x with 8 to 16 at 1.60x, exempt 0.9282 vs admit 0.9260 vs LRU 0.8448 point hit rate.
The fan model is deterministic and scale-independent, so its full-size rows repeat the calibration; the 512 MiB scan arms and the full-size admission cell had not been run when the bands were set.

## Run

```
./run.sh
```

## Results

```
cell,gets,mib_or_sec,extra
scan_perblock,4096,512.00,
scan_coalesce8,64,512.00,
scan_coalesce16,32,512.00,
plan_1gib_8mib,128,,
plan_1gib_16mib,64,,
plan_10gib_16mib,640,,
fan_1,,120.17,throughput 89 MB/s
fan_2,,60.09,throughput 179 MB/s
fan_4,,30.04,throughput 357 MB/s
fan_8,,15.02,throughput 715 MB/s
fan_16,,9.39,throughput 1144 MB/s
fan_32,,8.99,throughput 1194 MB/s
adm_s3fifo_exempt,12328,,point_hit 0.9128
adm_s3fifo_admit,12363,,point_hit 0.9122
adm_lru_admit,18697,,point_hit 0.8206
```

## Verdict

HIT on all three bands.
The real 512 MiB scan billed exactly the plan in every arm, 4096 per-block GETs against 64 and 32 coalesced, all checksum-exact at exactly 512 MiB transferred, and the plan rows land the doc 05 examples exactly, 640 GETs for 10 GiB at 16 MiB.
The fan curve confirms the default: 1 to 8 is exactly 8.0x, the 8 to 16 doubling is the first below 85 percent efficiency (1.60x), and 16 to 32 buys 1.04x against the NIC ceiling, so 8 is the last near-linear fan under the disclosed doc 01 fit.
The exemption never loses: 0.9128 exempt vs 0.9122 admitting, a 0.06 point delta that shows the S3-FIFO doorkeeper is already scan-resistant by design (the residual damage is small-queue and ghost churn), while the naive LRU reference arm drops 9.2 points below the exempt arm and buys 1.52x its total GETs (1.77x on point misses alone), which is what the doorkeeper plus exemption is worth over admit-everything.
The scan slice and the O3 cache milestone replace the lab-local fan model and cache with landed planes; the E-sim refit at O5 replaces the latency constants with measured distributions.
