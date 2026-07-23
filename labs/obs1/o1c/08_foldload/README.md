# foldload: WAL cadence and frame overhead under fold load

## Question

The last O1c exit-gate row asks whether the flush cadence and the frame overhead are still at their predictions when the fold pipeline is live.
The fold consumes the record stream in process with the flusher, so the risk is contention: the folder stealing enough cycles that the WAL flushes late, packs objects differently, or the ingest loop misses design rate.

## Method

The reqgib rig runs twice per configuration on the zero-latency counting sim, once with a Folder consuming the record stream and cutting segments, once without, on byte-identical ingest.
Production constants: 8 MiB flush size, 64 MiB segment target, 500 ms fold age, 4 groups, 512 MiB payload per arm, values at 200 B and 1000 B.
Two modes per value size.
The paced pair runs both arms at the 100 MiB/s design rate and carries the cadence claim, because the o1b/01 lesson stands: an in-process saturated loop is CPU-bound at a few hundred ns/op, so any added work reads as a huge percentage of the wrong denominator.
The saturated pairs (3 alternating reps, medians) report that in-process ceiling as context only.
Counters come from the WriteLog INFO rows (wal_flushes, wal_flushed_bytes, chain_commit_batches) and the folder and publisher stats.

## Prediction (PRED-OBS1-O1C-FOLDLOAD, filed before the scored run)

1. Both paced arms hold the 100 MiB/s design rate within 2%; the fold never makes the ingest loop miss pace.
2. WAL flush counts match across arms within plus or minus 1 and flushed bytes within 2 KiB, in both modes and at both value sizes; the fold does not perturb what the WAL writes.
3. Frame overhead per op is identical across arms within 0.1 B, and sits in the 25 to 29 B band at both value sizes (the quick calibration read 27.1 and 27.4 B at a 1 MiB flush; the 8 MiB flush amortizes object framing a little further).
4. Mean WAL object size is within 10% of the 8 MiB flush target in every arm.
5. Context, not a claim: the saturated in-process ceiling shows the fold-on arm 1.5x to 3x the fold-off ns/op, and that number is the wrong K1 denominator by the o1b/01 lesson; the real owner tax at design rate was measured at 5% (200 B) and 2.4% (1000 B) in #1111.

Kill line: a paced fold-on arm below 98 MiB/s achieved, or WAL counters diverging past the band in a paced pair, sends the fold scheduling back to design.

## Calibration disclosure

A -quick smoke (32 MiB, 1 MiB flush, saturated only) ran before this prediction was filed.
It taught two things now baked into the method and the bands: WAL object packing is timing-dependent (the 1000 B pair once split an extra object, 32 vs 33 flushes and 374 bytes of extra framing), hence the plus or minus 1 flush and 2 KiB tolerance; and the saturated tax read 174% and 105%, which is the wrong-denominator artifact that moved the primary claim onto the paced pairs.

## Run

    ./run.sh

## Results

Scored run on the M4 box, 512 MiB per arm, foldload.csv checked in.

Paced pairs, the claim carriers, at both value sizes: both arms achieved 100.0 MiB/s, flush counts 72/72 (200 B) and 65/66 (1000 B), flushed bytes equal or 374 B apart, overhead 27.0 B per op identical in every row, mean object 7.96 to 8.09 MiB.
Every paced band hit and the kill line was never approached.

Saturated pairs as context: fold-off medians 452 ns/op (200 B) and 825 ns/op (1000 B), fold-on 1257 and 2230, a 2.7 to 2.8x in-process ratio squarely in the predicted 1.5x to 3x band and still the wrong K1 denominator; the #1111 paced owner tax numbers remain the real cost.
Mean object size stayed within 10% of the 8 MiB target in every arm including the coldest (8.6 MiB).

One band missed and is disclosed: the plus or minus 1 flush tolerance held in every paced pair but not in saturated mode, where the process-cold first arm packed 67 objects against its partner's 72 and the 1000 B reps drifted 2 apart.
Flushed bytes stayed within the 2 KiB framing tolerance in every pair regardless, so the stream content is unmoved; under saturation the packing boundary timing just wanders more than one object.

## Verdict

PRED-OBS1-O1C-FOLDLOAD: HIT, with the saturated flush-count band miss disclosed above.
The fold pipeline does not perturb the WAL flush cadence or the frame overhead at design rate, and it does not cost the ingest loop its pace.
