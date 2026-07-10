# Lab 18: wake-batched completion path (M10 pull-forward slice 4)

The question: slice 3's reactor pays one owner-to-loop eventfd write per dirty connection per worker drain pass.
Slice 4 batches that edge: each claim marks its connection dirty on its loop, and the pass ends with one eventfd write per touched loop.
This lab sweeps connections per loop against pipeline depth on GET 64B and reads the akinet counters: conn wakes per op (the claims, which is exactly what the unbatched path wrote to eventfd), loop wakes per op (the writes the batched path actually pays), and throughput.

## Prediction (filed before measuring)

The m10-pullforward plan predicts, on the gate box at P16/512 conns, the wake band dropping from ~383 ns/op toward under ~80, moving GET 64B from ~1.24x to roughly 1.5-1.65x.
The mechanism this lab must show: eventfd writes per op drop from O(dirty conns) to O(touched loops), so loop wakes per op should sit well under conn wakes per op wherever a pass claims more than one connection, and the gap should widen with connection count (more conns per loop means more claims folded per write).
At P1 with few conns a pass rarely claims more than one connection, so the two counters should sit near each other and throughput should not regress.

## Gate box numbers

GamingPC WSL2, aki b7fb698, 2026-07-11, `taskset -c 0-15 go run . -dur 4s`, in process, 4 shards, 4 loops (the lab's fixed shape, kept from the container run for comparability; the shipping default is 3 per lab 19).

| conns | pipeline | ops/s | conn wakes/op | loop wakes/op | yield | writes/op |
|---|---|---|---|---|---|---|
| 8 | P1 | 81437 | 1.000 | 0.919 | 1.09x | 1.000 |
| 8 | P16 | 1046121 | 0.087 | 0.067 | 1.30x | 0.078 |
| 64 | P1 | 344183 | 1.000 | 0.463 | 2.16x | 1.000 |
| 64 | P16 | 4162516 | 0.098 | 0.026 | 3.84x | 0.082 |
| 256 | P1 | 871534 | 1.000 | 0.189 | 5.30x | 1.000 |
| 256 | P16 | 7633274 | 0.122 | 0.016 | 7.70x | 0.093 |
| 512 | P1 | 1182954 | 1.000 | 0.145 | 6.89x | 1.000 |
| 512 | P16 | 9045607 | 0.121 | 0.013 | 9.23x | 0.092 |

The prediction's mechanism lands on the box: the fold widens with connection count to 9.23x at the gate shape's 512/P16 (loop wakes 0.013/op against conn wakes 0.121/op), and the P1 rows sit near 1x at low conns exactly as filed.
The owed throughput A/B (main, wake-batched, vs the pre-batch d81a66b, both `-net reactor -net-loops 3`, GET 64B P16/512, redis-benchmark --threads 4, interleaved 3 reps per arm): old 6.642-6.647 Mops p99 1.39-1.46 ms, new 6.649 Mops (all three reps) p99 1.391 ms flat.
That is a tie at the harness plateau, not the predicted 1.24x-to-1.5x jump as an old-vs-new delta: at P16 the unbatched path only paid ~0.12 eventfd writes/op to begin with, so the batch's headroom at this depth is small and the campaign throughput claim rides the slice 7 driver A/B against the rivals, where the whole reactor (loops, batch, and all) is the arm.
The batched arm is never worse, holds the flatter tail, and at P1/512 (1.0 conn wakes/op unbatched) is where the fold pays.
A second interleaved A/B at exactly that shape (GET 64B P1/512, redis-benchmark --threads 4, n=3M, 3 reps per arm) reads the payment in latency, not throughput: both arms plateau at 855.2-855.9 krps (the client is the limiter at P1), but the batched arm holds avg 0.423-0.424 ms and p99 0.855-0.871 ms against the unbatched 0.454-0.464 ms avg and 0.903-0.919 ms p99, an ~8% avg and ~6% p99 cut from deleting most of the per-op eventfd writes.

## Container numbers (2026-07-10, superseded by the box table above)

Numbers below are from a podman golang:1.26 container (applehv VM, 5 CPUs, 6GiB) on the darwin dev box, not the gate box; they show the counter mechanism, not the campaign ratios.
The gate box A/B they owed is delivered in the box section above.

GET 64B, 1M keys, 4 shards, 4 loops, 4s per cell, in process, `-dur 4s`.

| conns | pipeline | ops/s | conn wakes/op | loop wakes/op | yield | writes/op |
|---|---|---|---|---|---|---|
| 8 | P1 | 80075 | 1.000 | 0.883 | 1.13x | 1.000 |
| 8 | P16 | 738281 | 0.147 | 0.083 | 1.77x | 0.105 |
| 64 | P1 | 202915 | 1.000 | 0.259 | 3.86x | 1.000 |
| 64 | P16 | 1542508 | 0.169 | 0.025 | 6.77x | 0.109 |
| 256 | P1 | 324915 | 1.000 | 0.132 | 7.56x | 1.000 |
| 256 | P16 | 2180952 | 0.182 | 0.018 | 9.96x | 0.110 |
| 512 | P1 | 360327 | 1.000 | 0.111 | 9.00x | 1.000 |
| 512 | P16 | 2241119 | 0.189 | 0.017 | 11.42x | 0.112 |

## Verdict (container, superseded)

The mechanism holds and moves the way the prediction said it must: the fold widens with connection count, from 1.13x at 8 conns P1 (a pass rarely claims more than one connection, the two counters sit near each other) to 11.42x at the gate shape's 512/P16, where the eventfd writes per op drop from the 0.189 the unbatched path paid to 0.017, O(touched loops) instead of O(dirty conns).
Throughput in the VM is loop-count and CPU bound, so the ops/s column is not campaign evidence; the throughput claim (GET 64B ~1.24x toward 1.5-1.65x at P16/512) stays with the owed gate box A/B.
