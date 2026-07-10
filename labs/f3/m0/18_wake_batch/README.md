# Lab 18: wake-batched completion path (M10 pull-forward slice 4)

The question: slice 3's reactor pays one owner-to-loop eventfd write per dirty connection per worker drain pass.
Slice 4 batches that edge: each claim marks its connection dirty on its loop, and the pass ends with one eventfd write per touched loop.
This lab sweeps connections per loop against pipeline depth on GET 64B and reads the akinet counters: conn wakes per op (the claims, which is exactly what the unbatched path wrote to eventfd), loop wakes per op (the writes the batched path actually pays), and throughput.

## Prediction (filed before measuring)

The m10-pullforward plan predicts, on the gate box at P16/512 conns, the wake band dropping from ~383 ns/op toward under ~80, moving GET 64B from ~1.24x to roughly 1.5-1.65x.
The mechanism this lab must show: eventfd writes per op drop from O(dirty conns) to O(touched loops), so loop wakes per op should sit well under conn wakes per op wherever a pass claims more than one connection, and the gap should widen with connection count (more conns per loop means more claims folded per write).
At P1 with few conns a pass rarely claims more than one connection, so the two counters should sit near each other and throughput should not regress.

## Container numbers (provisional)

Numbers below are from a podman golang container on the darwin dev box, not the gate box; they show the counter mechanism, not the campaign ratios.
The gate box A/B (P16/512 GET 64B against the slice 3 baseline) is owed and runs once the box frees up from the other campaign.

(pending: run `go run ./labs/f3/m0/18_wake_batch` in the container and paste the table)

## Verdict

(pending the container run; the box sweep stays owed either way)
