# Lab: hop batch cap

Spec 2064/f3/03 section 3.2, M0 lab 2.

## The question

The hop transport carries commands to owner workers in batch nodes published with one atomic tail swap, and doc 03 starts the node's command capacity at `batchCap = 32` (PRED-X8 pre-registers a {16, 32, 64} sweep). How big should the cap be? Bigger batches amortize the producer's swap and the consumer's pop over more commands; the cost side is head-of-line waiting inside a batch and a fatter node.

## Method

`go run .` stands up one consumer worker (LockOSThread) owning an `engine/f3/store`, and four producers filling batch nodes of cap B with random GETs and publishing them through a Vyukov intrusive MPSC queue, the exact publish discipline of the hop transport. Nodes recycle through per-producer rings; outstanding work is bounded per producer in commands (1024), not nodes, so queue depth is identical at every B and does not confound the sweep. Every 64th command is stamped at fill time and the consumer records completion, giving in-queue latency under saturation. 8M commands per configuration, B swept over 1, 4, 8, 16, 32, 64, 128, in two consumer regimes: a 64k keyspace where the GET is cache-warm and cheap, and a 1M keyspace where the GET misses to DRAM and dominates.

One reading note: under saturation with a fixed outstanding bound, in-queue p50 is mostly Little's law (4096 outstanding commands divided by throughput), so the latency columns largely re-express the throughput column; the residual is the head-of-line component.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26, 4 producers, 64B values.

keys = 65536 (cache-warm consumer):

| cap | Mcmds/s | p50 in-queue | p99 in-queue |
|---|---|---|---|
| 1 | 9.9 | 343µs | 1.493ms |
| 4 | 15.7 | 200µs | 1.208ms |
| 8 | 17.4 | 183µs | 1.209ms |
| 16 | 18.0 | 180µs | 1.133ms |
| 32 | 19.5 | 168µs | 986µs |
| 64 | 18.5 | 166µs | 1.09ms |
| 128 | 19.3 | 159µs | 1.012ms |

keys = 1048576 (memory-bound consumer):

| cap | Mcmds/s | p50 in-queue | p99 in-queue |
|---|---|---|---|
| 1 | 3.3 | 1.071ms | 2.259ms |
| 4 | 3.7 | 945µs | 2.037ms |
| 8 | 3.8 | 932µs | 1.997ms |
| 16 | 3.8 | 919µs | 1.997ms |
| 32 | 3.8 | 905µs | 2.006ms |
| 64 | 3.9 | 866µs | 1.909ms |
| 128 | 3.9 | 854µs | 1.868ms |

The shape: with a cheap consumer, cap 1 leaves half the throughput on the table (9.9 vs 19.5 Mcmds/s) and the curve climbs steeply to 8, keeps paying to 32, and is flat within noise after that. With a memory-bound consumer the queue is never the bottleneck and the whole sweep moves throughput by under 20 percent, all of it below cap 8. Nothing in either regime punishes a larger cap at these depths, because the latency cost of batching is bounded by the pipeline depth clients actually run, not by the cap.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. The doc 03 default `batchCap = 32` holds: it sits at the top of the throughput curve in the regime where the queue matters, covers the P16 gate depth in one node, and going to 64 or 128 buys nothing measurable while doubling or quadrupling node size against the two-page bound. Caps below 8 are the only real mistake available. The gate box rerun matters here mainly for the cache-warm regime, which is exactly the regime M0's prefetched batch drain (lab 04) is trying to create.
