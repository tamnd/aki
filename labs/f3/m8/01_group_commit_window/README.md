# Lab 01: the group-commit window

Part of issue #550, the M8 `.aki` single-file milestone, lab 01, the group-commit window the spec lists as a lab knob (doc 07 section 2). It is the per-perf-change lab the durable append path owes: the append writer slice was the first to fsync a group, and the per-perf-change rule wants the flush-amplification claim measured, not asserted.

## Question

The `.aki` file has one log-writer goroutine that drains every shard's segment buffers and issues a single fsync for the whole group (doc 07 section 2). The alternative, a writer per shard, gives each shard its own fsync. Two questions decide whether the single writer is worth the coupling:

- How many fewer flushes does group commit cost than a writer per shard, and does that keep the device off saturation at the gate's write rate?
- How big does a group have to be before the one ring-hop-and-wakeup handoff per group amortizes below the 1 percent the doc claims, and what commit latency does that group size cost?

## Model

The lab imports no engine package. The append writer issues exactly one fsync per `AppendGroup` under `SyncAlways` (akifile `writer.go`, `maybeSync`), so the flush rate is `1/T` for a group window `T`. The device flush time and the ring-hop cost are machine parameters the model fixes as flags, and the test pins the derived arithmetic against the spec's worked table rather than timing a real disk (which would be non-deterministic and flaky in CI).

For a group of `B` records at an offered write rate `W`:

- the group window is `T = B/W`, so the writer flushes `W/B` times a second;
- each flush costs the device `F` seconds, so group commit spends `F*W/B` of the device's second, and a writer-per-shard layout spends `N` times that (N shards, N flushes per window);
- the device flush amortizes to `F/B` seconds per record;
- the one ring-hop-and-wakeup handoff per group, `H` seconds, spreads to `H/B` per record, which as a fraction of the writer's `1/W` per-record service time is `H*W/B`.

## Verdict

At the spec's worked machine (N=16, W=2M writes/s, F=50 us, H=10 us):

- **Group commit flushes 1000/s at 5.0% device time; a writer per shard flushes 16000/s at 80.0%, a 16x cut.** The rival is near device saturation exactly where group commit still has headroom, which is the whole reason for the single writer.
- **The B=2000 design group is the W*T=1ms window the doc names**, and there the ring-hop tax is 1.00% of per-record service, at the doc's line. A 512-record group is still at 3.9%, so 2000 is the smallest group that buys the sub-1% handoff.
- The device flush amortizes to 25 ns/write at the design point. A bigger group cuts the flush rate, device fraction, and hop tax further, monotonically, at the cost of proportionally more commit latency (`T = B/W`), which is the trade the window sizes.

The pre-registered falsifier from doc 07 section 2 stands: if the single writer saturates below the gate bar at the design group, the answer is two writers over two interleaved append extents in the same file, not one file per shard. This lab says the design group is comfortably off saturation, so that contingency stays unspent.

## Run

```
go run .            # full sweep, spec defaults
go run . -quick     # short sweep
go run . -shards 32 -writerate 4e6 -flush 80 -hop 20   # a different box
```
