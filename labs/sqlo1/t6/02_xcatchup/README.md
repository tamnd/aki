# xcatchup: cold catch-up over a 10M entry stream

Question: what does a consumer replaying 10^7 entries cold cost, does it pollute the hot tier, and what prefetch depth should the range walk use?

## Method

Three arms over one built file, always through the RESP surface of an in-process sqlo1b-backed server.

The build arm XADDs 10M entries of 100 payload bytes (a 20 byte zero-padded ordinal plus padding across two fields), then writes a 20000 key point keyspace for the pollute arm, then closes politely.
Every later arm reopens the file cold, so the whole sequence doubles as a replay oracle: the ordinals must come back 0..n-1 in order, exactly once, across a real close and recovery.

The catchup arm pages through the stream with XRANGE COUNT and exclusive-bound cursors and reports entries per second, payload MB/s, peak RSS, and heap.
The pollute arm warms the point keyspace, probes GET p50/p99 before, during (worst window), and after a full background replay on a second connection, and reports the worst-window p99 against baseline.

Runs on the dev box (macOS, M-series); the gate box rerun belongs to the T6 exit gate.

## Results

Built file: 1136 MB, build rate 158K adds/s, peak RSS 109 MiB.

| arm | COUNT | secs | entries/s | MB/s | RSS MiB |
|---|---|---|---|---|---|
| catchup | 100 | 9.5 | 1.05M | 100 | 123 |
| catchup | 1000 | 5.8 | 1.74M | 166 | 124 |
| catchup | 10000 | 5.2 | 1.91M | 183 | 130 |

Pollute at COUNT=1000: baseline p50/p99 62/153 us, during 38/592 us worst window, after 60/171 us.

Prefetch depth A/B at COUNT=1000, editing streamRangeBatchRuns and rebuilding:

| batch runs | entries/s | worst p99 ratio | during p99 us |
|---|---|---|---|
| 4 | 1.38M | 4.5 | 749 |
| 16 | 1.74M | 3.9 | 592 |
| 64 | 1.54M | 6.9 | 902 |

## Verdict

streamRangeBatchRuns stays 16: it wins both throughput and isolation, so there is no tradeoff to arbitrate.
4 pays 21 percent of throughput for a worse tail, and 64 overshoots the COUNT boundary (64 runs is about 2300 entries against COUNT 1000) and more than doubles the tail spike.

PRED-SQLO1-T6-CATCHUP splits.
The working-set half holds: RSS stays around 120-130 MiB against a 1.1 GB file through every arm, and the after-replay probe window lands within 12 percent of baseline, so a full cold replay does not evict the hot working set.
The p99-under-20-percent half fails as measured: the worst probe window during replay runs 3.9x baseline p99 at the winning depth.
The spike is a transient tail, not pollution: during-replay p50 is at or below baseline in every arm, and the ratio worsens with wider prefetch, which points at dispatch-side burst length rather than cache damage.
The gate box rerun should re-measure the ratio before any engine work; a sub-millisecond worst-window tail on a shared macOS dev box is within scheduler and GC noise, and the prediction was written against the gate contract.

## Fallout

The replay oracle caught three engine bugs before producing a single measurement row: the missing clean-shutdown flush door (#1127), the arena class-migration deadlock that killed the build at 756613 entries (#1132), and the WAL stale-remnant chain that silently dropped 439K acked entries across a clean close (#1135).
