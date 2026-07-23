# B5 predictions, filed before the delta run

Milestone B5 (tamnd/aki#725); spec 2064/sqlo1 doc 13 discipline: the numbers go on record before the suite runs.
The software half of the milestone closed with #1325: the ring backend (#1311, #1316, #1318), the store wiring through the Backend seam (#1320), the self-test with silent iopool fallback and the INFO backend record (#1322), and the fsync-out-of-ring assertions.
What waits on the gate box is the ringpool constant sweep, the on/off delta run over the core suite, and the exit-gate rows; this note files the two milestone predictions first.

## The arms and the switch

The delta run is sqlo1 against itself: the same binary, the same core suite, iopool versus ioring, on the 1x and 16x dataset arms from doc 13.
The switch is `sqlo1srv -io-backend auto|iopool` over the `sqlo1b.ForceIOPool` knob, and every run's provenance is the `io_backend` line INFO reports, recorded per run because the fallback is silent by design.
iopool stays the correctness baseline everywhere; the ring has to buy its keep on the cold path or it stays off.

## PRED-SQLO1-B5-COLD

Cold-read p99 improves at least 20 percent under the 16x arm with ioring on.
Reasoning on record: the 16x arm serves most point reads from extents, and the iopool burns one blocking pread and usually one goroutine wake per group read across 4 workers, while the ring batches submissions (target 16 SQEs per enter, adaptive to 8 under CQ pressure, #1318) and reaps completions off the submit path, so the syscall count per cold read drops by roughly the realized batch size and the tail stops paying queue-wait behind 4 worker slots.
The io_uring study behind doc 04 put the shape on record, and the ringpool harness measures exactly this split: batched random group reads cold, per-op latency stamped at batch submit so self-queueing shows in the p99.
The one shakeout we have is explicitly disqualified: server3 at load average 137 on 8 cores showed ring depth 8 as the only competitive depth at 55k ops per second and a batch-128 p99 blowup on both backends, which reads as a saturated box, not a backend verdict.
The sweep on quiet hardware bakes depth, batch threshold, worker count, and the registered-buffer and O_DIRECT axes before the delta run, and the prediction stands against whatever constants the sweep picks.
A miss would show as the p99 delta inside 20 percent with EnterStats reporting a realized batch near 1, meaning the workload never gave the ring a batch to amortize, or as O_DIRECT losing to the page cache on this box's NVMe, both readable directly from the sweep CSV.

## PRED-SQLO1-B5-NEUTRAL

The 1x arm moves less than 5 percent either way.
Reasoning on record: at 1x the working set sits in the hot tier and the frame cache, cold reads are rare, and the ack path never touches the IO backend (R-I2: WAL buffer write is memcpy, fsync is the window's business).
The backend choice only changes who serves a cold group read, so a workload that almost never takes one cannot move, and the barriers are structurally identical on both backends: the bridge exposes no sync method, barriers stay on plain f.Sync and the WAL's own fdatasync (#1325), so durability cost is byte-for-byte the same code path on both arms.
A move above 5 percent in either direction would mean the backend is leaking into the hot path, most plausibly the bridge's per-read channel round trip showing up in the few cold reads the 1x arm does take, or the ring's submitter goroutine costing scheduler time it should not; either is a bug to chase, not a delta to report.

## What the exit gate needs beyond the predictions

- The delta note with both backends, both arms, and the io_backend provenance line per run.
- The fallback test green on a kernel without ring support, which the CI container arm covers through the skip-not-fail contract from #1311.
- The crash suite green with ioring live; the crash harness FaultFile keeps its own runs on deterministic pool IO by construction (#1322), so this row runs the timed-kill matrix against a real file with the ring selected.
- The results note with provenance.

## Bookkeeping

Filed before any B5 sweep or delta rep has run on the gate box.
The constants riding this prediction are all marked provisional in code: storeRingDepth 64, storeIOWorkers 4, batch target 16 adaptive to 8, registered buffers off in the store path, self-test timeout 2 seconds.
The sweep bakes them; the delta run judges the baked configuration; the predictions get their verdicts in the results note.
