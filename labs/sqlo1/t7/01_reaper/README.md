# Lab: sampling reaper budget and cadence

Question (doc 11 section 3.3, milestone T7 lab 01): what chunk budget and tick cadence should the slice 3 sampling reaper run at, given that a pass holds the store lock (its duration is the foreground stall bound) and lap time bounds expired-count staleness?

Arms: warm samples right after the build with index chunks resident, cold samples after checkpoint and reopen so chains page from disk, reap prices the batched tombstone ApplyBatch for the keys the probe finds dead.
The keyspace mixes classes from the write clock (near 30m, mid 24h, far 30d, rest none) with half the near keys planted already expired, plus a no-TTL control where the class skip must make passes nearly free.

Run: `./run.sh`, output reaper.csv in this directory.
The harness prints derived duty lines to stderr: mean cold pass cost against a tick period, and the lap wall time at that tick.

## Verdict (local, 2026-07-24, Apple Silicon; gate-box confirmation rides the T7 queue)

Accuracy is structural and the sweep confirms it: every lap found exactly the planted expired count (25000/25000 at 200k keys, 125000/125000 at 1M), with only wrap-boundary passes double-counting a few chunks, which is fine for an estimate.

The probe dominates and the class skip is the whole economy.
On the volatile-heavy mix a probe costs about 2.5us (near and mid entries resolve their records), so a pass costs roughly 45us per chunk at this fill; on the no-TTL control a cold chunk walk costs 1.6us and a warm one under 0.1us.
Same budget, 20x apart: budget 8 cold is 205-270us p50 on the volatile mix and 13us on the control.
Warm and cold are within about 20 percent on the volatile mix because probe cost, not chunk paging, is the bill.

The doc 11 defaults do not hold together on volatile keyspaces: 8 chunks per tick at a 10ms tick is 2.4-3.2 percent of a core there, not 1 percent.
A fixed chunk budget is the wrong knob because pass cost varies 20x with keyspace composition.
The slice 3 verdict is to time-box the pass instead: walk chunks until roughly 100us of pass time is spent, then yield.
That holds duty at 1 percent of a core at a 10ms tick by construction, sweeps non-volatile keyspaces at better than 64 chunks per tick for free, and bounds the foreground stall at the box plus one chunk overshoot (p99 under about 200us on every mix measured; fixed budget 8 shows 626-800us cold p99, which is too much to inject between commands).
Lap time at that duty on this hardware is about 90 seconds per million volatile-heavy keys, which is the DBSIZE staleness bound; acceptable for a layer doc 11 calls pure optimization, with compaction's expired credit (#1330) doing the heavy reclamation.

Reap batches are fsync-bound: an ApplyBatch of tombstones costs about 4ms flat whether it carries 8, 64, or 256 dels, so per-key cost is 500us at 8 and 16us at 256.
Slice 3 should accumulate probe hits and emit large tombstone batches (256, or ride the next drain cycle) rather than deleting as it finds.
