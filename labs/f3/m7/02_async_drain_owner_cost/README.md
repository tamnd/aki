# Lab 02: async cold drain, owner critical-path time saved by moving the pwrite off-owner

Part of issue #549, the M7 LTM milestone, lab 02, the two-phase whole-record migrator (doc 06 sections 3.1 and 3.4). This is the lab the async-drain slice depends on: it settles that handing the cold-region pwrite to the shard's off-owner I/O worker takes the write off the owner's critical path, so a drain-bound shard keeps serving commands instead of parking in the syscall, per the labs-per-perf-change rule.

## Question

The whole-record migrator has two forms. The synchronous form (migrate.go) frames a run of cold-bound records and pwrites them to the cold region inside the owner goroutine, then flips their index slots. Correct, but the owner sits in the pwrite: while a shard is blocked in that syscall it serves no commands. The two-phase form (coldstage.go) frames the run and hands the buffer to the shard's one off-owner I/O worker, which pwrites it while the owner goes back to serving; a completion event later runs the slot flips on the owner in program order. Same bytes moved, same records demoted; the difference is where the pwrite's wall-clock lands.

The saving per drain is the pwrite the owner no longer sits in, net of the channel hand-off the async form adds. The hand-off is one fixed cost per drain; the pwrite scales with the drain. So the questions: where is the crossover, how much of the owner's per-drain time does the async form reclaim across the warm-cache floor and a blocking write, and how much serving throughput does a drain-bound shard get back.

## Method

In-process, no server, no wire, no engine import, the lab-local model the other f3 labs use. The record and cold-frame geometry (12-byte cold frame header, a 16-byte key, an 8-byte int cell, a 36-byte frame) match the store, so the framed bytes are the store's bytes, not a stand-in. The writes go to a real temp file, both a warm buffered `WriteAt` (the cheap page-cache floor) and a `WriteAt` plus fsync (a real blocking cost, the kind of stall M8's durable posture adds). The model charges the owner framing plus the write plus the flip in the sync form, and framing plus the hand-off plus the flip in the async form; the reclaimed share is the write net of the hand-off, and the throughput uplift is the ratio of owner times under a duty cycle of commands served per drain.

Every rounding is against aki's win. The hand-off is charged a full channel round trip, more than the single send the real submit does, so the async cost is overstated. The flip is charged to both forms, so it cannot flatter the async side. The warm write is the best of several, so the floor is a real floor.

`go run .` runs the whole sweep. `-quick` is accepted for the shared runner. `TestAsyncCutsOwnerTime`, `TestReclaimedIsPwriteNetOfHandoff`, `TestBiggerDrainReclaimsMore`, and `TestUpliftRisesWithDrainIntensity` are what CI drives.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-15, one process. Per-record framing 10.3 ns, warm write 2.98 ns/rec, blocking write (fsync) 3.98 ms/call, hand-off 16.5 ns, flip 2.2 ns/rec. Record frame 36 B; async wins once the write exceeds the 16.5 ns hand-off.

Sweep A, a 1024-record drain, rising write latency from the warm floor through blocking-disk figures:

| write | ownerSync | ownerAsync | reclaimed | uplift@1k |
|---|---|---|---|---|
| warm floor | 15.90us | 12.86us | 19.1% | 1.03 |
| 1us | 13.85us | 12.86us | 7.1% | 1.01 |
| 10us | 22.85us | 12.86us | 43.7% | 1.11 |
| fsync (measured) | 4.00ms | 12.86us | 99.7% | 43.90 |
| 100us | 112.85us | 12.86us | 88.6% | 2.08 |
| 1ms | 1.01ms | 12.86us | 98.7% | 11.77 |

The hand-off is one fixed cost per drain (16.5 ns) while the write scales with the 1024-record drain, so even the warm floor (a few nanoseconds per record, 3 us across the drain) outweighs the single hand-off and reclaims 19%. A write that blocks moves the whole stall off the owner: a real fsync at 4 ms reclaims 99.7% of the owner's per-drain time, and a 100 us disk write reclaims 88.6%. The crossover sits just above the hand-off, so any drain past a handful of records wins.

Sweep B, the measured blocking write and a 1024-record drain, rising commands served per drain:

| cmds/drain | async/sync |
|---|---|
| 100 | 191.95 |
| 1000 | 43.90 |
| 10000 | 5.90 |
| 100000 | 1.50 |

The heavier the drain intensity, the fewer commands served between drains, the more of the owner's time was the write, so the async serving-throughput uplift climbs toward the per-drain owner-time ratio. A shard draining hard while serving 1000 commands per drain serves 44x the commands with the write off-owner; even at 100000 commands per drain it serves 1.5x.

## Verdict

Moving the cold-region pwrite off the owner takes the write off the critical path. The hand-off is a fixed per-drain cost the write always outweighs across a real drain, so async wins for any drain past a handful of records: 19% of the owner's per-drain time reclaimed at the warm-cache floor, 99.7% at a real blocking fsync, and a drain-bound shard serving 44x the commands under load. The two-phase form is the write the owner must not sit inside, so the slice lands the async drain.
