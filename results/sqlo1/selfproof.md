# Harness self-proof: core suite against the placeholder store

Milestone S0 (tamnd/aki#710), slice 6; spec 2064/sqlo1 doc 13.
Harness: tamnd/aki-bench at 6b5010c (bench PR tamnd/aki-bench#44), aki checkout 03da179 with the placeholder map store in the sqlo1 slot.
Box: the gate box (GamingPC, WSL2, 32 cpus), 2026-07-15; raw merged CSV and manifest beside this note under results/sqlo1/selfproof/.
Matrix: cap and data arms x mixes 90/50/10 x values 16/128/512/4096 B x uniform/zipfian x scales 1x/4x/16x x 2 reps = 288 reps, 864 server rows (aki, redis, valkey per rep).
The run split across five sessions after WSL teardowns (148 reps, then resumes of 2, 6, 36, and 96); the resume runner skips finished cells by their per-rep JSON, and the merged set holds 288 unique rep files with zero duplicates, zero failures, and no missing cells.

## What this run proves

The store behind the server is the S0 placeholder map, so the aki numbers are meaningless as performance; the run is evidence the plumbing measures, which is the slice 6 gate.

- Coverage capture works and matters: on the data arm all three servers hold 100 percent coverage in all 288 reps, while on the cap arm Redis and Valkey evict down to 4.0 to 4.2 percent at the largest cells and the harness records exactly that. This is the doc 13 fair-protocol column doing its job; a raw-ops table would have hidden the eviction.
- VmHWM lands non-zero on every one of the 864 rows, disk bytes and used_memory columns populate, and per-rep JSON carries the full detail.
- Alternating run order, per-rep timeouts (240 s times scale), and the failure ledger all exercised; failures.txt is empty in all five sessions.
- The placeholder lands at 0.26 to 1.07x Redis ops per second with a median of 0.51x, and every gate line prints FAIL against the 2.0x bar. Meaningless as a verdict, but the order of magnitude is sane and the harness shows no home-team inflation.

## One finding

Every rep logs "version probe aki: INFO server returned load.replyError, want a bulk string" because the placeholder server does not implement INFO, so the aki version column is blank in all 288 rows.
Non-fatal and cosmetic; measurements are untouched.
The column fills itself once a real store slice gives the server an INFO section, so no separate fix is queued.

## Verdict

Slice 6 passes: the harness runs the full doc 13 matrix end to end, survives session teardowns via resume, and captures coverage, memory, disk, and latency columns correctly.
Next S0 step is the exit-gate baseline table with AKISLOT=f3 against the same pinned rivals.
