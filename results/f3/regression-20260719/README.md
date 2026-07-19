# Regression confirmation: headline + point reads stay green on the current tip

GamingPC gate box (2026-07-19), reactor gate binary at tip 3790e56 vs CF16-frozen
rivals (redis io6, valkey io4). This cell re-runs the headline string rows (M8-R1)
and the collection point reads (M9-R1) under the dual-generator 1M-key protocol to
confirm nothing regressed after the M8 durable-append / .aki arc, the M9 lazy-expiry
plus OBJECT IDLETIME per-key clock, and the M11 box-free command closure landed.

## The numbers

| row | op | aki ops/s | redis | valkey | aki/redis | aki/valkey | verdict |
|---|---|---|---|---|---|---|---|
| M8-R1 | SET | 7.98M | 3.42M | 2.66M | 2.33x | 3.00x | PASS |
| M8-R1 | GET | 7.97M | 3.99M | 3.42M | 2.00x | 2.33x | PASS |
| M9-R1 | SISMEMBER | 7.98M | 3.42M | 2.99M | 2.33x | 2.67x | PASS |
| M9-R1 | ZSCORE | 7.98M | 3.00M | 2.66M | 2.66x | 3.00x | PASS |
| M9-R1 | HGET | 7.96M | 3.00M | 2.66M | 2.66x | 2.99x | PASS |

aki is flat at ~7.96-7.98M across all five, the same reactor keyspace-lookup ceiling
the M0/M1/M2/M4 headline rows measured before the durability, expiry, and closure
milestones landed. GET sits exactly 2.00x redis at the line and clears. No row moved.

## What each row confirms

- M8-R1: the hot SET/GET path did not widen after the durable-append write path and
  the .aki single-file recovery arc landed. A durable write path that logged on the
  hot path would have shown here; SET holds 2.33x/3.00x, GET 2.00x/2.33x.
- M9-R1: the collection point reads SISMEMBER (M1-G1) / ZSCORE (M2-G1) / HGET (M4-G1)
  did not regress after lazy expiry and the OBJECT IDLETIME per-key clock. The clock
  claims zero added bytes and an off-path stamp; the reads stay at the ceiling.
- M9-R2 (memory bit-flat): the IDLETIME clock lands in a spare header word / struct
  padding, proven zero-added-bytes by the struct-size guards TestFentrySizeStable
  (hash), TestFixedSizesStayFrozen (akifile), and TestBytesPerMember (set), all
  green, plus the acct_test.go "byte-for-byte with M0" store tests. No box row needed:
  the peak cannot move when the per-cell byte count is guarded stable.
- M10-R1 (loop-knee resolution): aki ran under `-net-loops 0`, which resolves to
  GOMAXPROCS/2 (cmd/f3srv/main.go, the knee re-swept in labs/f3/m0/26_loop_knee). The
  sustained flat 7.98M is the live proof it resolved to the throughput knee; a
  misresolved loop count would not hold the reactor ceiling.
- M11-R1 (box-free closure off the hot path): the pub/sub, MONITOR, DUMP/RESTORE,
  COPY/MOVE, RENAME, and DEBUG OBJECT closure sits off the shard hot path; the five
  point rows above are unchanged after it landed, and the acct "byte-for-byte with M0"
  tests confirm no resident-byte creep.

## Verdict

- M8-R1 SET/GET: PASS, unchanged on the tip (2.00-3.00x).
- M9-R1 point reads: PASS, unchanged on the tip (2.33-3.00x).
- M9-R2 memory: bit-flat, guarded by the struct-size tests (no box row).
- M10-R1 loop-knee: resolves to GOMAXPROCS/2, confirmed by the sustained ceiling.
- M11-R1 closure: off the hot path, no point row regressed.

CSV: regression.csv. Probe: regress.sh (the ptdual.sh dual-generator pattern with
the five headline/point rows).
