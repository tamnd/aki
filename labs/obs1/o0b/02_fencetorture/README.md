# Fence torture

## Question

Do C-I2 and C-I3 hold under adversarial delivery: does every independent folder, fed the same chain through any consumption path, land on the identical lease table and the identical per-section commit verdicts, no matter what races, faults, and crashes built that chain?

## Method

Each schedule puts 2 to 8 nodes on one shared chain, every node a real ChainAppender with its own live LeaseFold, and opens with a deliberately concurrent burst where every node grants itself group 0 at epoch 1 so CAS picks exactly one winner.
The sequential steps that follow are driven by a seeded rng and lean on stale beliefs as the weapon: a node commits under whatever epoch its own fold believes, holder or not, which manufactures zombie commits and expired-lease writers naturally; grants are computed from possibly stale tables, which manufactures ambiguous grants; releases, heartbeats, and incarnation-shuffled member records fill the gaps.
The simulator injects ambiguous PUTs on the append path at 0, 15, and 40 percent, half of them landed and half not, and up to two nodes per schedule crash-restart with a fresh batch counter and a bumped incarnation, replaying the whole chain to rebuild their fold.

After each schedule three independent folders consume the chain three different ways: folder A follows through a fresh appender, folder B walks raw objects with no appender at all, folder C replays to mid-chain, summarizes into a checkpoint, primes a fresh fold from it, and replays the rest.
A and B must agree on StateSum and on a running digest of every commit verdict; every surviving node's live fold, whatever mix of Append and Follow built it, must match A; and C must land on the same lease and member tables, which is C-I7's summary-never-authority claim exercised for real.
A run with no rejected grants or no dead sections fails as toothless, so the torture cannot silently pass by not torturing.
One honesty note: the opening burst makes schedules nondeterministic across runs even at a fixed seed, which is fine because the verification does not care how the chain got built, but it means a violation seed reproduces the shape of the race, not its exact bytes.

## Results

fencetorture.csv, all arms:

| store | schedules | steps | nodes | groups | faults | grants ok/rej | sections live/dead | crashes | violations |
|-------|-----------|-------|-------|--------|--------|---------------|--------------------|---------|------------|
| sim | 40 | 150 | 4 | 8 | 0% | 1156/233 | 214/299 | 80 | 0 |
| sim | 40 | 150 | 4 | 8 | 15% | 1120/215 | 211/313 | 80 | 0 |
| sim | 40 | 150 | 4 | 8 | 40% | 1138/225 | 225/297 | 79 | 0 |
| sim | 20 | 300 | 8 | 16 | 15% | 1146/263 | 109/318 | 40 | 0 |
| sim | 20 | 300 | 2 | 2 | 40% | 1095/150 | 450/248 | 40 | 0 |
| minio | 5 | 150 | 4 | 8 | real races | 145/24 | 32/46 | 10 | 0 |
| minio | 3 | 200 | 8 | 16 | real races | 115/35 | 7/41 | 6 | 0 |

168 schedules, roughly 19,900 appends, 5,900 folded grants against 1,145 rejected, 1,560 dead commit sections, 650 stale member records, 335 crash-restarts.
Zero violations everywhere, including the live MinIO arms where the CAS races are real HTTP.
The torture has teeth in every row: at 8 nodes and 16 groups nearly three quarters of committed sections die at the fence, exactly what a fleet of nodes acting on stale beliefs should produce.

## Fleet-scale re-run (O3a exit gate, filed before the run)

The O3a exit gate re-runs this lab at fleet scale, 16 nodes and 32 groups, the K2 checkpoint's fleet size, with the lab code unchanged.
Arms: sim at 0, 15, and 40 percent ambiguous PUTs, 20 schedules of 300 steps each, and a live MinIO arm with real HTTP CAS races.
Expectation, filed before the run: zero violations on every arm and teeth in every row, the same pass condition the original arms held; the fence argument is scale-free by construction, since every folder consumes the same chain, so a violation at 16 nodes that 8 nodes never showed would mean a fold or appender bug the fleet slices introduced, and the gate row stays open until it is named and fixed.

### Results

fencetorture-fleet.csv, all arms:

| store | schedules | steps | nodes | groups | faults | grants ok/rej | sections live/dead | crashes | violations |
|-------|-----------|-------|-------|--------|--------|---------------|--------------------|---------|------------|
| sim | 20 | 300 | 16 | 32 | 0% | 1093/414 | 30/370 | 40 | 0 |
| sim | 20 | 300 | 16 | 32 | 15% | 1098/412 | 31/384 | 40 | 0 |
| sim | 20 | 300 | 16 | 32 | 40% | 1107/424 | 24/368 | 40 | 0 |
| minio | 3 | 200 | 16 | 32 | real races | 119/62 | 3/45 | 6 | 0 |

Zero violations on every arm, and the teeth sharpen with scale exactly as the argument predicts: at 16 nodes and 32 groups over 92 percent of committed sections die at the fence, against roughly three quarters at 8 nodes, because more nodes acting on stale beliefs means more zombies for the fold to kill identically everywhere.
The expectation held as filed and the lab code is unchanged from the O0b run.

## Verdict

C-I2 and C-I3 hold under everything this lab can throw: independent folders agree bit for bit across three consumption paths, live folds built by mixed Append and Follow match cold replays, checkpoint primes land on the same tables, and every zombie commit dies identically everywhere.
No constant gets baked here; TTL, skew, and heartbeat interval stay declared knobs for the clock-skew lab, since nothing in this fence needed a clock, which is the point of the doc 02 section 3.3 design.
This lab is the O0b exit gate's fence-torture half and gets promoted to gate D4 later.
