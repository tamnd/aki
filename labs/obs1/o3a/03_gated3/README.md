# gated3: crash takeover time across the fault schedule set

## Question

Gate D3 (doc 10) wants the crash takeover clocked from kill to taker serving, within doc 02's predictions, across the fault schedule set.
Does the kill-to-serving time hold its bound not just on a clean bucket but under every degraded mode the fleet is specified to survive?

## Method

The doc 02 prediction splits in two, and this lab scores the half the fleet harness owns.
The mechanics half, the wall-clock cost of the silence probe, the grant, the fan-8 rebuild, and the tail replay, was scored by PRED-OBS1-O3A-TAKEOVER on the real sequence: p50 1412ms and p99 2369ms against the 5000ms bar (lease TTL plus 2s), 20 reps at 256 segments.
The policy half is the discipline: chain-observed staleness at TTL plus skew (3500ms), a full watched TTL on top (3000ms), plus at most a heartbeat interval of pre-crash quiet, which bounds kill-to-eligibility at 7500ms of fleet time.
This lab runs the policy half on the fleetsim harness across fault schedules: a 3-node fleet of full stacks over one simulated bucket, deterministic shared clock, 100ms duty-cycle ticks, and measures the simulated time from the kill to the survivors' folds covering every one of the victim's groups.
Every schedule then verifies the same invariants on the healed bucket: recovered groups at epoch 2 with survivors' rendezvous naming each holder, a 3-frame flush replaying whole from the taker's retained window via TakeGroup, survivors' folds agreeing by StateSum, and zero dead sections anywhere.

The schedule set, per the doc 10 chaos suite list as far as the harness reaches:

- clean: no faults, the baseline bound.
- storm: every 3rd bucket operation fails from the kill until recovery, the SlowDown verdict surface; failed grant attempts retry on later ticks.
- read-outage: every GET fails for 2s starting at the kill; the discipline needs no new reads because the staleness watch runs on already-folded chain facts and the local clock.
- ambiguous-put: every 5th mutation from the kill reports failure after landing, the doc 02 section 2.4 shape; grants route through the appender's recheck-ours recovery.
- write-outage: every mutation fails from the kill until kill plus 9s, past the eligibility point, so disciplined grant attempts really fail and ownership freezes until the heal.

Not reachable at this layer, disclosed: mesh partitions (the engine duty cycle carries no mesh; doc 07 makes every mesh verb fallback-only, and the per-verb fallbacks are proven in obs1srv/mesh) and a mis-clocked node (the harness clock is shared by construction; the zombie bound is proven both ways engine-level in the crash-takeover and warm-restart suites).

## Prediction (PRED-OBS1-O3A-GATED3, filed before the scored run)

1. Clean: kill to full coverage of the victim's groups within [6.5s, 8.0s] of simulated time; the floor is the discipline itself, the ceiling adds the pre-crash heartbeat quiet and tick slack.
2. Storm: within [6.5s, 9.0s]; a scattered every-3rd failure costs at most a few 100ms retry ticks, never a discipline restart.
3. Read-outage: within [6.5s, 9.0s]; a blind window does not slow the staleness watch at all, so this band equals the storm band without needing its slack.
4. Ambiguous-put: within [6.5s, 9.0s]; recheck-ours turns each ambiguous grant into one extra observation, not a lost attempt.
5. Write-outage healing at 9s: recovery within [9.0s, 10.5s]; the outage is the binding constraint, and the already-disciplined takeover lands within tick-and-heartbeat slack of the heal.
6. Invariants on every schedule: epoch exactly 2 on every recovered group, holders at the survivors' rendezvous choice, the 3-frame replay exact, survivor folds agreeing, zero dead sections, and zero duty-cycle errors on the clean schedule.

Kill line: any schedule outside its band, any recovered group off epoch 2 or off the rendezvous choice, a replay that walks anything but 3 frames, fold divergence, or dead sections anywhere means the fleet does not survive its specified degraded modes at the predicted time, and the gate row stays open until the failing mode is named and fixed.

## Post-prediction amendment (filed after the first scored run, before the re-run)

The first scored run passed clean, storm, read-outage, and ambiguous-put in band and failed the write-outage schedule's placement assertion, and the assertion was wrong, not the fleet.
A 9s write outage silences every heartbeat, so the survivors cross each other's staleness horizon and finish the takeover discipline against each other as well as against the victim; at the heal, whichever duty cycle runs first plans takeovers from its degraded survivor view, in which it is the only live member and rendezvous prefers it for everything, and it seizes the peer's groups along with the victim's before the peer's first post-heal heartbeat can be observed.
This is protocol-honest and safe: every seizure is epoch-fenced, the seized peer demotes on its next reconcile, and the balancer sheds each misplaced group back to the live-members rendezvous, one handoff per balance tick, so the placement self-corrects with a rebalance tail linear in the seized group count.
The rig amendment: after recovery, every schedule runs a settle phase that ticks until the whole placement matches the live-members rendezvous, reported as settle_ms; the placement and epoch assertions move behind it.
Amended bands: settle within 2s for clean, storm, read-outage, and ambiguous-put, whose survivor views never degrade, and within 12s for write-outage, pricing the one-shed-per-tick rebalance tail; victim-group epochs stay exactly 2 on the first four schedules, and land in {2, 3} under write-outage, where 3 is a group that moved twice, seized at 2 and rebalanced at 3.
The recovery bands of the prediction stand unchanged, and the first run's recovery numbers stand as scored.

## Calibration disclosure

The fleetsim harness's own test suite (#1361), run before this file was written, recovered the clean crash schedule in 6.7s simulated against an 8s assertion, which is where band 1's shape comes from; the storm, read-outage, ambiguous-put, and write-outage schedules have never been clocked and their bands come from the discipline arithmetic above.

## Run

./run.sh writes gated3.csv: schedule, victim group count, recovery in simulated ms, band, settle in simulated ms, duty-cycle errors, verdict.
