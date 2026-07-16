# Clock skew

## Question

Doc 02 section 5 claims folded-state safety needs no clock at all and that clock quality only moves the liveness window of section 3.4 (TTL plus skew plus one heartbeat interval, about 4.5s at the 3000/500/1000ms defaults).
Does that hold when one member's clock is actually wrong, and do the three constants survive as defaults?

## Method

Virtual time: a real-time cursor advances in 10ms steps and the holder sees clock(t) = offset + rate * t while the taker's clock is honest.
Appends run synchronously against the simulator and count as instantaneous next to the second-scale protocol windows; the clock adversary is orthogonal to the store, and fence-torture already covered store-level races on live MinIO, so this lab is sim-only and fully deterministic.

The holder H heartbeats on its own clock through a real ChainAppender and acks client writes whenever its LeaseGuard believes the lease alive; at a set point it is partitioned from the chain and becomes the section 3.4 zombie.
The taker T watches renewals arrive on its own clock and grants itself the group at epoch plus one once staleness passes TTL plus skew, having itself observed for a full TTL.
The zombie ack window is the real time between T's grant landing and H's last ack.
H also honors doc 02 section 3.3 self-demotion: every successful append catches its fold up to the tail, and a holder whose own fold shows a foreign grant drops the guard on the spot.
The first sweep missed that rule, so H kept acking on a stale guard long after its own appends had folded the takeover, which models a broken node rather than a mis-clocked one and inflated the window on the cadence-starved arm from 6.5s to 41.5s; the fix landed before scoring and only that arm moved.
After each run H reconnects and its buffered commit goes to the chain under the epoch it still believes; three folds (H's, T's, and a cold replay) must agree on StateSum and the commit's verdict.

Arms: constant offsets from -10s to +10s at honest rate, rate skew from 1.0 down through the predicted knife edge to 0.1, a frozen clock (the VM-pause pathology), and healthy no-partition arms with wrong clocks.

## Prediction (PRED-OBS1-O0B-CLOCKSKEW, filed before the measured run)

1. Constant offset is invisible at any magnitude: every offset arm produces identical acks, zombie acks, and window to the offset-0 baseline, because both sides only ever subtract their own clock from itself and no wall-clock value crosses a node boundary.
2. With holder clock rate r, the guard suspends (TTL - skew) / r real ms after the last renewal while the taker grants TTL + skew after the last arrival, so zombie acks start near r = (TTL - skew) / (TTL + skew), about 0.714 at defaults, and the window grows as (TTL - skew) / r - (TTL + skew) as r falls.
3. At honest rate the holder suspends a full second before the earliest possible takeover, so zero zombie acks at r = 1 and for any constant offset.
4. The frozen clock never suspends and acks to the end of the run: the window is unbounded, which is the spec's honestly stated worst case.
5. Zero safety violations in every arm including frozen: all three folders agree and the zombie's late commit dies at the fence everywhere.
6. A wrong clock without a partition causes no suspension and no takeover, so the constants 3000/500/1000ms survive as defaults; badly slow rates additionally starve the heartbeat cadence below the staleness bound, which costs the lease early but never safety.

## Results

| arm | offset_ms | rate | partition | acks | zombie_acks | window_ms | suspended | takeover | violations |
|---|---|---|---|---|---|---|---|---|---|
| offset | 0 | 1.000 | true | 750 | 0 | 0 | true | true | 0 |
| offset | 250 | 1.000 | true | 750 | 0 | 0 | true | true | 0 |
| offset | -2000 | 1.000 | true | 750 | 0 | 0 | true | true | 0 |
| offset | 10000 | 1.000 | true | 750 | 0 | 0 | true | true | 0 |
| offset | -10000 | 1.000 | true | 750 | 0 | 0 | true | true | 0 |
| rate | 0 | 1.000 | true | 750 | 0 | 0 | true | true | 0 |
| rate | 0 | 0.900 | true | 834 | 0 | 0 | true | true | 0 |
| rate | 0 | 0.800 | true | 813 | 0 | 0 | true | true | 0 |
| rate | 0 | 0.714 | true | 912 | 0 | 0 | true | true | 0 |
| rate | 0 | 0.600 | true | 1084 | 65 | 650 | true | true | 0 |
| rate | 0 | 0.400 | true | 1375 | 273 | 2730 | true | true | 0 |
| rate | 0 | 0.100 | true | 1000 | 648 | 6480 | false | true | 0 |
| frozen | 0 | 0.000 | true | 4000 | 3648 | 36480 | false | true | 0 |
| healthy | 0 | 1.000 | false | 2000 | 0 | 0 | false | false | 0 |
| healthy | -5000 | 0.500 | false | 2000 | 0 | 0 | false | false | 0 |
| healthy | 5000 | 2.000 | false | 2000 | 0 | 0 | false | false | 0 |

Every offset arm is byte-identical to the offset-0 baseline, out to plus and minus ten seconds, because no wall-clock value ever crosses a node boundary; both sides only subtract their own clock from itself.
At honest rate H's last renewal lands at t=5s, the guard suspends at 7.5s, and T's grant cannot land before 8.52s, so the window is closed by a full second and zombie acks are zero, and the same holds at rates 0.9, 0.8, and exactly the knife edge 0.714.
Past the knife edge the window tracks the (TTL - skew) / r - (TTL + skew) formula: 650ms measured against 666ms predicted at rate 0.6 and 2730ms against 2750ms at rate 0.4, the gap being tick quantization.
Rate 0.1 fails differently: the real heartbeat interval dilates to hb / r = 10s, which starves the 3.5s staleness bound, so T takes over at 3.51s while H is still connected and healthy.
H's next heartbeat at t=10s succeeds, folds the takeover grant through its own catch-up, and self-demotes, so the window is 6.48s, which is the section 3.4 shape with the heartbeat-interval term dilated by 1/r.
The frozen clock never fires its cadence (believed time never advances), so it neither suspends nor discovers, and the window runs to the end of the schedule: unbounded in principle, exactly the spec's stated worst case.
The healthy arms show a wrong clock alone, slow or fast, causes no suspension and no takeover.
All 16 arms: zero safety violations; H's fold, T's fold, and a cold replay agree on StateSum everywhere, and the zombie's late commit dies at the fence in every takeover arm.

## Verdict

PRED-OBS1-O0B-CLOCKSKEW scores six for six.
Offset invariance (1), the knife edge and window formula (2), the one-second guard margin at honest rate (3), the frozen-clock unbounded window (4), zero safety violations everywhere including frozen (5), and the healthy arms plus heartbeat starvation at badly slow rates (6) all came out as filed.
The constants survive as defaults: TTL 3000ms, skew bound 500ms, heartbeat 1000ms give zero zombie acks down to rate 0.714, a clock 40 percent slow costs about 2.7s of stale acks and nothing else, and even a frozen clock only widens the window while every folder still agrees and every fenced commit still dies.
That is the exit-gate claim: past the section 3.4 bound the loss is liveness only, safety is unaffected at any offset and any rate.
Two honest notes: the window formula assumes the holder keeps heartbeating, and once the dilated interval crosses the staleness bound the window is instead set by discovery latency, one real heartbeat interval; and the frozen clock has no discovery channel at all because the cadence itself is clock-driven, so if VM pauses are a real concern the heartbeat scheduler should key on appends observed rather than believed time.
Neither moves the defaults.
