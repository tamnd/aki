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

(to be filled by run.sh)

## Verdict

(pending the run)
