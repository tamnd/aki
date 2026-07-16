# salgebra: probe batching and the probe-vs-merge switch

Milestone T3 lab 01 (spec 2064/sqlo1 doc 08 sections 4 and 6).

## Question

Three inputs for T3 slice 5.
First, the milestone's headline cell: how many effective probes does one IO round carry when SINTER gathers a driver segment's members, groups them by target segment, and fetches the group as one LookupBatch, against the naive one-round-per-member floor.
Second, the strategy switch: probing reads only the touched target segments but fragments its rounds, while a straight fh-order merge walk of both fences reads everything exactly once in packed 16-segment rounds; where does the walk win, in units both sides can check in O(1) from the roots?
Third, SUNION's dedupe: doc 08 dedupes by member digest, and a digest collision silently drops a distinct member from the union, so the width is a correctness constant, not a tuning one.

## Method

No store underneath, the spop lab's model reused: driver and target sets built through the real insert-split path (fh median splits at 503 entries), 25 percent churn to spread occupancy, short members.
Both arms price in cold segment reads (the IOPS bill), IO rounds of 16 segments (the latency bill), and payload bytes; a segment read once stays hot for the rest of the op under both arms.
Temperature applies per first touch (hot hits cost nothing and take no round slot), swept at cold and 90 percent hot.
Dedupe memory is a live heap measurement (build, hold, release, diff) and collision odds are the birthday bound.
Tests pin the build invariants, the exact read/round accounting of both arms at cold, touched-share saturation, window monotonicity, and the collision math.

## Run

    ./run.sh            # targets {2e5, 1e6}, ratio + window + dedupe sweep
    go run . -quick     # smoke
    go test ./...       # invariants, accounting, saturation, collision math

## Results (local, 2026-07-16, deterministic and box-independent)

Probe batching pays exactly as doc 08 expects: cold effective probes per IO round run 14 at tiny drivers up to 370 once the touched set saturates, against the naive floor of 1; at 90 percent hot the same rows read 100 to 2090.

The switch, cold rows against a 10^6-member target (2903 segments):

| driver members | driver/fence | touched share | probe bytes | merge bytes | probe rounds | merge rounds |
|---|---|---|---|---|---|---|
| 1600 | 0.55 | 42% | 4.8 MB | 11.1 MB | 79 | 183 |
| 6400 | 2.2 | 88% | 9.9 MB | 11.1 MB | 167 | 183 |
| 25600 | 8.8 | 100% | 11.3 MB | 11.3 MB | 216 | 187 |
| 102400 | 35 | 100% | 12.2 MB | 12.2 MB | 278 | 199 |

The touched share is 1 - exp(-driver/fence), so by 4 members per fence entry the probe arm already reads 98 percent of the target and keeps paying fragmented rounds for it; past the crossover merge wins rounds outright at equal bytes, and below it probe wins both.
The two arms sit within 6 percent of each other across the whole 3..9 window at both target sizes and both temperatures, so the constant is insensitive; the 2 x 10^5 target (530 segments) crosses in the same units.

The gather window is flat: widening from one driver segment to eight merges a few partial rounds (43 to 35 at the small target, 168 to 161 at the large) and buys nothing else, so the natural per-driver-segment gather stands and its memory bound is one segment's members.

Dedupe, measured at 10^6 uniques: 37.8 bytes per unique for 8-byte digests, 55.8 for 16-byte.
Collision odds at 10^9 uniques: 2.7 percent for 64-bit, 1.5e-21 for 128-bit.
64-bit is corruption-grade at the sizes this engine advertises; the 48 percent memory premium is the price of a correct union.

## Verdict

Slice 5 bakes three decisions.
SINTER (and SDIFF's probes into the rest sets) switches per probed set: batched probe while the driving count is under 4 x the probed set's fence length, fh-order merge walk at or above it, both sides O(1) from the roots (root count against fence length, pages x 250 on a paged root, the spop lab's proxy).
Any factor in 3..9 is defensible; 4 is where the touched share passes 98 percent, so probing past it rereads the whole target with worse round packing.
The probe gather stays one driver segment per window.
SUNION dedupes on 128-bit member digests in the spill-bounded structure, never 64-bit; the in-RAM bound stays a budget knob for the slice, at 55.8 bytes per unique member.
SINTERCARD needs nothing extra: the early exit truncates the driver walk at LIMIT hits and inherits whichever arm the switch picked.
