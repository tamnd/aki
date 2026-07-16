# strictlatency: what a strict ack really waits on

Milestone O1b lab 03 (spec 2064/obs1 doc 04 sections 3.2, 5, and 12, doc 11 section 4).

## Question

What latency distribution does a strict ack see under light, medium, and heavy strict load on the Standard and Express placement models, and which of doc 04 section 3.2's two statements wins: the p50 formula flush-age/2 plus PUT plus append (90 to 140ms Standard, 15 to 30ms Express), or the barrier floor sentence, under which a pending strict ack pulls the next flush to at most 5ms after the last one and the flush-age/2 term never applies?
The winner's numbers seed the doc 04 section 5 table.

## Prediction (PRED-OBS1-O1B-STRICT, filed before the scored run)

The milestone rung is the doc band: strict ack 90 to 140ms on the Standard model, 15 to 30ms on Express.
The mechanism reading filed here: the two section 3.2 statements conflict, and the barrier floor should win, because a pending strict ack is barrier demand by definition and the floor caps the wait-for-swap at 5ms, not flush-age/2 = 25ms.
So light-load Standard p50 should land at 75 to 85ms (floor wait plus PUT p50 35ms plus append p50 35ms), UNDER the doc band's bottom, and Express at 13 to 18ms, at the band's bottom edge; p99 around 350 to 450ms Standard (two lognormal tails plus head-of-line) and 30 to 50ms Express.
Under heavy strict load the distribution is not the formula's at all: strict throughput degrades to the flush cadence per section 3.2, acks share flushes by the hundreds, and Standard p50 moves to pipeline queueing territory around 180 to 220ms while Express stays floor-bound near 20ms.
If the measurement confirms the floor, the section 3.2 formula prose gets corrected when the strict slice bakes these constants, and the section 5 table rows inherit the measured numbers.

Disclosure: the model is the flush-cadence lab's, extended with the barrier trigger, and a -quick smoke ran while wiring that trigger before this prediction was frozen; the smoke pointed the same direction as the mechanism reading above.
The Express PUT distribution is a lab assumption (p50 6ms, p99 25ms from the doc 01 single-digit-ms envelope, no published tail) and the O5 E-cloud refit replaces it; Express rows are priced from the doc 01 section 2.3 sheet the lab carries.

## Method

The flush-cadence lab's virtual-time flusher model (swap only on a free pipeline slot, 4-deep PUTs, seq-ordered batched chain commits, block-not-drop cap) with every write strict.
Barrier demand per section 3.2: while any strict write is pending, the next swap fires at the 5ms floor after the last one, ahead of the 50ms age trigger.
The ack is arrival to covering chain commit, the exact watermark a strict reply parks on.
Defaults from the cadence lab: flush-age 50ms, flush-size 8 MiB.
Loads: light 10 ops/s at 100 B, medium 1000 ops/s at 512 B, heavy 20000 ops/s at 512 B.
Each cell runs 10s virtual warmup plus 120s measured; zero-latency unit tests pin the floor arithmetic and the acks-share-flushes claim.

## Run

    ./run.sh            # full sweep into strictlatency.csv
    go run . -quick     # tiny windows, smoke only

## Results

Full sweep in strictlatency.csv, run 2026-07-17, 10s warm plus 120s measured virtual time per cell.

| placement | load | flushes/s | acks/flush | ack p50/p90/p99 ms | request $/month |
|-----------|------|-----------|------------|--------------------|-----------------|
| standard | light | 10.0 | 1.0 | 99 / 236 / 476 | 247 |
| standard | medium | 78.7 | 12.7 | 185 / 363 / 799 | 1190 |
| standard | heavy | 77.7 | 257.4 | 178 / 342 / 614 | 1188 |
| express | light | 10.0 | 1.0 | 13 / 23 / 38 | 59 |
| express | medium | 200.0 | 5.0 | 20 / 32 / 49 | 922 |
| express | heavy | 200.0 | 100.0 | 20 / 32 / 48 | 921 |

## Verdict

PRED-OBS1-O1B-STRICT: the milestone band scores HIT on Standard (p50 99ms inside 90 to 140) and misses Express by 2ms on the fast side (13ms against the band's 15 bottom); the mechanism refinement filed above half-hit, and the miss is the interesting part.
The barrier floor wins as mechanism, exactly as called: light-load waits for the swap are near zero, heavy Express strict runs floor-bound at 200.0 flushes/s, and the zero-latency unit test pins the arithmetic; the flush-age/2 term never shows up.
But the filed 75 to 85ms Standard band was sum-of-medians arithmetic, and the median of a SUM of two right-skewed PUT-class draws sits well above the sum of their medians: measured 99ms against 35 plus 35 plus floor.
So doc 04 section 3.2's formula got the right number range on Standard through the wrong mechanism, and the correction when the strict slice bakes constants is to keep the 90 to 140 Standard row, restate it as floor plus PUT plus append with the skew premium named, and move Express to 13 to 20ms p50.
Degradation-to-cadence is proven, not just claimed: heavy strict load rides 257 acks per flush on Standard and 100 on Express with latency roughly flat against medium load, so strict throughput follows the flusher, never per-op PUTs.
Head-of-line blocking shows again under pipeline saturation (standard medium p99 799ms), the same seq-ordered-commit property the cadence lab flagged for the commit-records slice.
One placement economics note for doc 09: at floor cadence Express serves 310 req/s for $921 a month while Standard serves 90 req/s for $1188, so barrier-heavy strict workloads are cheaper AND 8x faster on Express; it stays a latency product for storage, but for strict request traffic it is also the cost winner.
