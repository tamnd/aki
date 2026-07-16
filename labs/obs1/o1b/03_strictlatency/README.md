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

Pending the scored run; this section lands in the results commit.
