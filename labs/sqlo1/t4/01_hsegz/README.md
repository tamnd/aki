# hsegz: the zset run and member segment sizes

Milestone T4 lab 01 (spec 2064/sqlo1 doc 09 sections 2 and 8).

## Question

T4 slices 2 and 3 bake two split thresholds, and the zset couples them in one command: a score-moving ZADD bills one member-segment post-image plus one or two run post-images in its frame group, so the doc 06 W4 bandwidth knob turns into a dual-family bill.
mem_max prices the member side (T2 machinery, value fixed at the 8-byte sortable score); run_max prices the score side, where bigger runs also mean fewer runs per ZRANGE window and a shorter fence for the rank math, and smaller ones mean cheaper moves.
PRED-SQLO1-T4-WALZ's input is here too: WAL frames and bytes per score-moving ZADD under zipfian member reuse, the doc 14 wal-delta tripwire.

## Method

The model is the doc 09 shape resident, no store underneath (the salgebra pattern; the drain-substrate half of the segment-size trade was priced by T2's hseg lab on the real backends, and what T4 adds is the dual-family bill, which is arithmetic).
Member side: mh partitioning, sorted entries, fence binary search, median splits.
Score side: (score, member)-sorted runs with exact counts, median splits, whole-run death on empty.
The WAL column bills every committed frame group its full post-images plus an inline root or one fence page when the fence changes shape; drain traffic accumulates dirty post-images against the 8 MiB threshold for the WA column.
Preload builds 4 x 100000 members through the two-sided insert path; the measured mix (200000 ops) then moves scores over the loaded universe with zipfian member reuse.
Mixes: zaddheavy (70 ZADD fresh score / 20 ZSCORE / 10 ZRANGE), zrangeheavy (10/10/80), board (10 ZADD fresh + 40 ZINCRBY-shaped bounded delta / 10 ZSCORE / 10 ZRANK / 30 ZRANGE from rank 0).
An oracle test pins the model against a reference map through scores, ranks, walks, counts, encoded sizes, and both fences' partitioning, at a threshold small enough to cross the split paths thousands of times.
The model's zrank is a flat prefix sum over run counts, so its latency overstates the fence-length term the engine's two-level (fence-paged) prefix sums will pay; the direction stands, the magnitude is an upper bound.

## Run

    ./run.sh            # 3 mixes x mem_max {2016, 4032, 8064} x run_max {2016, 4032, 8064}
    go run . -quick     # smoke
    go test ./...       # oracle, range window, transform mirror

## Results (local, 2026-07-17, macbook; the model is deterministic and the trade is arithmetic, so the shape is box-independent)

Frames per score-moving ZADD are exactly 3.00 under fresh random scores at every size (member segment plus remove-run plus insert-run; the same-run share is zero when the new score is uniform), and drop only through score locality: the board mix's bounded increments land 42/61/70 percent of moves in the same run at run_max 2016/4032/8064, for 2.58/2.40/2.30 average frames.

WAL bytes per move, board mix at mem_max 4032: 5064 / 6714 / 9744 for run_max 2016 / 4032 / 8064.
Each mem_max step adds ~1.4 KB more (the member post-image), with zero read-side win: ZSCORE sits at 210-300 ns at every size, one segment either way.

The range window, invariant across mixes: a 100-element walk touches 2.91 / 1.95 / 1.50 runs and pulls 4.0 / 5.4 / 8.0 KB encoded at run_max 2016 / 4032 / 8064.
The rank math follows the fence length (7656 / 3858 / 1994 runs at 10^5 members): 1431 / 850 / 624 ns in the model's flat prefix sum.

Splits and fence-shape bills per 1000 moves: 2.3 / 0.6 / 0.1 at run_max 2016 / 4032 / 8064 (zaddheavy; the churn keeps sizes drifting, so the steady state still splits occasionally).

Occupancy at 4032: 104 entries per run, 102 members per member segment, both ~2.7 KB encoded, matching doc 09's 80-150 density band.

## Verdict

mem_max stays 4032, the T2 segment family constant.
The member side inherits T2's machinery and its flatness evidence byte for byte; the sweep shows mem_max only scales the move bill linearly with no read-side return, so nothing argues to leave the family constant, and slice 2 bakes it.

run_max is also 4032, and it is a real knee rather than a default.
At 2016 a 100-element ZRANGE window pays 49 percent more cold runs (2.91 vs 1.95, and rounds are the latency bill), the fence is twice as long for every rank prefix-sum, and splits run 4x hotter; at 8064 every score move carries 45 percent more WAL bytes and the same window cold-reads 48 percent more payload to save 0.45 of a run it can never push below 1.
Slice 3 bakes 4032 with the fence entry's u32 count exact.

The WALZ tripwire, priced for the prediction note: a score-moving ZADD on the board mix bills 2.40 frames and 6.7 KB WAL average at the chosen sizes against 24 logical bytes, roughly 2.4x the single-family HSET bill T2 shipped with.
That is the number PRED-SQLO1-T4-WALZ puts on record, and the doc 14 logical-zset-WAL lever stays a priced v2 candidate on the same W4 deferral T2 took; nothing here forces it before the gate run.

The sweep CSV (hsegz.csv) stays untracked, like every lab CSV.
