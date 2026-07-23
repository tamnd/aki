# zdict: does the trained dictionary earn its lifecycle

Milestone B4 lab 02 (spec 2064/sqlo1 doc 04 section 11, tracking issue #724).

## Question

Doc 04 scheme 6 is zstd with a trained dictionary behind a full lifecycle: ~112 KiB dictionaries on ~100x samples, replacement on a 5 percent held-out win, at most 4 live per file, retirement tracked through compaction.
The research number motivating it (3x to 4x on small records) comes from per-record compression, and B4 compresses groups, so before the zstd slice builds any of that machinery this lab asks where a dictionary still pays once the group carries its own context, how big it needs to be, how much training data it needs, and whether the 5 percent replacement trigger ever fires under workload drift.

## Method

Real training and real codecs: klauspost dict.BuildZstdDict for the dictionary, zstd at SpeedDefault with WithEncoderDict/WithDecoderDicts against a plain arm, groups framed with the cascade lab's raw framing so ratios line up across the two labs.
Four small-value corpora with two template families each for drift: json (~90 B templated bodies), sess (structured token with a 32-hex core), user (short profile strings over small vocabularies), rand (64 random bytes, negative control).
Four workloads: dictsize sweeps the dictionary size at 100x training and 64-value groups, gsize sweeps values per group at 112 KiB, train sweeps training volume as a multiple of the dictionary size, and shift trains the incumbent on template set 1, drifts the corpus toward set 2, and reports a fresh candidate's held-out byte win over the incumbent, the number the 5 percent trigger reads.

## Run

    ./run.sh > zdict.csv    # dictsize 16/48/112/224 KiB x 4 shapes; gsize 4..4096 x 3; trainx 10/30/100/300 x 3; drift 0..1 x 3
    go test ./...           # framing and dict round trips, dict-beats-plain premise, degenerate-corpus fallback, drift generator sanity

Column key: ratio is the dict arm, x1 the plain zstd arm, x2 the dict win pct over plain (shift: the candidate's held-out win over the incumbent).

## Verdict (local, Apple Silicon, 2026-07-23)

The dictionary pays only where the group is too small to carry its own context, and the cascade lab already ruled those groups out.
At 4-value groups the win over plain zstd is real (json 15.5 percent, user 20.9); by 64-value groups it is 3 to 8 percent; by 256 it is 0.5 to 3; at 1024 and 4096, the selection window the cascade lab says compaction should use, it is 0.8 percent at best and negative on sess.
Sess is the cautionary shape: the random hex core poisons the dictionary and every dictionary above 16 KiB loses to plain zstd outright, so an untyped trainer can make compression worse.
Rand pins the fallback: training panics or fails on incompressible corpora (klauspost BuildZstdDict panics rather than erroring on some degenerate inputs, v1.19.1, so the engine trainer needs a recover), and the trained dict loses slightly when it does build.

The lifecycle constants do no work on these shapes either.
Dictionary size is flat from 16 KiB to 224 KiB (json 3.2 percent win at both ends, user peaks at 112 with 8.1 against 7.9 at 16), so 112 KiB buys nothing 16 KiB does not.
Training volume is flat from 10x to 300x, so the 100x sample stream is over-provisioned.
The 5 percent replacement trigger essentially never fires: even at full template drift the fresh candidate beats the stale incumbent by only 4.2 percent on json, 0.8 on sess, and the single crossing (user at quarter drift, 6.1 percent) is within a point of the threshold.
That is consistent, not broken: where the dictionary barely matters, staleness barely matters.

So the verdict for the slices: build scheme 5 (plain zstd) as the fallback workhorse and demote scheme 6 to a boxed follow-up alongside FSST, gated on compaction ever emitting small groups in practice.
The 4-dictionary lifecycle, the trainer goroutine, and the retirement tracking are not worth their complexity for under 1 percent at extent-stream selection windows; if the box ever opens, train at 16 KiB on 10x samples, per type family, never on shapes with high-entropy cores, and wrap the trainer in a recover.
