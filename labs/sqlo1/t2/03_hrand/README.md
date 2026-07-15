# hrand: HRANDFIELD fence-weighted sampling

Milestone T2 lab 03 (spec 2064/sqlo1 doc 06 section 3).

## Question

Does picking a segment by the fence fill class, then a uniform entry inside it, keep HRANDFIELD uniform over fields?
T2 slice 8 bakes the weighting rule and the fill-class bit width, and the fence-entry meta has 15 bits left after the has-TTL bit.
Segments split at the entry median and merges are lazy, so occupancy sits anywhere between half full and full and drifts lower under deletes; unweighted segment picking is therefore biased by construction, and the lab must show both that weighting fixes it and how much precision the weight needs.

## Method

No store underneath: rules W1 and W2 drain fill and segments in the same batch, so the drained fill class is never stale, and the per-draw record cost is the operator table's O(samples) with hfence pricing record reads; the only thing left to guard is the distribution.
The lab builds a segmented hash through the real insert-split path, churns a quarter of the fields out and back in to widen the occupancy spread, then draws with-replacement samples (the negative-count contract; positive counts sample distinct on the same per-draw law) under four weightings: exact counts, the 15-bit capped fill class as shipped, a 4-bit quantized class, and the unweighted null.
Chi-square against uniform over all live fields gives chi2/dof and its z-score; the worst segment-level relative deviation shows where bias lives before per-field noise settles.
Tests pin the model: build invariants (fence partition, split threshold, churn conservation), hand-built sampler ratios including a zero-weight segment, and the oracle that exact passes while the null fails.

Read the sweep as: exact and fill15 must sit inside |z| < 3 at every field count, unweighted must sit far outside it, and quant4's z at the big counts prices what quantizing the fill class would cost if its meta bits ever get taken.

## Run

    ./run.sh            # fields {5e4, 2e5, 1e6}, four arms each
    go run . -quick     # smoke
    go test ./...       # invariants, sampler ratios, pass/fail oracle

## Results

Pending: deterministic and box-independent, but recorded from the gate box run with the other T2 labs for provenance.

## Verdict

Pending.
