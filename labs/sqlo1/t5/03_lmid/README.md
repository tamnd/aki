# lmid: adversarial middle inserts and the lazy merge threshold

Milestone T5 lab 03 (spec 2064/sqlo1 doc 07 sections 3 and 8).

## Question

LINSERT and LREM work where the deque path's edge amendment does not apply: a middle insert into a full node splits it, and middle removals shrink nodes that never empty.
The doc 14 kill-table row for the list type is unbounded fence growth under this churn, as nodes erode toward slivers that each still cost a fence entry and a seek step.
The counterweight is lazy merge, and this lab prices its threshold: merge_max in encoded pair bytes, with 0 disabling it, against the WAL surcharge every merge bills (both images become one, plus a tombstone and a fence cut).

## Method

The model is the lnode lab's resident shape (doc 07 nodes behind an ordered fence, WAL billed per doc 06 W2/W4) plus the two middle operators: insertAt splits a full node at its byte midpoint, removeAt drops emptied nodes or lazily merges a shrunken survivor with whichever neighbor makes the smaller pair when that pair fits merge_max under both caps.
Node thresholds are fixed at the lnode verdict (4032/128); elements are 100 B.
Mixes: storm (every insert at the same middle position on a growing list, the pure split adversary), churn (steady length 10^5, one random-position insert plus one random-position remove per pair), decimate (steady length 2 x 10^4, each round removes every other element across the whole list then refills at one fixed point, so eroded nodes are never backfilled and only decay further next round).
An oracle test pins insertAt and removeAt against a reference slice at tiny thresholds with merge off and at two thresholds; a second test holds the churn bound in miniature with the no-merge arm required to be measurably worse.

## Run

    ./run.sh            # {storm, churn} then decimate, each x merge_max {0, 1008, 2016, 3024}
    go run . -quick     # smoke
    go test ./...       # oracle, churn bound

## Results (local, 2026-07-17, macbook; the model is deterministic and the trade is arithmetic, so the shape is box-independent)

The storm is bounded by construction and merge never fires on it: fixed-position inserts settle at 0.492 occupancy at every merge_max (splits leave halves and nothing removes), 52.6 splits per 1000 ops, 3370 WAL bytes per op.
The B-tree half-full floor, not a pathology.

Random churn barely erodes even with merge off: 200000 insert-remove pairs on a 10^5 list grow the fence 1.044x at occupancy 0.471, because random inserts backfill what random removals erode.
merge_max 2016 turns the drift negative (0.983x, occupancy 0.500) for a 1 percent WAL surcharge (2161 vs 2140 B per op); 1008 almost never fires (0.045 merges per 1000); 3024 pays 12 percent more WAL to reach 0.593.

The decimation adversary is where the row would trigger, and it separates the thresholds cleanly: with merge off the fence grows 3.09x at occupancy 0.159 and is still eroding per round; 1008 slows it (1.74x, 0.282) but does not bound it; 2016 holds it flat (1.003x, occupancy 0.490, indistinguishable from the start state) for 13.7 percent more WAL on this deliberately hostile mix; 3024 matches 2016's bound (0.998x, 0.493) while billing another 10 percent WAL on top.

## Verdict

merge_max is 2016, half of node_max, the classic half-merge rule, and the sweep shows it is the knee on both sides: it is the smallest threshold that bounds the decimation adversary flat, and its surcharge on realistic churn is 1 percent of WAL.
1008 fails the bound; 3024 doubles the churn surcharge and adds nothing to it.
The kill-table row does not trigger: with 2016 baked, all three mixes hold occupancy at or above the half-full floor and the fence never grows past its start shape under steady-length churn.
The slice that lands the noded layout bakes merge_max 2016 alongside the lnode verdict's 4032/128.

The sweep CSV (lmid.csv) stays untracked, like every lab CSV.
