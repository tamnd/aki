# zrank: the score fence shape at 10^9

Milestone T4 lab 02 (spec 2064/sqlo1 doc 09 sections 2 and 10).

## Question

T4 slice 5 bakes the score fence's paged shape, and the flat fence shipped in slice 3 caps at 100 runs, roughly 10^4 members at the hsegz occupancy, against a headline that says ZRANK stays flat to 10^9.
Doc 09 sketches one root page index with per-page totals, but the arithmetic does not close: 10^9 members are ~10^7 runs at ~104 entries per run, and a 250-entry root over 250-entry pages addresses 62500.
Something gives, and each candidate has a price with a different unit.
A longer root index bills every command, because the root is the plane's commit point and carries its full frame each time.
Bigger or deeper index pages bill per score move, because a move edits a run count and the edit propagates one node per level.
More levels also lengthen the cold path, one record per level.
The lab prices all three so the slice can pick fanouts on numbers, and it prices the drain-coalescing lever on fence pages so slice 5 knows whether it must build it.

## Method

The model is fence arithmetic only, resident, no store (the salgebra pattern).
Run contents never matter above the leaf, so runs are synthetic: a per-run entry count array is the leaf, index levels group it by a fanout per level, and one encoded run image stands in for the in-run scan every rank pays.
The data-record side of a move's bill (member segment plus two run post-images, ~2.7 KB each) is carried as constants from the hsegz occupancy row, since this lab only decides the fence shape on top of them.
Shapes: flat, one-level fanouts 250 and 1000, two-level 128x128, 250x250, 512x512, each swept 10^3 to 10^9 members under uniform and board (bounded-delta) move locality.
The hot walk is timed on the real arrays; the move bill has a strict arm (every touched node is a post-image in the command's frame group) and a deferred arm (dirty pages coalesce across a 64-command drain window, root still per command).
An oracle test pins the walk against a brute prefix sum at every run boundary and under churn, and pins the per-level touched-node counts of every move.

## Run

    ./run.sh            # 6 shapes x 10^3..10^9 x {uniform, board}
    go run . -quick     # smoke
    go test ./...       # walk oracle, level totals, touched-node counts

## Results (local, 2026-07-17, macbook; the model is deterministic and the trade is arithmetic, so the shape is box-independent)

Flat and one-level die on the root-per-command bill, not the walk.
Flat is 188 KB of root at 10^6.
One-level p250 reaches 10^9 only by growing the root to 38462 entries, 901 KB billed every command, with the rank p99 at 33 us on top; p1000 still carries 225 KB of root.

Two-level shapes all hold the walk sub-microsecond to 10^9: p250x250 runs 42 ns p50 at 10^3 to 334 ns p50 / 708 ns p99 at 10^9, p128x128 and p512x512 within 2x of that.
What separates them is where the bytes sit.
p128x128 has the cheapest pages (2.5 / 3.0 KB) but its root grows to 587 entries, 13.8 KB at 10^9, billed every command.
p512x512 keeps the root at 37 entries, 0.9 KB, but each touched page is 10 / 12 KB and the uniform move bill hits 52.5 KB.
p250x250 balances both: 154 root entries, 3.7 KB at 10^9, pages 5.0 / 5.9 KB, and the board move bill is 22.4 KB strict against 8.1 KB of data records, the best of the six at the headline cell.

The deferred arm saves 34-63 percent on uniform moves in the 10^5 to 10^8 mid-band, where few pages exist and the window coalesces them.
At the 10^9 headline it fades to 11 percent uniform and 5 percent board, because moves scatter over 38K leaf pages and almost nothing coalesces.

The cold path at p250x250 is 5 records (member segment, root, upper page, leaf page, run), 19.3 KB at 10^9, two records over the hash paged cold path.

## Verdict

The score fence pages two-level at 250/250, the hash fence paging constants again.
Leaf pages hold up to 250 fence entries (20 B: score_lo, segid, u32 count) at ~5.0 KB, upper pages up to 250 index entries (24 B, u64 subtree total since a top subtree overflows u32) at ~5.9 KB, and the root holds up to 250 upper entries, 5.9 KB worst.
Reach is 250^3 runs, ~1.6e9 members at the hsegz occupancy, past the 10^9 headline; a third level errs like the hash fence does, capacity is not silent.
At the headline itself the root is 3.7 KB, under one 4 KB yardstick, and the rank walk is 708 ns p99 across six decades, which is the flatness PRED-SQLO1-T4-RANK will claim.

Slice 5 bills fence pages strictly in the command's frame group and does not build the drain-coalescing lever.
At the gate cell the lever buys 5 percent; the mid-band save is real but the mid-band bill is half the headline bill anyway, and strict keeps the W1 with-or-after discipline byte for byte the same as segments.
It joins hsegz's wal-delta as a priced v2 candidate.

The WALZ restatement this lab puts on record: paged mode adds the root and one or two page images to hsegz's 2.40-frame 6.7 KB flat-era move bill, so a 10^9 board move bills ~22.4 KB and up to 8 frames worst case (3 data, root, up to 4 pages), and the prediction note must carry the paged numbers, not the flat-era ones.

The sweep CSV (zrank.csv) stays untracked, like every lab CSV.
