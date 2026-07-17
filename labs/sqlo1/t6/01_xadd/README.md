# xadd: the stream run size and entry cap

Milestone T6 lab 01 (spec 2064/sqlo1 doc 10 sections 2, 3, and 7).

## Question

T6 slice 2 bakes the run cut thresholds that bind whichever comes first: run_max in encoded bytes and ecap in entries.
The trade is the same W4 bandwidth knob the list side priced, shifted to the stream's append-only shape: every XADD amends the tail run and bills its full post-image in the frame group, so bigger runs make steady append traffic carry more WAL bytes per op, while smaller ones cut runs more often (each cut a fence-shape bill), lengthen the fence, and page it earlier.
What the list lab did not have to price is the run encoding itself: doc 10's master-entry form with a field name table, varint ID deltas, and per-field name references has to earn its complexity against a naive per-entry encoding, and the encode arm measures both the byte ratio and the ns cost.
PRED-SQLO1-T6-XADD takes its pricing input here: WAL frames and bytes per XADD at the chosen thresholds.

## Method

The model is the doc 10 shape resident, no store underneath (the lnode pattern; the drain substrate was priced by T2's hseg lab and what T6 adds is the append-shaped bill and the codec).
Runs hold entries in ID order behind an ID-keyed fence, ~70 entries inline in the root and kind 3 pages of 146 beyond, and the root keeps the per-page index.
The WAL column bills every XADD its amended tail run's full post-image plus the structural bill when the fence changes shape (inline root whole, or fence page plus the root page index once paged); a dropped run bills a 16 B tombstone; approximate trim cuts whole runs only and exact trim rewrites at most one edge, both held by an oracle test against a reference slice.
The codec is real, not arithmetic: encodeRun and decodeRun implement the doc 10 run payload byte for byte, a roundtrip test pins them across bursty and sparse IDs and one to sixteen fields, and a third test holds the model's incremental size tracking to the real encoder's output on every run.
Drain traffic accumulates deduped dirty run images against the 8 MiB threshold, shared across streams so the fanout mix prices global coalescing.
Mixes: append (pure auto-ID XADD, 4 fields, burst 10 per ms), feed (XADD MAXLEN ~ 100000 per op), fanout (round-robin XADD over capped streams), encode (encode ns and bytes per entry over the sealed runs against a naive 16 B ID plus full names plus u32 lengths form).

## Run

    ./run.sh            # append, feed, encode x run_max {2016, 4032, 8064} x ecap {64, 128, 256} x elen {100, 1000, 4096}; fanout x nstreams {10, 100, 1000}
    go run . -quick     # smoke
    go test ./...       # model oracle, codec roundtrip, size arithmetic

## Results (local, 2026-07-17, macbook; the model is deterministic and the trade is arithmetic, so the shape is box-independent)

The append bill scales with the run, not the op: 1471 / 2217 / 4179 WAL bytes per XADD at run_max 2016 / 4032 / 8064 for 100 B values, at 1.059 / 1.029 / 1.014 frames since only a cut adds a second frame, the mirror of the list queue bill.
The knee is at 1000 B values: 2016 fits one entry per run, so every XADD is a cut plus a structural bill (2.0 frames, 53 KB per op against 8.8 KB at 4032, which still packs three per run).
4 KiB values degenerate to one entry per run at every size in the sweep, which is the standing argument for the doc 10 note that fat stream values want the string type's blob door rather than a bigger run.
Deep untrimmed appends surface a real tail cost the slice should carry forward: at 500k runs the fence is thousands of pages and the model bills the root's per-page index on every cut, which is where the 53 KB pathology comes from; the real root must keep that index at 12 B per page and the W2 elision follow-up applies to stream roots the same as list roots.

The feed pair follows the same knee: 1685 / 2329 / 4236 B per op at elen 100 with drops amortized to one whole run per run-fill (28.6 per 1000 ops at 4032), and at elen 1000 the 2016 column collapses to 4.0 frames and 47.7 KB while 4032 holds 2.0 frames and 9.1 KB and 8064 1.43 and 6.1 KB.
Steady length holds within 32 entries of the cap at every size, so approximate trim's whole-run rule costs nothing observable at realistic caps.

The encode arm earns the format: 112 B per entry against 161 naive at 100 B values (ratio 0.70) and ~21 ns per entry, with the win coming from the name table (four names paid once per run instead of per entry) and the varint ID deltas (2 to 4 B against 16).
At 1000 B values the ratio is 0.96 to 0.97 and at 4 KiB it is 1.00, so the format is free where values dominate and 30 percent where they do not, at a cost the append ns column cannot distinguish from noise.

Fanout is where the drain model pays off: round-robining 100 capped streams at run_max 4032 bills 1.122 frames and 2393 B WAL per op, and the data-file write amplification holds at 1.1 with 13 drains over the sweep, since every dirty tail run coalesces its amendments between drain windows.
At 10 streams the whole working set sits under the 8 MiB threshold and never drains at all, and at 1000 streams WA is still 1.1 to 1.2, so the WAL is the whole bandwidth story for streams exactly as it was for lists and drains stay noise.

## Verdict

run_max is 4032, the family constant, and the elen 1000 column is what makes it a measured floor rather than an inheritance: 2016 degenerates to one entry per run for kilobyte entries and pays 6x the feed bill, while 8064 saves bytes only on those same fat entries (6.1 against 9.1 KB) and pays 80 to 90 percent more WAL per op on the small-entry mixes that dominate stream traffic.
ecap is 128: the byte cap binds first for every payload at 100 B and up (35 entries per run at 4032), so the entry cap only exists to bound tiny-entry runs, where 128 keeps the tomb bitmap at 16 B and the per-run decode cost flat, and it matches the doc 10 provisional figure and the list side's constant.
Slice 2 bakes 4032/128.

For the prediction note: a steady 4-field 100 B-value XADD bills 1.03 frames and ~2.2 KB WAL at the chosen sizes with WA 1.0 to 1.1, the same bill a T5 queue push carries, and the encode cost is ~21 ns per entry at ratio 0.70 against the naive form; those are the inputs PRED-SQLO1-T6-XADD will put on record.

The sweep CSV (xadd.csv) stays untracked, like every lab CSV.
