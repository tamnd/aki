# hfence: point-op flatness across the ladder

Milestone T2 lab 02 (spec 2064/sqlo1 doc 06 sections 1-2.3).

## Question

Does a point op cost the same records on a billion-field hash as on a ten-field one?
That is the hash model's design target: root, then one fence page in fence-paged mode only, then one segment, never more.
T2 slice 9 bakes the fence paging and its 3-record cold path, and PRED-SQLO1-T2-FLAT rides on the curve this lab draws.

## Method

The lab materializes the full ladder (inline root, segmented with the fence inline in the root, fence-paged with rtype-5-shaped page rows) at 10^2 to 10^9 fields and prices the point lookup cold and hot.
Preload builds segments directly at target occupancy instead of pushing 10^9 HSETs through a resident model: field hashes are placed deterministically inside each segment's fence range and the field name and value derive from the hash, so any slot can be regenerated at lookup time and verified byte-exact while the clock runs.
Every lookup counts its record reads and the run fails if any lookup exceeds its tier's ceiling (1 inline, 2 segmented, 3 fence-paged), so a fast-but-wrong path cannot win.
The cold arm reopens the connection per lookup with the open outside the clock; the hot arm reuses one warm connection, and the engine proper would do better still by keeping the root resident.
An oracle test shrinks every threshold so all three tiers appear at tiny counts, looks up every preloaded field at exactly the tier's read cost, probes an absent field, and asserts the plan-time refusal past the one-level page index (doc 06 keeps a third level out of scope).

Read the sweep as: ns_per_op and rec_reads across the fields axis are the flatness claim; root_b and fence_mb are the RAM a resident hash pins beyond its hot segments (fence bytes are per segment, not per field, which is the memory-bar story).
Caveat: below the box's RAM the OS page cache blunts the cold arm (SQLite-cache-cold only); the 10^9 run is the true cold point.

## Run

    ./run.sh                      # fields {30, 1e2, 1e4, 1e6, 1e8}, gate box
    go run . -fields 1000000000   # the 10^9 point, ~56 GB, box disk decides
    go run . -quick               # smoke
    go test ./...                 # smoke plus the ladder oracle

## Results

Pending: runs on the gate box after the A2 queue frees it.

## Verdict

Pending.
