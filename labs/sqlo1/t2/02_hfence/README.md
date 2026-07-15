# hfence: point-op flatness across the ladder

Milestone T2 lab 02 (spec 2064/sqlo1 doc 06 sections 1-2.3).

## Question

Does a point op cost the same records on a billion-field hash as on a ten-field one?
That is the hash model's design target: root, then one fence page in fence-paged mode only, then one segment, never more.
T2 slice 9 bakes the fence paging and its 3-record cold path, and PRED-SQLO1-T2-FLAT rides on the curve this lab draws.
Since B3 the suite runs on both backends: -store a is the SQLite schema below, -store b the same ladder over sqlo1b records, and the verdict reads from the arm that will actually carry the fence paging.

## Method

The lab materializes the full ladder (inline root, segmented with the fence inline in the root, fence-paged with rtype-5-shaped page rows) at 10^2 to 10^9 fields and prices the point lookup cold and hot.
Preload builds segments directly at target occupancy instead of pushing 10^9 HSETs through a resident model: field hashes are placed deterministically inside each segment's fence range and the field name and value derive from the hash, so any slot can be regenerated at lookup time and verified byte-exact while the clock runs.
Every lookup counts its record reads and the run fails if any lookup exceeds its tier's ceiling (1 inline, 2 segmented, 3 fence-paged), so a fast-but-wrong path cannot win.
The cold arm reopens the connection per lookup with the open outside the clock; the hot arm reuses one warm connection, and the engine proper would do better still by keeping the root resident.
An oracle test shrinks every threshold so all three tiers appear at tiny counts, looks up every preloaded field on both arms at exactly the tier's read cost, probes an absent field, and asserts the plan-time refusal past the one-level page index (doc 06 keeps a third level out of scope).
On the b arm the root is a plain record under the user key, segments and fence pages are subkey records under a minted rooth (kinds SubkindSeg and SubkindFence), a preload write set is one DrainBatch kept well under the 64 MiB WAL segment, and the checkpoint cadence calls the store's checkpoint, so a cold lookup is the same three Gets the a arm pays as row probes.
Cold on the b arm is one checkpoint, close, and reopen of the store before the phase: there is no per-connection page cache to drop, so the cold row prices the post-open read path rather than per-lookup eviction.

Read the sweep as: ns_per_op and rec_reads across the fields axis are the flatness claim; root_b and fence_mb are the RAM a resident hash pins beyond its hot segments (fence bytes are per segment, not per field, which is the memory-bar story).
Caveat: below the box's RAM the OS page cache blunts the cold arm (SQLite-cache-cold only); the 10^9 run is the true cold point.

## Run

    ./run.sh                      # both arms x fields {30, 1e2, 1e4, 1e6, 1e8}, gate box
    go run . -fields 1000000000   # the 10^9 point, ~56 GB, box disk decides (add -store b for the Track B arm)
    go run . -quick               # smoke (add -store b for the Track B arm)
    go test ./...                 # smoke plus the ladder oracle, both arms

## Results

Pending: runs on the gate box after the A2 queue frees it.

## Verdict

Pending.
