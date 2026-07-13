# f3 prediction ledger

Pre-registered predictions, committed before each milestone's first gate run per doc 19 rule 1.3.
Falsified predictions that change the design become numbered reversal notes continuing the f1 R-trail from R8.

## M0

PRED-F3-M0-SPREAD. SET/GET/INCR at 64B values, 1M keys, uniform, P16: floor at the K2 carried cells, ceiling plus 10 to 25 percent over them.

PRED-F3-M0-HOT. Single-hot-key SET at P16: floor 2.0x over both rivals. A reading in the 1.5 to 2.0x band engages F13 same-key batching rather than failing the milestone outright; below 1.5x is a miss.

PRED-F3-M0-BIGVAL. GET and SET at 64KiB and 1MiB values: 2x both rivals with RSS bounded by the streaming window. Falsifier: any whole-value reply buffering observed in the profile.

PRED-F3-M0-LTMSTR. GET/SET over 1M keys at 1KiB values against a 512MiB resident cap (dataset roughly 2x the cap): floor 2x both rivals in the same scenario, with K7 recorded as ancestry.

PRED-F3-M0-P1REC. SET at P1, 50 and 512 connections: recorded only, no gate. Expected band 1.4 to 2.4x at 50 connections on the Go netpoller path; the number feeds the M10 campaign baseline.

## M1

Filed for issue #543 before the M1 gate run.
The gate run happens on the GamingPC box after the LTM campaign frees it; these floors are frozen now and carry no wiggle room after the numbers land.

PRED-F3-M1-SPOP. One hot key drawn from ~2M members, P16. Floor clears the Valkey bar (>= 2.12M ops/s); target >= 4.2M (2x Redis's 2.1M). A reading below 1.06M kills F2's story and forces a written autopsy; 1.06M to 2.1M engages the F13 partitioned-draw escalation rather than failing the milestone outright.

PRED-F3-M1-SINTER. 1M-and-1M equal-overlap and skewed pairs, P16. Floor 2x over both rivals. Falsifier: the inline sorted-array maintenance missing its own write gate.

PRED-F3-M1-STORE. SUNIONSTORE, SINTERSTORE, SDIFFSTORE. Floor 2x over both rivals (v1 was 0.30 to 0.55x, the flank this milestone attacks).

PRED-F3-M1-INLINE. SADD, SISMEMBER, SMEMBERS at cardinality 1 and 10. Floor 2x over both rivals at the listpack shapes.

PRED-F3-M1-SETMEM. Bytes per member at or under the Valkey 8.1 embedded-entry bar.

## M2

Filed for issue #544 before the M2 gate run.
Slices 1 through 7 are merged (#605, #610, #613, #615, #616, #619, #620); these floors are frozen before slice 8 lands and before any gate cell runs.

PRED-F3-M2-ZRANKZIPF. ZRANK zipfian s=0.99, P16. Floor 2x over both rivals (v1 read 1.86x and 1.83x, a miss). A reading in the 1.5 to 2.0x band engages F13 before any structural rework; below 1.5x is a miss.

PRED-F3-M2-ZRANGE. ZRANGE and ZRANGEBYSCORE over 10k-member windows. Floor 2x over both rivals (v1 read 1.59x to 2.20x).

PRED-F3-M2-ZSETMEM. Tree overhead at 2 to 3 bytes per entry per F14. Anything over 5 bytes per entry blocks the milestone. Watch item carried from #613: the dual structure measured 40.2 B/entry over member plus score on darwin against the doc's 34 to 36; the gate box settles it.

PRED-F3-M2-ZREMTAIL. ZREM against one hot key, P16. Floor 2x with p99 inside 125 percent of the best rival. v1's 7.6 to 8.1ms shoulder must be gone; lab 04 read the shoulder as a deferral artifact and the #619 delete path is inline.

PRED-F3-M2-ZADD. Hold or improve K4's carried 5.49x to 7.03x. Any regression blocks.

## M5

Filed for issue #547 before the M5 gate run.
Every stream slice is merged (#662 through #676, the labs at #661/#666/#675/#678 and labs 02/03 in-tree); v1 shipped zero stream code, so these are first-ever numbers and the floors are frozen before any gate cell runs on the GamingPC box.

PRED-F3-M5-XADD. XADD of a 3-field entry against one hot stream, P16. Floor 2x over both rivals, and within 15 percent of the same box's SET number, since an append is one ID allocation off the coarse shard clock plus one master-delta encode into the open block, no tree touch until a block seals. A reading below the SET number by more than 15 percent means the block append or the per-batch clock cache is paying more than a string set's arena write, and the block-append path is the lever. Falsifier: any per-entry directory insert on the hot path (the directory takes one insert per sealed block, ~1 in 128 appends).

PRED-F3-M5-XREADGROUP. The XREADGROUP `>` plus XACK loop against a 10k-entry stream, P16. Floor 2x against Redis 8.8's post-#14885 fast path, whose bar moved +83 percent when it rewrote the group cursor to a rax; the prediction names that raised bar and clears it anyway, because the f3 advantage is not the cursor (both are O(1) at steady state via the section 3.5 block memo) but the surrounding architecture: F1 shard parallelism, F6 batched execution, F19 replies, and the PEL write being one owner-local counted-tree insert (lab 03's tree-only PEL) rather than two rax insertions. Falsifier: a miss here with XADD and the tail read (S4/S5) passing isolates the PEL write path, and the PEL-pairing question re-opens.

PRED-F3-M5-STREAMMEM. Native-band overhead 7 to 9 bytes per entry over the field payload, directory ~0.25 bytes per entry, both measured as (server RSS after load minus baseline) over entry count against the same load on both rivals. Lab 01 froze the 4096/128 block geometry at 7.36 B/entry overhead for the 64B fixed-schema entry, inside the 6-to-8 bar, with the directory at 0.25 B/entry over the counted tree; the ID field rides the delta codec lab 05 priced (shipped base-delta, ~2.5 B/entry on dense IDs, a switch to the ~0.5-to-1.2-B/entry-smaller successive base pending a follow-up slice). Anything over 10 B/entry native overhead blocks the milestone. The PEL is reported alongside at ~76 B per in-flight pending entry (lab 03's tree-only figure), not a battleground but carried for honesty against Redis's streamNACK-in-two-raxes.
