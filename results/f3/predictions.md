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
