# f3 prediction ledger

Pre-registered predictions, committed before each milestone's first gate run per doc 19 rule 1.3.
Falsified predictions that change the design become numbered reversal notes continuing the f1 R-trail from R8.

## M0

PRED-F3-M0-SPREAD. SET/GET/INCR at 64B values, 1M keys, uniform, P16: floor at the K2 carried cells, ceiling plus 10 to 25 percent over them.

PRED-F3-M0-HOT. Single-hot-key SET at P16: floor 2.0x over both rivals. A reading in the 1.5 to 2.0x band engages F13 same-key batching rather than failing the milestone outright; below 1.5x is a miss.

PRED-F3-M0-BIGVAL. GET and SET at 64KiB and 1MiB values: 2x both rivals with RSS bounded by the streaming window. Falsifier: any whole-value reply buffering observed in the profile.

PRED-F3-M0-LTMSTR. GET/SET over 1M keys at 1KiB values against a 512MiB resident cap (dataset roughly 2x the cap): floor 2x both rivals in the same scenario, with K7 recorded as ancestry.

PRED-F3-M0-P1REC. SET at P1, 50 and 512 connections: recorded only, no gate. Expected band 1.4 to 2.4x at 50 connections on the Go netpoller path; the number feeds the M10 campaign baseline.
