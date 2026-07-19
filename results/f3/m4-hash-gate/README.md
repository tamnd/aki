# M4 hash gate

## M4-G4 HGETALL stream, 10k band (PASS)

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), same rig as the M1
set gate: f3srv gate config vs CF16-frozen rivals (redis io6, valkey io4), card
10k fields per hash, P16, 8s + 3s warm.

| workload | aki ops/s | redis | valkey | vsR | vsV | min | verdict |
|---|---|---|---|---|---|---|---|
| HGETALL (rep 1) | 3291 | 1579 | 1129 | 2.08x | 2.92x | 2.08x | PASS |
| HGETALL (rep 2) | 3349 | 1589 | 1072 | 2.11x | 3.12x | 2.11x | PASS |
| HGETALL (rep 3) | 3267 | 1584 | 1106 | 2.06x | 2.95x | 2.06x | PASS |

Median-of-3 2.08x redis / 3.06x valkey, a clean PASS.

This row passes on the same lever that flipped the 10k SMEMBERS gate cell: PR #1193
carried the reply-copy elision (frame each element straight onto the wire chunk
instead of scratch-then-copy) to the HGETALL/HKEYS/HVALS enumeration stream. The
framing kernel is priced and proven byte-identical in `labs/f3/m1/13_smembers_copy`.
The range-reply double-copy, not per-command dispatch, was the binding cost for
these full-collection reads.
