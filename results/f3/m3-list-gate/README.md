# M3 list gate: LINDEX positional read (M3-G4) PASS

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), same rig as the M1
set, M2 zset, and M4 hash gates: f3srv gate config vs CF16-frozen rivals (redis
io6, valkey io4), card 10k list, positional LINDEX, P16, 8s + 3s warm,
median-of-3.

| rep | aki ops/s | redis | valkey | vsR | vsV | verdict |
|---|---|---|---|---|---|---|
| 1 | 2775582 | 1123585 | 1060690 | 2.47x | 2.62x | PASS |
| 2 | 2605078 | 1126129 | 1059938 | 2.31x | 2.46x | PASS |
| 3 | 2712340 | 1122063 | 1055955 | 2.42x | 2.57x | PASS |

Median-of-3 **2.42x redis / 2.57x valkey, all three reps PASS**.

## Why this passes where the range reads do not

LINDEX is a single positional read, not a window walk. aki's list holds its
elements in a packed cursor with O(log n) descent to an index, then reads one
element and frames a single bulk reply. redis and valkey answer LINDEX by walking
their quicklist node chain to the index, which on a 10k list crosses several
listpack nodes per read. The reply is one small bulk either way, so this row is
not reply-encode bound like ZRANGE, and it is not per-op index-insert bound like
ZADD. It is a pure indexed read where aki's packed representation has a real
structural edge over the quicklist walk, and the reactor networking edge stacks
on top rather than being diluted by collection compute. That combination clears
2x against both rivals with margin (2.31x to 2.47x redis across three reps, never
marginal).

This is the read-side counterpart to the write-floor finding: the collection
point *writes* fail 2x because per-op index compute dilutes the reactor edge, but
a collection point *read* that redis answers with a chain walk (LINDEX) keeps the
edge intact and passes.

## Row status

M3-G4 LINDEX: GREEN, median-of-3 2.42x redis / 2.57x valkey.
