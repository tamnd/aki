# M5-G6 XREADGROUP: drained nil-poll ~2x faster, gate row a harness artifact

Connect-mode aki-bench on the GamingPC gate box (2026-07-19), f3srv reactor gate
config vs CF16-frozen rivals (redis io6, valkey io4), card-10k stream, P16, single
generator.

## The gate row is a harness artifact

The `xreadgroup` aki-bench workload reads with `>` (new entries): the first read
drains the group, then every subsequent read returns nil, so the value-bearing-ops
gate reads 0 for aki AND both rivals (the M1-G5 / M3-G8 drain-to-empty shape). The
2x gate cannot be scored on a workload where every target reports zero vops. So
M5-G6's throughput gate is a harness artifact, not a pass/fail signal, and its
correctness (groups, PEL, XACK, durability) is covered by the M8 Slice B arc and
the engine test suite, not this row.

## The drained nil-poll path, made ~2x faster anyway

The nil-poll rate the artifact exposes was a real inefficiency worth fixing. A
caught-up consumer polling `>` reads nil on every call, and aki read those nils far
slower than redis:

| stage | aki nil/s | redis | valkey | vs redis |
|---|---|---|---|---|
| before | 279K | 2.27M | 2.06M | 0.12x |
| + drained short-circuit | 511K | 2.21M | 1.97M | 0.23x |
| + lazy results alloc | 550K | 2.35M | 2.02M | 0.23x |

Two cheap fixes, ~2x total (279K -> 550K):

1. **Drained-cursor short-circuit** (readAfter + XREAD immediate loop). The empty
   `>` read resolved to a forward range walk over `(cursor, +inf]`, which seeked
   the tail block and walked its whole entry run testing each against the exclusive
   lower bound, all failing, before returning nil. On a card-10k stream that walked
   hundreds of entries per empty poll. The fix returns nil the moment the cursor is
   at or above `s.lastID`, the O(1) not-newer check Redis makes first
   (labs/f3/m5/09_drained_read_shortcut: flat ~2ns vs the walk's 14-3597ns scaling
   with tail-block size).

2. **Lazy results-slice alloc**. The `[key, entries]` results slice allocated on
   every call; it now allocates only once a stream produces entries, so a nil poll
   returns the null array with no allocation.

## The residual gap: multi-key stream dispatch

At 550K vs redis's 2.35M, aki is still ~4.3x behind on this single-generator nil
poll. This is not the walk (removed) or the alloc (removed): it is the per-command
dispatch cost of a multi-key stream command. The single-generator point-op ceiling
(SISMEMBER/HGET, all pinned ~2.6-3.3M under one generator) is the client cap; aki's
XREADGROUP at 550K sits well below it, so aki is server-bound here, not
client-capped. XREADGROUP routes through the multi-key co-location check and the
group/consumer name lookups, a heavier dispatch than the single-key point path the
reactor fast-lane carries to the client cap. Closing it is a dispatch/routing
change on the intent path, not a surgical read-path fix, and it is not gate-blocking
(the row is zero-vops for everyone), so it is left as a noted dispatch-path residual
rather than spent here. Evidence: this file, gate doc M5-G6.
