# Lab 01: chunk capacity and element cap

Part of issue #545, the M3 list milestone.
This lab lands before the chunked-deque slice so the slice bakes a settled chunk
geometry and element-inline cap, not a guess.

## Question

Doc 13 (13-list-model.md) builds the native list as an owner-local ring of
fixed-capacity chunk slabs (section 2.2): each chunk holds a contiguous run of
positions packed as uvarint-length-prefixed frames, with a u16 offset directory
for O(1) in-chunk random access and a `firstLive` pop cursor.
Section 2.3 fixes the default at a 4KiB blob budget with a 128-element cap,
whichever binds first, mirroring the way Redis tunes quicklist nodes with
`list-max-listpack-size -2` and its 128-element directory ceiling.
It pre-registers the sweep in the same breath (section 2.3, section 10 exit 1):
sweep 2/4/8 KiB budgets and caps 64/128, score on end-push throughput, interior
memmove cost, and bytes-per-element overhead, and freeze one geometry for all
bands unless a gated row cannot clear at any single value.
This lab runs that sweep, extends it down to 512B and 1KiB to bracket the loss
side, settles the chunk geometry and the element-inline cap, and pins the
observed Redis conversion thresholds the inline-band slice mirrors.

The memory bar is PRED-F3-M3-LISTMEM, which F14 states as 3 to 4 bytes of
overhead per element at or under quicklist for the native band (doc 13 section
8.2: one uvarint prefix plus one u16 directory entry is the 3-4B floor, plus the
8-byte chunk header and slab slack bounded by one max frame).
Section 9.3 pins the block line: over ~4.5B per element at the 64B band trips the
lean-directory lab item.

## Method

In-process, no server, no wire, no engine import.
The chunk deque here is lab-local code that models the doc's section 2.2 layout so
the geometry can be priced before the slice writes it: each chunk is a
fixed-capacity slab carrying its live-ordered frames in a byte blob plus a u16
offset directory, so an interior edit memmoves real bytes and the byte accounting
is honest.
Resident cost is counted as chunks times the slab budget, so slab slack sits on
the F14 ledger the way the bar requires rather than being hidden; a full chunk's
slack is bounded by one max frame exactly as section 8.2 states.
A frame whose value meets the reference threshold stores a 16-byte value-log
reference in the chunk and books the payload to the value log (F5), which is the
element-inline cap the second sweep settles.
The ring carries a flat per-chunk live-count directory (the Fenwick crossover is
lab 02); this lab prices only its maintenance, not its seek.

Three sweeps:

- Sweep A walks the byte budget (512/1024/2048/4096/8192) at caps 64 and 128,
  per value band (64B, 1KiB), reading RPUSH ns (edge append), LPOP ns (edge pop),
  the pure in-chunk interior surgery ns (memmove plus directory shift, isolated
  from the pivot scan and the directory select), elements per chunk, and bytes of
  overhead per element beyond the payload.
- Sweep B walks element size against the 4096/128 budget, inline against a 16-byte
  reference, to find where inlining stops paying.
- Sweep C prices the per-chunk count maintenance the deque pays per push, the term
  lab 02's directory builds on.

`go run .` runs all three sweeps; `-quick` shrinks the op counts.
`go test -bench .` runs the same three cost shapes as Go benchmarks per arm
(BenchmarkRPush, BenchmarkLPop, BenchmarkInsert); note BenchmarkInsert drives the
full `insert` path including the flat-directory `locate` scan, so its number
carries the lab-02 seek term, while Sweep A's surgNs isolates the surgery alone.
`go test` runs the invariant and reference-band property tests.

## Redis 8.8 observed list encoding thresholds

Recorded against `~/bin/redis-server` v8.8.0 on a scratch port, so the inline-band
slice mirrors the real conversion and not a remembered one.

Under the shipped default `list-max-listpack-size -2` (an 8KiB listpack byte
budget, no entry-count bound):

| probe | result |
|---|---|
| 3 short entries | listpack |
| 122 x 64B entries (~8.1KiB listpack) | listpack |
| 123 x 64B entries (~8.2KiB listpack) | quicklist |
| single 8100B element | listpack |
| single 8192B element | quicklist |
| single 64B / 65B / 128B element | listpack |

Under a positive `list-max-listpack-size 128` (the classic entry-count cap):

| probe | result |
|---|---|
| 128 short entries | listpack |
| 129 short entries | quicklist |
| single 64B / 65B element | listpack |

The finding that matters for the slice: Redis 8.8's default conversion is
**byte-bounded at 8192 bytes of listpack**, not the "128 entries / 64 bytes" of
the old default.
The 128-entry cap is real only when `list-max-listpack-size` is set positive, and
there is no standalone 64-byte per-element rule under either setting (a 65B or
128B lone element stays listpack; the flip is the 8KiB budget or the positive
entry count).
So the inline-band slice pins its observable listpack-to-quicklist boundary to the
`list-max-listpack-size` knob with the -2 default meaning 8KiB (doc 4.4's table),
and honors a positive N as a 128-capped entry count, rather than hardcoding
128/64.
This is independent of the internal chunk geometry below, which is unobservable
(doc 4.4): a user tuning the Redis knob moves the compatibility boundary, not the
deque's slab size.

## Results

Apple M4 (darwin/arm64), go 1.26, 2026-07-11, one process.
ns columns are nanoseconds per op; ovhd/el is bytes of overhead per element beyond
the payload (resident slab bytes plus any value-log bytes, minus payload), read
directly against the 3-4B F14 bar.
The el/chunk and ovhd/el columns are structural and stable across runs; the ns
figures wander with cache and allocator noise, so the ordering and the level are
the signal, not the last digit.
The chunks column is for the memory-build size (200K elements at 64B, 60K at
1KiB); scale linearly for other cardinalities.

### Sweep A: chunk geometry

64B value band:

| arm | el/chunk | pushNs | popNs | surgNs | ovhd/el |
|---|---|---|---|---|---|
| 512/128 | 7.0 | 30 | 2.0 | 45 | 9.14 |
| 1024/128 | 15.0 | 19 | 1.7 | 50 | 4.27 |
| 2048/128 | 30.0 | 16 | 1.5 | 92 | 4.27 |
| 4096/128 | 61.0 | 14 | 2.0 | 195 | 3.15 |
| 8192/128 | 122.0 | 13 | 1.6 | 310 | 3.17 |
| 4096/64 | 61.0 | 13 | 2.0 | 190 | 3.15 |

1KiB value band (sub-budget arms cannot pack two values inline, so they are the
reference case in the real design, not a geometry point):

| arm | el/chunk | pushNs | popNs | surgNs | ovhd/el |
|---|---|---|---|---|---|
| 512/128 | must-ref | - | - | - | - |
| 1024/128 | must-ref | - | - | - | - |
| 2048/128 | must-ref | - | - | - | - |
| 4096/128 | 3.0 | 165 | 3.0 | 350 | 341.3 |
| 8192/128 | 7.0 | 175 | 2.0 | 290 | 146.4 |
| 4096/64 | 3.0 | 135 | 2.5 | 205 | 341.3 |

At 1M elements the 64B chunk counts are ~140K (512B), ~16.4K (4096B), ~8.2K
(8192B); the directory scaling arithmetic in doc 6.3 is priced against those.

### Sweep B: element-inline cap at 4096/128, inline versus 16B reference

| elem | el/chunk | inline ovhd/el | reference ovhd/el | verdict |
|---|---|---|---|---|
| 16 | 127.8 | 16.1 | 40.1 | inline |
| 64 | 61.0 | 3.2 | 40.1 | inline |
| 256 | 15.0 | 17.1 | 40.1 | inline |
| 512 | 7.0 | 73.2 | 40.1 | inline |
| 1024 | 3.0 | 341.4 | 40.1 | inline |
| 1536 | 2.0 | 512.0 | 40.1 | inline (last) |
| 2048 | 1.0 | 2048.0 | 40.1 | ref-wins |
| 3072 | 1.0 | 1024.0 | 40.1 | ref-wins |
| 4096 | - | does not fit | 40.1 | must-ref |
| 8192 | - | does not fit | 40.1 | must-ref |

### Sweep C: per-chunk count maintenance at 4096/128, 64B values

| path | ns/op |
|---|---|
| push with count maintenance | ~14 |
| frame write only | ~14 |
| maintenance + chunk-link share | within noise (< 1 ns) |

## Reading the sweeps

The 64B memory column is the gate.
The framing floor is 3 bytes per element (a 1-byte uvarint prefix plus a 2-byte
directory entry), and the 4096-byte budget lands at 3.15B, the 8-byte header and
sub-one-frame slab slack amortized to ~0.15B across 61 elements.
The 8192 budget matches it at 3.17B: doubling the slab does not lower the floor
because the floor is the per-element framing, not the per-chunk fixed cost, once
the chunk holds tens of elements.
Below 4KiB the memory bar misses: 2048 and 1024 sit at 4.27B (the header and slack
share is now large across 30 and 15 elements), and 512 is 9.14B, over the block
line before churn even starts.
So the byte budget must be at least 4KiB to clear the 3-4B F14 bar at 64B, and
4KiB is the smallest arm that does.

Edge ops do not care which of the passing budgets wins.
Push is O(1) amortized and gets slightly cheaper as the budget grows because a
fresh slab is linked less often (every 61 pushes at 4KiB against every 7 at 512B),
flattening around 13-14 ns at 4KiB and up.
Pop is O(1) at ~1.5-2 ns across every arm, a `firstLive` bump with no memmove,
which is the whole point of the design and holds at every geometry.

Interior surgery is where a bigger budget costs, and it points the other way.
The pure in-chunk memmove plus directory shift grows with the byte budget because
it moves more bytes per edit: at 64B it runs ~90 ns at 2KiB, ~195 ns at 4KiB, and
~310 ns at 8KiB, roughly doubling from 4KiB to 8KiB.
(The 512B and 1024B surgery numbers are noisy-low because a half-full chunk of a
tiny slab holds only a few frames; the reliable signal is the monotone climb from
2KiB up.)
This is the doc 2.3 tension made concrete: too small and the ring carries too many
chunks and misses the memory bar, too large and interior memmoves and cold faults
drag more bytes per touch.
4KiB is the knee: it clears the memory bar and holds the interior memmove to half
the 8KiB cost.

The element cap is nearly free to keep at 128.
At 64B values the byte budget binds first (61 elements fill a 4KiB blob well under
128), so 4096/64 and 4096/128 are identical on every column; the cap only binds
for values under ~32B, and holding it at 128 keeps the u16 directory at or under
256 bytes and every `count`, `bytesUsed`, and `dir[i]` honestly u16 at any legal
geometry, exactly the section 2.3 reasoning.
64 buys nothing and gives up headroom, so 128 stays.

The 1KiB band is the one place a single geometry strains.
A 4KiB chunk holds only 3 frames of 1KiB, so per-element overhead is slab slack:
341B per element, almost all of it the unfilled tail bounded by one 1KiB frame
(doc 8.2's "bounded by one max frame" made literal).
Doubling to 8KiB holds 7 frames and halves the overhead to 146B, exactly the
prediction section 8.2 pre-registers for this band.
This does not unseat 4KiB as the default, because the 1KiB band's overhead is
slab slack that F16 gates against the value-log story, not the framing bar, and
the memory pitch (less RAM than quicklist) is carried by the 64B and small-value
bands where the framing floor rules.
But it is the named fallback lever: if the 1M x 1KiB memory gate row (9.3) misses,
the per-band 8KiB CAP for the 1KiB-and-up bands is the pre-registered move, and
this sweep confirms it does the work.

## The element-inline cap

Sweep B places the reference crossover cleanly.
Inline overhead is flat and tiny while a chunk packs many frames (3.2B at 64B,
17B at 256B), then climbs steeply as the frame count per chunk falls: 73B at
512B (7 per chunk), 341B at 1KiB (3), 512B at 1536B (2).
The 16-byte reference form is flat at ~40B per element regardless of value size,
because the value bytes leave the chunk for the value log and the chunk keeps
packing ~200 references per slab.
The two curves cross where a chunk can no longer hold two inline frames: at 1536B
(two per chunk) inline still wins at 512B against the reference's 40B, and at
2048B (one per chunk) inline's 2048B slack loses to the reference outright.
So the element-inline cap is one inline frame per half-chunk: a value at or above
half the byte budget (2KiB at the 4KiB default) goes out-of-line as a value-log
reference, which guarantees every chunk packs at least two live frames and caps
the interior memmove and the cold-fault granule at real data instead of slack.
A value at or above the whole budget must be a reference; there is no inline path
for it.

This internal reference threshold is not the Redis parity surface.
Redis keeps elements packed in a listpack node up to the 8KiB node budget and
spills a lone oversized element to a plain node only at that budget (measured
above: the flip is at 8192 bytes).
aki's 2KiB internal reference threshold is tighter, but it is unobservable: the
OBJECT ENCODING boundary a client sees is the `list-max-listpack-size` knob (doc
4.4), which the inline-band slice mirrors independently, so tightening the
internal threshold for a smaller fault granule does not change any observable
behavior.

## Darwin caveat

These constants are measured on the darwin/arm64 box because the GamingPC gate box
is busy with another campaign.
The geometry decision rests on the structural ovhd/el and el/chunk columns, which
are deterministic and platform-independent, on the chunk-count arithmetic, and on
ns orderings wide enough to survive a platform change: the 64B memory bar miss
below 4KiB is structural, and the surgery climb from 4KiB to 8KiB is a ~2x gap.
The absolute push/pop/surgery ns get their Linux confirmation at the M3 gate run
on GamingPC before any list cell is scored, and the Redis conversion thresholds
above should be re-confirmed on that box's Redis 8.8 build in the same hour as the
gate run per F20.

## Verdict

Frozen for the chunked-deque slice:

- **Chunk byte budget: 4096 (4KiB).** It is the smallest budget that clears the
  3-4B F14 memory bar at the 64B band (3.15B against 2KiB's 4.27B), while holding
  the interior memmove to half the 8KiB cost (~195 ns against ~310 ns at 64B) and
  keeping push in the fast tier. 512B, 1KiB, and 2KiB are rejected: all three miss
  the 64B memory bar (9.14B, 4.27B, 4.27B), and the sub-4KiB ring carries 2-8x the
  chunks for a deeper directory. 8KiB is rejected as the default: it matches 4KiB
  on the memory floor but doubles the interior memmove and the cold-fault granule
  for no memory win at 64B.
- **Element cap: 128.** It costs nothing at the 64B band (the byte budget binds
  first, so 4096/64 and 4096/128 are identical) and it keeps the u16 directory at
  or under 256 bytes and every header field honestly u16 at any legal geometry, as
  doc 2.3 requires. 64 is rejected: no measured benefit, less headroom.
- **Element-inline cap: half the byte budget (2KiB at the 4KiB default).** A value
  at or above 2KiB stores a 16-byte value-log reference instead of inline bytes,
  which caps inline overhead at the ~40B reference cost instead of letting slab
  slack climb past 300B per element, and guarantees every chunk packs at least two
  live frames. This is subject to the global F5 value-separation threshold (the
  reference threshold is min(F5, half the budget)) and is independent of the
  observable `list-max-listpack-size` encoding boundary.

Fallback lever: **a per-band 8KiB budget for the 1KiB-and-up value bands.**
The single 4KiB geometry strains only there, where 3 frames per chunk leave 341B
of slab slack per element; 8KiB halves that to 146B. This is the doc 8.2
pre-registered move and this sweep confirms it works, so if the 1M x 1KiB memory
gate row (9.3) misses at 4KiB, the fix is a second budget for the large-value
bands, not a redesign. The default stays one geometry until a gated row forces the
split, per section 2.3.

What the slice should bake in: 4096-byte chunk slabs with a 128-element cap
(whichever binds first), a value-log reference for any frame at or above half the
budget subject to F5, the u16 header and directory format of section 2.2, a flat
per-chunk count directory whose maintenance is a sub-nanosecond integer add per
push (Sweep C), and the observable listpack-to-quicklist boundary driven by
`list-max-listpack-size` (default -2 meaning 8KiB byte budget, positive N a
128-capped entry count) rather than a hardcoded 128/64.

## What the chunked-deque slice must encode as tests

Surfaced by this lab's own property tests (`TestAgainstModel`, `TestReferenceBand`,
`TestElemCapBinds`):

- After every push, pop, and interior insert: `count` equals the sum of live chunk
  counts, and the flat directory agrees with the live counts at every chunk (the
  section 2.8 invariant 2).
- Directory offsets are strictly increasing across live frames within a chunk
  (invariant 3), and `firstLive` only advances within a chunk's lifetime and stays
  in range (invariant 4).
- An interior chunk never sits drained to empty; a head chunk that drains is
  recycled and `headChunk` advances (invariant 5, the pop-recycle path).
- Every external position maps to exactly one (chunk, ordinal) pair through the
  count directory (invariant 1), checked against a shadow model across all
  geometry arms so a budget-specific split or recycle bug surfaces.
- The element cap binds before the byte budget for tiny values: a chunk of 1-byte
  values seals at exactly 128, not at the ~1300 the byte budget alone would allow.
- The reference band packs many frames per chunk for oversized values and books
  their bytes to the value log, so a chunk of 2KiB values holds tens of references
  rather than one inline frame.
- Split correctness: an interior insert that overflows a chunk splits it, and the
  two halves carry counts summing to the pre-insert live count plus one, checked
  on the drain-to-empty path down to a valid empty structure.
