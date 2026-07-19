# M1 lab 13: SMEMBERS reply-framing copy elision

## Question

The SMEMBERS streaming encoder frames one member at a time and pumps the reply
through the shard's bounded chunk ring.
The original encoder framed each member's bulk into a scratch buffer and then
copied that scratch into the wire chunk: a second copy of every member's bytes on
a reply whose whole cost is byte movement.
On the 10k-member gate cell SMEMBERS lands at 1.97x redis, a bandwidth-bound
near-miss, so a redundant per-member copy is exactly the kind of waste that keeps
it under 2x.

## Change

When the whole bulk frame fits in the space left in the current wire chunk (the
common case for a 64B member against a kilobyte-plus chunk) frame it straight onto
the wire and skip the scratch copy.
Only a member that crosses the chunk boundary falls to the scratch-and-resume
path, which is what the scratch buffer is actually for.
The fit check guarantees the chunk's capacity, so `slices.Grow` inside
`AppendBulk` reuses the chunk's backing and writes in place.

## Method

In-process, no server, no wire, no engine import.
The two `Next` shapes are reproduced verbatim from `engine/f3/set/smembers.go`
over a synthetic member slab, and the resp framing is inlined so the lab is
standalone.
The wire-drain harness pumps a whole reply through a single reused chunk and
discards each filled chunk, the way the real pump sends and reuses the wire
buffer, so the framing cost is isolated from any reassembly allocation.

## Result

Two things are measured: copied member-bytes per member (deterministic) and
framing time (noisy on a loaded laptop, firm only on the box).

Copied bytes per member, card 10000 (the metric the elision directly changes):

| member size | chunk | scratch copied/member | direct copied/member |
|---|---|---|---|
| 8 | 4096 | 14.0 | 0.0 |
| 16 | 4096 | 23.0 | 0.1 |
| 64 | 4096 | 71.0 | 1.2 |
| 256 | 4096 | 264.0 | 16.5 |

The direct path copies essentially none of the member payload: only the rare
member that straddles a chunk boundary is copied, so the second copy of every
in-chunk member is gone.
For the 64B gate member that is 71 bytes of copy removed per member, ~700 KiB per
10k-member SMEMBERS reply.

Framing time (`main.go` sweep, ns/member) favours the direct path in almost every
cell, 6% to 33% for the representative 64B and 256B members, with one noisy 64B
16384-chunk cell that inverts on laptop scheduler noise while its copied bytes
still drop to 0.3.
`go test -bench` on the wire drain has the direct best case 30% ahead of scratch
best but with high run-to-run variance under laptop load.

## Verdict

The elision is byte-identical (`TestFramingIdentical` over 8 sizes x 6 chunk
sizes, including boundary-straddling sizes) and removes the second copy of every
non-straddling member, a deterministic reduction in reply-path byte traffic.
Landing it is safe on that ground alone.
Whether it flips the 10k SMEMBERS gate cell from 1.97x to >=2x is a box question:
the loopback-bandwidth ceiling and the copy saved are both real, so the flip is
plausible but must be confirmed by re-running the M1 range-read gate on the
GamingPC, not asserted from this laptop.
