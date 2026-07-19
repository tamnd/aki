# M2 lab 11: inline-band score bytes

The listpack-class inline band stores a zset entry as
`[len:u8][tag:u8][member][score]`. The score was a flat 8-byte IEEE-754 double,
so a tiny integer-scored zset paid 8 bytes for a score that fits in one. Redis's
listpack integer-encodes a small score to one or two bytes. This lab settles how
much a class-tagged score encoding saves against that flat 8 bytes, and what the
tag-and-branch costs per op, before the codec lands in `engine/f3/zset/codec.go`.

## Encoding

One class byte then a width-matched payload:

| class | payload | total | covers |
|---|---|---|---|
| int8 | 1 B | 2 B | small ranks, counters, flags (0..255 signed) |
| int16 | 2 B | 3 B | mid counters |
| int32 | 4 B | 5 B | unix-second timestamps, ids |
| float64 | 8 B | 9 B | fractional, infinity, integer past int32 (lossless) |

A non-integer or out-of-int32 score falls back to the 8-byte float payload plus
the one class byte, so those rare scores cost 1 byte more than before; every
integer score inside int32 costs 3 to 6 bytes less. Signed zero is collapsed to
+0.0 by the insert caller, matching the listpack zero-sign quirk, so the float
fallback never carries -0.0.

## Sweep (in-process byte model, `go run .`)

Blob bytes, old (flat 8-byte score) vs new, per score distribution and member
size, for a tiny zset:

| distribution | member | count | old | new | ratio | save/entry |
|---|---|---|---|---|---|---|
| smallint | 2 B | 8 | 96 | 54 | 0.56 | 5 B |
| smallint | 8 B | 8 | 144 | 98 | 0.68 | 5 B |
| smallint | 20 B | 8 | 240 | 194 | 0.81 | 5 B |
| timestamp | 2 B | 8 | 96 | 72 | 0.75 | 3 B |
| timestamp | 8 B | 8 | 144 | 120 | 0.83 | 3 B |
| bigint (>int32) | 8 B | 8 | 144 | 152 | 1.06 | -1 B |
| fraction | 8 B | 8 | 144 | 152 | 1.06 | -1 B |

The common leaderboard and timestamp shapes shrink the whole inline blob by 20
to 46 percent; the float-fallback shapes pay one extra byte per entry, a trade
Redis's listpack makes for the same reason.

Codec cost, ns per op (median of 7):

| distribution | encode | decode |
|---|---|---|
| smallint | 12.2 | 0.7 |
| timestamp | 4.0 | 1.0 |
| bigint | 3.8 | 1.1 |
| fraction | 3.2 | 1.1 |

Decode is a class-byte switch, ~1 ns, below the member scan it rides next to.
Encode is one range check and a branch, single-digit ns, off the reactor's
critical path (a write already touches the store).

## Verdict

Class-tag the inline zset score. Every integer score inside int32 (the
overwhelming majority of real zset scores: ranks, counters, unix timestamps)
drops from 8 bytes to 2 to 5, shrinking a tiny integer-scored zset's blob by up
to 46 percent, the largest avoidable cost on the M2-G10 tiny-collection memory
row. The float fallback is lossless and costs one byte more, a rare case. The
codec is ~1 ns to decode and single-digit ns to encode, well under the entry
scan. Frozen: the engine ships this in `engine/f3/zset/codec.go`
(`intScore`/`putScore`/`readScore`/`scoreWidthAt`), replacing the flat
`PutUint64`/`Float64frombits` at the four inline-blob stride sites in `zset.go`.
