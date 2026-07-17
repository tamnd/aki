# 04 value region re-home

Models the on-disk cost of re-homing the store's per-shard scratch value log into
the single `.aki` value region, and finds the batch size that amortizes it.

The scratch log appends raw value bytes into a per-shard file: no framing, no
alignment, so `V` values of `S` bytes cost `V*S`. The `.aki` value region frames
each value (uvarint length, bytes, trailing CRC32C) and cuts each batch as a
4KiB-aligned `value_log` segment with a 64-byte header. So the re-home pays a
fixed per-value frame tax plus, per batch, a segment header and the padding that
rounds up to the next 4KiB.

The model imports `akifile` and frames with the real codec (`AppendValueFrame`,
`SegmentSpan`), so the byte counts are the format's, not a restatement.

## Run

```
go run .            # full sweep, value size x batch size
go run . -quick     # short sweep
go run . -bar 1.05  # tighten the amplification bar the threshold reports
go test ./...       # pin the arithmetic
```

## Verdict

The padding is what bites, and batching is what pays it off: the per-value share
of the 4KiB segment falls as `1/B`.

- A 16-byte value in its own segment costs a whole 4KiB, 256x amplification.
  Per-value segments are unusable, which is why the spill path stages values and
  cuts one segment per group, not per value (the `ValueLogWriter` accumulator).
- Batched, the padding amortizes to about 1 B/value at B=4096 and only the frame
  tax remains. The tax is 5 to 7 bytes (varint plus CRC), so it dominates only for
  the smallest values, which stay resident and rarely spill anyway.
- Large values are insensitive: a 4KiB value amplifies 2x unbatched and 1.00x by
  B=64; a 64KiB value is already 1.25x unbatched. The spill path pays the padding
  only where values are small, and there a batch is cheap to fill.

The practical read: a spill flush threshold in the low thousands of values (or,
equivalently, a payload target of a few segments) holds the value region within a
few percent of the raw bytes for every value size that actually spills. The frame
tax is the floor and is the torn-blob guard the record's bare pointer leans on, so
it is not a cost to remove.
