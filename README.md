# aki

A modern, high-performance, low-latency database that speaks Redis on the wire and stores everything in a single file, the way SQLite does. `aki` (赤, "red" in Japanese, a nod to the colour Redis made famous) is to an in-memory key/value server what SQLite is to a client/server SQL database: you point any Redis client at it and it answers byte-for-byte like Redis, but underneath there is one `.aki` file, an optional write-ahead log sidecar, a buffer-pool pager, MVCC snapshots, and atomic crash-safe commits.

**Redis is the API; SQLite is the file.** `aki` implements the Redis command surface and the RESP2/RESP3 wire protocol on top of a single-file, write-ahead-logged, MVCC paged storage engine, so you get Redis compatibility and Redis latency with SQLite durability, SQLite operational simplicity, and larger-than-RAM datasets.

The design is captured in a detailed multi-document specification, and implementation notes track what has actually been built milestone by milestone.

## Status

Early development. The storage substrate (the virtual-filesystem seam, CRC-32C checksums, the varint/record encoding, the single-file format, the pager and buffer pool, the write-ahead log, group commit, and crash recovery) is the M0 milestone and lands first; the Redis personality (RESP server, command dispatch, data types) follows on top of it.

## Layout

- `vfs` — the virtual-filesystem seam: open/read/write/sync/truncate over a named file, with an in-memory and a fault-injecting backend for crash testing.
- `checksum` — CRC-32C (Castagnoli) used by the file header, page headers, and WAL frames.
- `encoding` — varint (LEB128), zigzag, and fixed-width little-endian integer codecs.
- `format` — the on-disk `.aki` format: the 16-byte magic, the file header, page types, the slotted page layout, and the double-buffered meta pages.
- `pager` — the pager and buffer pool: page allocation, the freelist, pin/unpin, dirty write-back under WAL discipline.
- `wal` — the write-ahead log, group commit, the fsync model, checkpointing, and crash recovery.
- `cmd/aki` — the `aki` binary (`server`, `cli`, `check`, `dump`, `import`, `bench`).

## Build

```
make build      # build bin/aki
make test       # go test ./...
make race       # go test -race ./...
```

Pure Go, no cgo. The file extension is `.aki`; sidecars are `.aki-wal` and `.aki-shm`.

## License

BSD-3-Clause. `aki` is a clean-room reimplementation of the Redis wire protocol and semantics; it is not derived from Redis source.
