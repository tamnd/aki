# aki

A modern, high-performance, low-latency database that speaks Redis on the wire and stores everything in a single file, the way SQLite does. `aki` (赤, "red" in Japanese, a nod to the colour Redis made famous) is to an in-memory key/value server what SQLite is to a client/server SQL database: you point any Redis client at it and it answers byte-for-byte like Redis, but underneath there is one `.aki` file, an optional write-ahead log sidecar, a buffer-pool pager, MVCC snapshots, and atomic crash-safe commits.

**Redis is the API; SQLite is the file.** `aki` implements the Redis command surface and the RESP2/RESP3 wire protocol on top of a single-file, write-ahead-logged, MVCC paged storage engine, so you get Redis compatibility and Redis latency with SQLite durability, SQLite operational simplicity, and larger-than-RAM datasets.

The design is captured in a detailed multi-document specification, and implementation notes track what has actually been built milestone by milestone.

## Status

Early development. The tree holds the f1 engine (`engine/f1raw`, `f1srv`), which keeps serving as the reference, and the f2 spike (`engine/f2raw`, `f2srv`). The f3 rebuild lands next under `engine/f3` and `f3srv`; the pre-f1 btree/pager storage tree has been removed.

## Layout

- `engine/f1raw` — the f1 in-memory engine: bucket index, arenas, cold tiering.
- `f1srv` — the RESP server on top of `engine/f1raw`.
- `cmd/f1srv` — the `f1srv` binary, the serving reference until the f3 engine replaces it.
- `engine/f2raw`, `f2srv`, `cmd/f2srv` — the f2 point-store spike, kept until the f3 M0 gate posts.
- `labs` — standalone microbenchmarks that settle design constants.
- `bench` — benchmark helpers.

## Build

```
make build      # build bin/f1srv
make test       # go test ./...
make race       # go test -race ./...
```

Pure Go, no cgo. The file extension is `.aki`; sidecars are `.aki-wal` and `.aki-shm`.

## License

BSD-3-Clause. `aki` is a clean-room reimplementation of the Redis wire protocol and semantics; it is not derived from Redis source.
