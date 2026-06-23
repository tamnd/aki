---
title: "Release notes"
description: "What changed in each aki release."
weight: 60
---

This page tracks what shipped in each release.
Releases are cut from a single version tag, so the binaries, Linux packages, container image, Homebrew cask, and Scoop manifest all carry the same version.

## v0.1.0

The first public release.

aki speaks Redis on the wire and keeps the whole dataset in one file the way SQLite does.
You point any Redis client at it and it answers like Redis, but underneath there is one `.aki` file, a write-ahead log sidecar, a buffer-pool pager, MVCC snapshots, and atomic crash-safe commits.

### What works

- RESP2 and RESP3 wire protocol, with `HELLO` to negotiate the version.
- The Redis data-type commands: strings, lists, sets, sorted sets, hashes, streams, bitmaps, HyperLogLog, and geo.
- Generic key commands: expiry, scan, rename, dump and restore, and the rest.
- Transactions with `MULTI`, `EXEC`, and `WATCH`.
- Pub/sub, including sharded channels.
- Scripting with `EVAL`, and functions with `FUNCTION` and `FCALL`.
- ACLs, authentication, and keyspace notifications.
- Single-node to replica replication.
- A single durable file with a write-ahead log and MVCC, so durability is the default and the dataset can exceed RAM.
- RDB import and export, which is how you move data to and from real Redis.

The server reports its version as `7.2.0-aki-0.1.0`, so clients that gate features on the server version treat it as Redis 7.2.

### What is out of scope

These parts of Redis are not implemented.
The commands and config directives are still accepted so clients see the surface, but they do not bring the subsystem with them.

- Redis Cluster. `CLUSTER` answers as a single-node cluster, with no multi-node sharding and no cluster bus.
- TLS transport. Terminate TLS with a proxy in front of aki.
- Loadable C modules.

See [Redis compatibility](/reference/redis-compatibility/) for the full honest map.

### Install

```sh
# Homebrew
brew install tamnd/tap/aki

# Scoop
scoop bucket add tamnd https://github.com/tamnd/scoop-bucket
scoop install aki

# Docker
docker pull ghcr.io/tamnd/aki:0.1.0
```

Linux deb, rpm, and apk packages, plus prebuilt binaries for Linux, macOS, Windows, and FreeBSD, are attached to the [GitHub release](https://github.com/tamnd/aki/releases/tag/v0.1.0).
