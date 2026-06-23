---
title: "aki"
description: "A Redis-compatible database in a single file. Point any Redis client at aki and it answers like Redis, but underneath there is one .aki file with a write-ahead log, MVCC snapshots, and crash-safe commits, the way SQLite does."
heroTitle: "Redis on the wire, SQLite on disk"
heroLead: "aki is a single pure-Go binary that speaks the Redis protocol and stores everything in one file. Point redis-cli at it and it answers byte for byte like Redis, but the data lives in an .aki file with a buffer-pool pager, a write-ahead log, MVCC snapshots, and atomic crash-safe commits. No daemon to babysit, no separate RDB to schedule, no data lost on a hard kill."
heroPrimaryURL: "/getting-started/quick-start/"
heroPrimaryText: "Get started"
---

Redis is fast and its command set is a joy to use, but it keeps everything in RAM and treats the disk as a backup you have to configure.
SQLite is durable and trivial to operate, but it speaks SQL and a client library, not a wire protocol.
aki takes the half of each that you want: the Redis command surface and the RESP2/RESP3 wire protocol on top of a single-file, write-ahead-logged, MVCC storage engine.

```bash
aki server --dbfile data.aki            # start the server on one file
redis-cli set greeting "hello"          # any Redis client just works
redis-cli get greeting
redis-cli -3 hgetall config             # RESP3 maps, push, the lot
```

**Redis is the API. SQLite is the file.** You get Redis compatibility and Redis latency with SQLite durability, SQLite operational simplicity, and datasets that can grow past the size of RAM.

## Why aki

- **One file, like SQLite.** State is a single `.aki` file plus two sidecars (`.aki-wal` and `.aki-shm`). Copy it, back it up, ship it, open it read-only. There is no `dump.rdb` and no `appendonly.aof` to reconcile.
- **Crash safe by default.** Every write goes through a write-ahead log with group commit. Pull the power and aki recovers to the last committed transaction on the next open. Durability is the default, not a tuning exercise.
- **Larger than RAM.** A buffer-pool pager keeps hot pages in memory and the rest on disk, so the working set is what has to fit, not the whole dataset.
- **Real Redis compatibility.** RESP2 and RESP3, around 345 commands across strings, lists, sets, hashes, sorted sets, streams, bitmaps, HyperLogLog, and geo, plus transactions, pub/sub, Lua scripting, functions, ACLs, keyspace notifications, and replication.
- **Pure Go, zero dependencies.** No cgo, no external libraries, one static binary. It builds and runs the same on Linux, macOS, and Windows.
- **Speaks RDB.** Import an existing `dump.rdb` into an `.aki` file and export back out, so you can move data in and out of real Redis.

## What you can do with it

- **Drop it in for Redis.** Use it as the cache, queue, or data store behind an app that already speaks Redis, and get a durable single file instead of a memory image you have to snapshot.
- **Embed a Redis-shaped store.** Ship one binary and one file with your application, the way you would ship SQLite, and talk to it with any Redis client.
- **Move data between engines.** Convert a Redis `dump.rdb` to an `.aki` file and back with `aki import` and `aki dump`.
- **Inspect and verify on disk.** Open an `.aki` file with `aki check` to validate its structure, or dump its contents without a running server.

## Where to go next

- New here? Read the [introduction](/getting-started/introduction/) for the model aki is built on, then run the [quick start](/getting-started/quick-start/).
- Want to install it? See [installation](/getting-started/installation/).
- Looking for a specific topic? The [guides](/guides/) cover the data types, persistence and crash safety, transactions, pub/sub, scripting, replication, and security.
- Need a specific command or flag? The [command reference](/reference/commands/) and the [CLI reference](/reference/cli/) are the full surface.
