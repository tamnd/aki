---
title: "Persistence"
description: "How aki stores data in one file, protects it with a write-ahead log, and exchanges data with Redis dumps."
weight: 20
---

aki keeps everything in a single file and protects it the way SQLite does, with a write-ahead log and atomic commits.
There is no memory image to snapshot and no second durability scheme to bolt on.
This page covers the file layout, how a write reaches disk, the durability knobs, the append-only log, and how to move data in and out of real Redis.

## The single-file model

A running aki database is one file and two sidecars.

- `data.aki` is the database itself: the header, the pages, the freelist, and the committed data.
- `data.aki-wal` is the write-ahead log, where commits land before they are folded into the main file.
- `data.aki-shm` is the shared-memory index for the log, used to find pages quickly during reads and recovery.

To back up, copy the three files together while the server runs, or checkpoint first and copy the single `.aki` file once the log is empty.
This is the same shape SQLite uses.

Compare that with Redis, where durability means juggling two separate schemes.
You keep a point-in-time `dump.rdb` snapshot, an `appendonly.aof` command log, or both, and you reason about how they interact on restart.
aki has one file plus its log, and the log is always part of the database, not an optional add-on.

## The write-ahead log and group commit

A write does not just update a map in memory.

1. The command is parsed from the RESP wire protocol and dispatched to its handler.
2. The handler applies the change to the keyspace inside a transaction.
3. The change is written to the write-ahead log and the log is fsynced according to the durability policy.
4. The commit is acknowledged to the client.

Many concurrent writes can share a single fsync through group commit, so throughput stays high without giving up durability.
Dirty pages are written back to the main `.aki` file later, and the log is checkpointed into it once its contents are safely folded in.

If the process dies at any point, the next open replays the write-ahead log and recovers to the last committed transaction.
A half-written commit at the tail of the log is discarded, so you never see a partial write.

## Durability policy

The `appendfsync` directive controls how aggressively the log is flushed to disk.

- `always` fsyncs on every write. Safest, slowest, and it loses nothing on a crash.
- `everysec` fsyncs about once a second. This is the default, and a crash risks at most one second of writes.
- `no` lets the operating system decide when to flush. Fastest, and the window of risk is whatever the OS buffers.

You can change it at runtime.

```bash
redis-cli CONFIG SET appendfsync everysec
redis-cli CONFIG GET appendfsync
```

There is also `no-appendfsync-on-rewrite`, which lets you pause fsyncs during a log rewrite to keep latency steady while the rewrite runs.

## The append-only log

aki keeps an append-only command log alongside the page-level write-ahead log.
Two directives control how it is loaded and written.

`aof-load-truncated` defaults to `yes`, which means a half-written record at the tail is tolerated on load: aki reads up to the last good record and starts.
Set it to `no` to make aki refuse to start when the tail is truncated, which is the strict choice if you would rather fail loudly than start with a slightly short log.

```bash
redis-cli CONFIG SET aof-load-truncated no
```

`aof-timestamp-enabled` writes a `#TS:<ms>` annotation before records so you can see when each batch was logged.
This is an aki extension, and the annotations are skipped on replay, so they do not affect recovery.

```bash
redis-cli CONFIG SET aof-timestamp-enabled yes
```

## RDB interchange

aki can read and write the Redis dump format, which is how you move data between aki and a real Redis.

Import a Redis dump into a fresh `.aki` file.

```bash
aki import dump.rdb
```

Export an aki database back to a Redis dump.

```bash
aki dump --file data.aki
```

Load a dump on first open only, so an existing database is left alone on later starts.

```bash
aki server --dbfile data.aki --load-rdb dump.rdb
```

Use these to seed aki from an existing Redis, or to hand your data back to Redis if you need to.

## Snapshots and long reads

aki reads run against MVCC snapshots.
A long-running read, like a backup scan or a `SCAN` over a large keyspace, sees a consistent point-in-time view of the data.
It never blocks writers, and writers never make it see a half-applied change partway through.

For the full list of directives and their defaults, see the [configuration reference](/reference/configuration/).
