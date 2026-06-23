---
title: "Introduction"
description: "What aki is, the Redis-API-on-a-SQLite-file model it is built on, and where it fits next to Redis and SQLite."
weight: 10
---

aki is a database that speaks Redis on the wire and stores everything in a single file.
The name is 赤, "red" in Japanese, a nod to the colour Redis made famous.
You point any Redis client at it and it answers exactly like Redis, but underneath there is no memory image waiting to be snapshotted.
There is one `.aki` file with a paged storage engine, a write-ahead log, MVCC snapshots, and atomic commits.

The one-line version: Redis is the API, SQLite is the file.

## The model

Redis and SQLite each solved half of a problem.

Redis gave us a wonderful command set and a simple wire protocol, and it is fast because it keeps everything in RAM.
The cost is that durability is something you bolt on.
You pick between RDB snapshots and an append-only file, you tune fsync policies, and a hard crash can still cost you the writes since the last save.

SQLite gave us durability and operational calm.
The whole database is one file with a write-ahead log, commits are atomic and crash safe, and there is no server to run.
The cost is that it speaks SQL through a library, not a protocol a cache client can use.

aki puts the Redis personality on top of the SQLite-style engine.
You get the Redis command surface and the RESP2/RESP3 protocol, and you get one file, a write-ahead log, MVCC, and crash-safe commits underneath.

## How a write travels

When a client sends `SET key value`, aki does not just update a map in memory.

1. The command is parsed from the RESP wire protocol and dispatched to its handler.
2. The handler applies the change to the keyspace inside a transaction.
3. The change is written to the write-ahead log and the log is fsynced according to the durability policy.
4. The commit is acknowledged to the client.
5. Dirty pages are written back to the main `.aki` file later, and the log is checkpointed into it.

If the process dies at any point, the next open replays the write-ahead log and recovers to the last committed transaction.
Readers run against MVCC snapshots, so a long read never blocks a writer and never sees a half-applied change.

## The files

A running aki database is one file and two sidecars:

- `data.aki` is the database: the file header, the pages, the freelist, and the committed data.
- `data.aki-wal` is the write-ahead log, where commits land before they are folded into the main file.
- `data.aki-shm` is the shared-memory index for the log, used to find pages quickly during reads and recovery.

Back up the database by copying the three files together, or by checkpointing and copying the single `.aki` file.
This is the same shape SQLite uses, for the same reasons.

## Where aki fits

Reach for aki when you want Redis semantics but you do not want to operate a Redis the careful way: snapshots, append-only files, replicas just for durability, and the memory bill for keeping everything resident.

- It is a drop-in for an app that already speaks Redis and wants a durable single file instead of a memory image.
- It is an embeddable Redis-shaped store you can ship next to your application the way you ship SQLite.
- It holds datasets larger than RAM, because only the working set has to fit in the buffer pool.

It is not a distributed system.
aki is a single node with optional replication, the way SQLite is a single file.
If you need sharding across many machines and a cluster bus, that is Redis Cluster territory, not aki.

## Compatibility, honestly

aki aims for byte-for-byte wire compatibility with Redis, and where the two ever disagree, aki follows real Redis behavior so existing clients keep working.
A handful of Redis config knobs name subsystems aki does not have, such as cluster, TLS transport, or manual memory defragmentation that the Go runtime handles on its own.
Those knobs are accepted so tooling sees the full surface, but they do not change behavior.
The [Redis compatibility](/reference/redis-compatibility/) page is the honest map of what matches and what does not.

Next: [install aki](/getting-started/installation/), then take the [quick start](/getting-started/quick-start/).
