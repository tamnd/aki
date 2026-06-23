---
title: "Redis compatibility"
description: "An honest map of what aki matches in Redis and what is out of scope."
weight: 40
---

aki speaks Redis on the wire and stores everything in a single file.
You point any Redis client at it and it answers like Redis.
This page is the honest map: what matches, what differs, and what is out of scope.

The guiding rule is simple.
Where the aki spec and real Redis ever disagree, aki follows real Redis.
Wire compatibility with existing clients is the goal, so the client's expectation wins.

## Protocol

aki speaks both RESP2 and RESP3.
`HELLO` negotiates the version, and a connection can switch to RESP3 to get maps, sets, and the other typed replies.

The server reports its version as `7.2.0-aki-<version>`, so it presents as Redis 7.2.
Clients that gate features on the reported version treat aki as a 7.2 server.

## What matches Redis

These work like Redis, so existing code and tooling behave as expected.

| Area | Status |
| --- | --- |
| Data-type commands (strings, lists, sets, sorted sets, hashes, streams, bitmaps, HyperLogLog, geo) | Supported |
| Generic key commands (expiry, scan, rename, dump/restore, and so on) | Supported |
| Transactions (`MULTI`, `EXEC`, `WATCH`) | Supported |
| Pub/sub, including sharded channels | Supported |
| Scripting (`EVAL`) and functions (`FUNCTION`, `FCALL`) | Supported |
| ACLs and authentication | Supported |
| Keyspace notifications | Supported |
| Replication (single node to replica) | Supported |

The full command list is in the [commands reference](/reference/commands/).

## The big difference: storage

This is where aki is genuinely different from Redis.

Redis is in-memory with optional snapshots.
You hold the dataset in RAM and bolt on durability with an RDB snapshot, an append-only file, or both.

aki is a single durable file with a write-ahead log and MVCC.
Durability is the default, not an add-on, and the dataset can exceed RAM because aki reads pages from the file as needed.
There is no separate `dump.rdb` or `appendonly.aof` to manage on disk.
aki can still import and export the RDB format, which is how you move data to and from real Redis (see the [persistence guide](/guides/persistence/)).

## Out of scope

These parts of Redis are not implemented.
The relevant commands and config directives are still accepted so clients see the surface, but they do not bring the subsystem with them.

| Area | What aki does |
| --- | --- |
| Redis Cluster | No multi-node sharding and no cluster bus. `CLUSTER` answers as a single-node cluster. |
| TLS transport | No TLS listener. Terminate TLS with a proxy in front of aki. |
| Modules | No loadable C modules. |
| Subsystem config knobs | Directives for things aki has no subsystem for are accepted but inert. |

For exactly which directives are accepted but inert and why, see the [configuration reference](/reference/configuration/).
