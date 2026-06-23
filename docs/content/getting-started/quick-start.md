---
title: "Quick start"
description: "Start the aki server, connect with redis-cli, write some keys, and watch them survive a restart."
weight: 30
---

This walks you from an empty directory to a durable Redis-compatible store in a few commands.
You need the `aki` binary from the [installation](/getting-started/installation/) page and any Redis client.
The examples use `redis-cli`, which ships with Redis.

## Start the server

```bash
aki server --dbfile data.aki
```

aki creates `data.aki` (plus the `data.aki-wal` and `data.aki-shm` sidecars) and listens on `127.0.0.1:6379`, the default Redis port.
Leave it running and open a second terminal.

To listen somewhere else, pass `--addr`:

```bash
aki server --dbfile data.aki --addr 0.0.0.0:6379
```

## Connect and run commands

```bash
redis-cli
```

```
127.0.0.1:6379> set greeting "hello from aki"
OK
127.0.0.1:6379> get greeting
"hello from aki"
127.0.0.1:6379> rpush tasks build test ship
(integer) 3
127.0.0.1:6379> lrange tasks 0 -1
1) "build"
2) "test"
3) "ship"
127.0.0.1:6379> hset user:1 name ada role admin
(integer) 2
127.0.0.1:6379> hgetall user:1
1) "name"
2) "ada"
3) "role"
4) "admin"
```

Everything you know from Redis works the same way.
Strings, lists, sets, hashes, sorted sets, streams, expirations, and the rest all behave as they do on a real Redis server.

## Use RESP3

aki speaks both RESP2 and RESP3.
Ask for RESP3 with the `-3` flag and you get the richer reply types, such as maps for `HGETALL`:

```bash
redis-cli -3 hgetall user:1
```

```
1# "name" => "ada"
2# "role" => "admin"
```

You can also switch a connection at runtime with `HELLO 3`.

## Prove it is durable

This is the part Redis makes you work for and aki does by default.
Write a key, stop the server with a hard kill, start it again, and the key is still there.

```bash
redis-cli set survives yes
# stop the server with Ctrl-C, or even kill -9 its PID
aki server --dbfile data.aki
redis-cli get survives
```

```
"yes"
```

The write was committed to the write-ahead log and fsynced before the client got its `OK`, so a crash cannot lose it.
On the next open aki replayed the log and recovered to the last committed transaction.

## Bring data in from Redis

If you have an existing Redis dump, import it when you first create the file:

```bash
aki server --dbfile data.aki --load-rdb /path/to/dump.rdb
```

The import only runs when `data.aki` does not exist yet, so it never clobbers a database you already have.
You can also convert between formats offline with `aki import` and `aki dump`, covered in the [CLI reference](/reference/cli/).

## Inspect the file

Without a running server, you can validate the on-disk structure:

```bash
aki check data.aki
```

## Where to go next

- The [guides](/guides/) go deep on the [data types](/guides/data-types/), [persistence and crash safety](/guides/persistence/), [transactions](/guides/transactions/), [pub/sub](/guides/pub-sub/), [scripting](/guides/scripting/), and [replication](/guides/replication/).
- The [command reference](/reference/commands/) lists the full command surface by group.
- The [configuration reference](/reference/configuration/) explains which config directives aki honors and which it accepts for compatibility.
