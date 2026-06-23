---
title: "Data types"
description: "A tour of the data types aki supports, with short command examples for each."
weight: 10
---

aki speaks Redis on the wire, so the data types are the Redis data types.
Strings, bitmaps, lists, sets, sorted sets, hashes, streams, HyperLogLog, and geo all work the way your Redis client expects.
This page walks through each one with a small example you can paste into `redis-cli`.

Every command runs through the same paged engine and write-ahead log, so a write here is durable the moment it returns, not the next time something snapshots memory.

## Strings

Strings are the basic type: a key holds a sequence of bytes.
`SET` and `GET` take options like `EX`/`PX` for a TTL, `NX`/`XX` to set only if the key is missing or present, and `GET` to return the old value while setting the new one.

```bash
redis-cli SET greeting "hello" EX 60 NX
redis-cli GET greeting
redis-cli SET greeting "hi" GET
```

Strings that hold numbers can be incremented and decremented atomically.

```bash
redis-cli SET counter 10
redis-cli INCR counter
redis-cli INCRBY counter 5
redis-cli DECR counter
redis-cli INCRBYFLOAT counter 0.5
```

You can append to a string, read or write a substring by offset, and batch multiple keys at once.

```bash
redis-cli APPEND greeting " there"
redis-cli GETRANGE greeting 0 4
redis-cli SETRANGE greeting 0 "HI"
redis-cli MSET a 1 b 2 c 3
redis-cli MGET a b c
```

`LCS` finds the longest common subsequence of two string values, with `LEN` and `IDX` for the length and the matched ranges.

```bash
redis-cli SET key1 "ohmytext"
redis-cli SET key2 "mynewtext"
redis-cli LCS key1 key2
redis-cli LCS key1 key2 LEN
```

## Bitmaps

A bitmap is a string treated as a bit array.
You set and read individual bits, count the set bits, find the first 0 or 1, and combine bitmaps with boolean operations.

```bash
redis-cli SETBIT visits 7 1
redis-cli GETBIT visits 7
redis-cli BITCOUNT visits
redis-cli BITPOS visits 1
redis-cli BITOP AND dest visits other
```

`BITFIELD` packs several integers of arbitrary width into one string and operates on them in a single call.

```bash
redis-cli BITFIELD mykey SET u8 0 255 GET u8 0 INCRBY u8 0 10
```

## Lists

Lists are ordered sequences you push and pop from either end.
`LRANGE` reads a window, `LMOVE` shifts an element from one list to another, and `LMPOP` pops from the first non-empty list in a set of keys.

```bash
redis-cli RPUSH tasks a b c
redis-cli LPUSH tasks start
redis-cli LRANGE tasks 0 -1
redis-cli LPOP tasks
redis-cli RPOP tasks
redis-cli LMOVE tasks done LEFT RIGHT
redis-cli LMPOP 2 tasks done LEFT
```

The blocking variants wait for an element to arrive instead of returning nil on an empty list, which is how you build a simple queue.

```bash
redis-cli BLPOP tasks 5
redis-cli BRPOP tasks 5
redis-cli BLMOVE tasks done LEFT RIGHT 5
```

## Sets

A set is an unordered collection of unique members.
You add members, list them, test membership, and compute intersections, unions, and differences across sets.

```bash
redis-cli SADD tags go redis db
redis-cli SMEMBERS tags
redis-cli SISMEMBER tags go
redis-cli SADD other go rust
redis-cli SINTER tags other
redis-cli SUNION tags other
redis-cli SDIFF tags other
redis-cli SSCAN tags 0
```

## Sorted sets

A sorted set keeps unique members ordered by a floating-point score.
You can range by rank, by score, or by lexical order, look up a member's rank, bump a score, and pop the lowest or highest scoring member.

```bash
redis-cli ZADD board 100 alice 200 bob 150 carol
redis-cli ZRANGE board 0 -1 WITHSCORES
redis-cli ZRANGEBYSCORE board 120 250
redis-cli ZRANGEBYLEX board "[a" "[c"
redis-cli ZRANK board bob
redis-cli ZINCRBY board 25 alice
redis-cli ZPOPMIN board
redis-cli ZPOPMAX board
```

`ZADD` takes `GT`, `LT`, `NX`, and `XX` so you can update a score only when the new one is greater, lesser, or only when the member is new or already present.

```bash
redis-cli ZADD board GT 250 alice
redis-cli ZADD board NX 50 dave
```

## Hashes

A hash maps fields to values inside one key, like a small record.
You set and get fields, fetch the whole hash, delete fields, increment numeric fields, and scan large hashes incrementally.

```bash
redis-cli HSET user:1 name alice age 30
redis-cli HGET user:1 name
redis-cli HGETALL user:1
redis-cli HINCRBY user:1 age 1
redis-cli HDEL user:1 age
redis-cli HSCAN user:1 0
```

Fields can carry their own TTL.
`HEXPIRE` sets a per-field expiry, `HTTL` reads it, and `HPERSIST` removes it, so one field can expire while the rest of the hash stays.

```bash
redis-cli HEXPIRE user:1 60 FIELDS 1 name
redis-cli HTTL user:1 FIELDS 1 name
redis-cli HPERSIST user:1 FIELDS 1 name
```

## Streams

A stream is an append-only log of entries, each with an ID and a set of field-value pairs.
`XADD` appends, `XRANGE` and `XLEN` read, and `XREAD` follows the stream from a given ID.

```bash
redis-cli XADD events '*' type login user alice
redis-cli XLEN events
redis-cli XRANGE events - +
redis-cli XREAD COUNT 10 STREAMS events 0
```

Consumer groups let several workers split the entries and track what has been processed.
`XGROUP` creates a group, `XREADGROUP` hands out entries, `XACK` marks them done, and `XAUTOCLAIM` reassigns entries from a worker that went away.

```bash
redis-cli XGROUP CREATE events workers 0
redis-cli XREADGROUP GROUP workers w1 COUNT 5 STREAMS events '>'
redis-cli XACK events workers 1700000000000-0
redis-cli XAUTOCLAIM events workers w2 60000 0
```

## HyperLogLog

A HyperLogLog estimates how many distinct items it has seen using a tiny, fixed amount of space.
You add items, count the approximate cardinality, and merge several into one.
The trade is a small error rate in exchange for not storing every member.

```bash
redis-cli PFADD visitors alice bob carol
redis-cli PFCOUNT visitors
redis-cli PFADD visitors2 carol dave
redis-cli PFMERGE total visitors visitors2
redis-cli PFCOUNT total
```

## Geo

Geo commands store longitude and latitude points in a sorted set and query them by distance.
`GEOADD` stores points, `GEOSEARCH` finds points within a radius or box, and the rest read back distance, position, and geohash.

```bash
redis-cli GEOADD places -122.27 37.80 oakland -122.42 37.77 sf
redis-cli GEOSEARCH places FROMMEMBER sf BYRADIUS 50 km ASC
redis-cli GEODIST places sf oakland km
redis-cli GEOPOS places sf
redis-cli GEOHASH places sf
```

## Generic key commands

These work across every type.
You delete keys, test existence, set and read TTLs, ask a key's type, list or scan keys, rename, copy, serialize, and sort.

```bash
redis-cli DEL greeting
redis-cli EXISTS user:1
redis-cli EXPIRE user:1 120
redis-cli TTL user:1
redis-cli PERSIST user:1
redis-cli TYPE board
redis-cli SCAN 0 MATCH 'user:*' COUNT 100
redis-cli RENAME user:1 user:one
redis-cli COPY user:one user:two
redis-cli SORT tasks ALPHA
```

`DUMP` and `RESTORE` serialize a single key to and from the Redis dump format, which is handy for moving one key between servers.

```bash
redis-cli DUMP user:one
redis-cli RESTORE user:copy 0 "<serialized bytes>"
```

For the exact arguments, return values, and edge cases of every command, see the full [command reference](/reference/commands/).
