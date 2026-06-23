---
title: "Commands"
description: "The full command surface aki supports, grouped by type."
weight: 20
---

aki implements around 345 commands, counting subcommands.
They behave like their Redis counterparts, so existing clients work without changes.

The surface is introspectable at runtime.
`COMMAND` lists every command, `COMMAND COUNT` returns the total, `COMMAND INFO <name>` returns the arity and flags, and `COMMAND DOCS <name>` returns the documented signature.
Those four read the live surface, so they always match what the running build actually serves.

## Strings

`APPEND` `DECR` `DECRBY` `GET` `GETDEL` `GETEX` `GETRANGE` `GETSET` `INCR` `INCRBY` `INCRBYFLOAT` `LCS` `MGET` `MSET` `MSETNX` `PSETEX` `SET` `SETEX` `SETNX` `SETRANGE` `STRLEN` `SUBSTR`

## Bitmaps

`BITCOUNT` `BITFIELD` `BITOP` `BITPOS` `GETBIT` `SETBIT`

## Lists

`BLMOVE` `BLMPOP` `BLPOP` `BRPOP` `BRPOPLPUSH` `LINDEX` `LINSERT` `LLEN` `LMOVE` `LMPOP` `LPOP` `LPOS` `LPUSH` `LPUSHX` `LRANGE` `LREM` `LSET` `LTRIM` `RPOP` `RPOPLPUSH` `RPUSH` `RPUSHX`

## Sets

`SADD` `SCARD` `SDIFF` `SDIFFSTORE` `SINTER` `SINTERCARD` `SINTERSTORE` `SISMEMBER` `SMEMBERS` `SMISMEMBER` `SMOVE` `SPOP` `SRANDMEMBER` `SREM` `SSCAN` `SUNION` `SUNIONSTORE`

## Sorted sets

`BZMPOP` `BZPOPMAX` `BZPOPMIN` `ZADD` `ZCARD` `ZCOUNT` `ZDIFF` `ZDIFFSTORE` `ZINCRBY` `ZINTER` `ZINTERCARD` `ZINTERSTORE` `ZLEXCOUNT` `ZMPOP` `ZMSCORE` `ZPOPMAX` `ZPOPMIN` `ZRANDMEMBER` `ZRANGE` `ZRANGEBYLEX` `ZRANGEBYSCORE` `ZRANGESTORE` `ZRANK` `ZREM` `ZREMRANGEBYLEX` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZREVRANGE` `ZREVRANGEBYLEX` `ZREVRANGEBYSCORE` `ZREVRANK` `ZSCAN` `ZSCORE` `ZUNION` `ZUNIONSTORE`

## Hashes

`HDEL` `HEXISTS` `HEXPIRE` `HEXPIREAT` `HEXPIRETIME` `HGET` `HGETALL` `HGETDEL` `HGETEX` `HINCRBY` `HINCRBYFLOAT` `HKEYS` `HLEN` `HMGET` `HMSET` `HPERSIST` `HPEXPIRE` `HPEXPIREAT` `HPEXPIRETIME` `HPTTL` `HRANDFIELD` `HSCAN` `HSET` `HSETNX` `HSTRLEN` `HTTL` `HVALS`

## Streams

`XACK` `XADD` `XAUTOCLAIM` `XCLAIM` `XDEL` `XGROUP` `XINFO` `XLEN` `XPENDING` `XRANGE` `XREAD` `XREADGROUP` `XREVRANGE` `XSETID` `XTRIM`

## HyperLogLog

`PFADD` `PFCOUNT` `PFDEBUG` `PFMERGE` `PFSELFTEST`

## Geo

`GEOADD` `GEODIST` `GEOHASH` `GEOPOS` `GEORADIUS` `GEORADIUSBYMEMBER` `GEOSEARCH` `GEOSEARCHSTORE`

## Generic / key

`COPY` `DEL` `DUMP` `EXISTS` `EXPIRE` `EXPIREAT` `EXPIRETIME` `KEYS` `MIGRATE` `MOVE` `PERSIST` `PEXPIRE` `PEXPIREAT` `PEXPIRETIME` `PTTL` `RANDOMKEY` `RENAME` `RENAMENX` `RESTORE` `SCAN` `SORT` `TOUCH` `TTL` `TYPE` `UNLINK`

## Pub/sub

`PSUBSCRIBE` `PUBLISH` `PUBSUB` `PUNSUBSCRIBE` `SPUBLISH` `SSUBSCRIBE` `SUBSCRIBE` `SUNSUBSCRIBE` `UNSUBSCRIBE`

## Transactions

`DISCARD` `EXEC` `MULTI` `UNWATCH` `WATCH`

## Scripting and functions

`EVAL` `EVAL_RO` `EVALSHA` `EVALSHA_RO` `FCALL` `FCALL_RO` `FUNCTION` `SCRIPT`

## Connection

`AUTH` `ECHO` `HELLO` `PING` `QUIT` `RESET` `SELECT` `WAIT` `WAITAOF` `CLIENT`

## Server / admin

`BGREWRITEAOF` `BGSAVE` `COMMAND` `CONFIG` `DBSIZE` `DEBUG` `FAILOVER` `FLUSHALL` `FLUSHDB` `INFO` `LASTSAVE` `LATENCY` `LOLWUT` `MEMORY` `OBJECT` `SAVE` `SHUTDOWN` `SLOWLOG` `SWAPDB` `TIME`

## Replication

`REPLICAOF` `SLAVEOF` `PSYNC` `REPLCONF` `SYNC`

## ACL

`ACL` with subcommands `SETUSER` `GETUSER` `DELUSER` `LIST` `USERS` `WHOAMI` `CAT` `GENPASS` `DRYRUN` `LOG` `LOAD` `SAVE`

## Cluster (single-node stubs)

`ASKING` `READONLY` `READWRITE` `CLUSTER`

aki is a single node, so `CLUSTER` answers as a one-node cluster and the other three are accepted for client compatibility.

## Container commands

Some commands are containers with their own subcommands: `ACL`, `CLIENT`, `CLUSTER`, `COMMAND`, `CONFIG`, `FUNCTION`, `LATENCY`, `MEMORY`, `OBJECT`, `PUBSUB`, `SCRIPT`, and `SLOWLOG`.
Run `COMMAND DOCS <name>` against a running server for the live signature of any command.
