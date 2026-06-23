---
title: "Configuration"
description: "How to configure aki at startup and at runtime, which directives change behavior, and which are accepted but inert."
weight: 30
---

aki reads its configuration the way Redis does.
You set directives at startup, and you change most of them at runtime over the wire.

At startup you pass the common ones as flags to `aki server` (see the [CLI reference](/reference/cli/)).
At runtime you use the `CONFIG` family.

```bash
redis-cli CONFIG GET maxmemory
redis-cli CONFIG SET maxmemory 256mb
redis-cli CONFIG REWRITE
redis-cli CONFIG RESETSTAT
```

`CONFIG GET` reads a directive (globs work, so `CONFIG GET max*` returns all matches).
`CONFIG SET` changes one.
`CONFIG REWRITE` persists the live values.
`CONFIG RESETSTAT` clears the runtime statistics.

aki registers the full Redis directive surface, around 207 directives, so tooling that enumerates `CONFIG GET *` sees everything it expects.
Not all of those change behavior.
The next two sections split them: the ones that do, and the ones that are accepted for compatibility but inert.

## Directives that change behavior

These actually change how aki runs.

| Directive | Notes |
| --- | --- |
| `maxmemory` | Memory ceiling before eviction kicks in. |
| `maxmemory-policy` | Eviction policy: `noeviction`, `allkeys-lru`, `volatile-ttl`, and so on. |
| `appendonly` | Turn the append-only log on or off. |
| `appendfsync` | Durability policy: `always`, `everysec` (default), or `no`. |
| `no-appendfsync-on-rewrite` | Pause fsyncs during a log rewrite to keep latency steady. |
| `aof-load-truncated` | Tolerate a half-written record at the tail on load. Default `yes`. |
| `aof-timestamp-enabled` | Write `#TS:<ms>` annotations before records. |
| `save` | Snapshot points, as `seconds changes` pairs. |
| `databases` | Number of logical databases. Default `16`, set at file creation. |
| `maxclients` | Maximum connected clients. Default `10000`. |
| `requirepass` | Password for the default user. |
| `timeout` | Close idle client connections after this many seconds. |
| `tcp-keepalive` | TCP keepalive interval. |
| `hash-max-listpack-entries`, `hash-max-listpack-value` | Hash small-encoding limits. |
| `list-max-listpack-size` | List small-encoding limit. |
| `set-max-intset-entries` | Integer-set encoding limit. |
| `set-max-listpack-entries`, `set-max-listpack-value` | Set small-encoding limits. |
| `zset-max-listpack-entries`, `zset-max-listpack-value` | Sorted-set small-encoding limits. |
| `notify-keyspace-events` | Keyspace notification flags. |
| `acl-pubsub-default` | Default channel access for new users. Default `resetchannels`. |
| `client-query-buffer-limit` | Maximum size of a single client query buffer. |
| `proto-max-bulk-len` | Maximum bulk string size accepted on the wire. |
| `lua-time-limit`, `busy-reply-threshold` | How long a script may run before it is reported as busy. |
| `slowlog-log-slower-than` | Microsecond threshold for the slow log. |
| `slowlog-max-len` | Maximum slow-log length. |
| `latency-monitor-threshold` | Millisecond threshold for the latency monitor. |

A few defaults worth stating, since pages elsewhere refer to them: `appendfsync` is `everysec`, `aof-load-truncated` is `yes`, `acl-pubsub-default` is `resetchannels`, `databases` is `16`, and `maxclients` is `10000`.

## Accepted for compatibility, not acted on

A large set of directives, around 103, stay registered so `CONFIG GET` and `CONFIG SET` report the full Redis surface, but they do not change behavior.
The subsystem they configure does not exist in aki.
Setting one is honest about this: the value is stored and returned, and nothing reads it.
This matches how Redis itself treats configs its current build does not act on.

The inert groups, and why:

- **Manual memory management** (active defragmentation, lazyfree frees, `maxmemory-clients`). The Go runtime allocates, frees, and compacts memory on its own, so there is nothing for these to drive.
- **Clustering** (the `cluster-*` family). aki is a single node and runs no cluster bus.
- **TLS transport** (the `tls-*` family, `metrics-tls`). aki ships no TLS listener. Terminate TLS with a proxy in front.
- **IO threading** (`io-threads`, `io-threads-do-reads`, `io-uring`). aki serves each connection on its own goroutine and lets the Go scheduler place the work. There is no shared IO pool to size.
- **Daemonization** (`daemonize`, `supervised`, `pidfile`, `proc-title-template`, and similar). aki is one process started by its command, not a forking daemon.
- **Listener tuning** (`bind`, `tcp-backlog`, `unixsocketperm`, `protected-mode`). The address and socket are chosen when the server binds, not re-read from the live store.
- **Storage engine internals** (`page-size`, `buffer-pool-size`, `wal-autocheckpoint`, the `aki-*` checkpoint knobs, `aki-filename`). The pager and WAL run on built-in defaults, and the page geometry is fixed in the file header when the file is created.
- **Compression and encryption** (`compression`, `rdbcompression`, `encryption`, and their keys). aki stores values as is and ships no codec, which would pull in non-stdlib code.
- **RDB and AOF writer knobs that do not map** (`rdb-compat`, `rdb-save-incremental-fsync`, `aof-rewrite-incremental-fsync`, `aof-use-rdb-preamble`). aki's snapshot is its own file codec, and its AOF rewrite is inline, so these writer steps do not exist.
- **Replication transport tuning** (`repl-diskless-sync`, `repl-timeout`, `repl-backlog-ttl`, the `replica-announce-*` pair, and similar). aki's replica link is its own command stream, not the Redis fork-and-socket path.
- **Output buffer limits** (`client-output-buffer-limit`). aki has no growing per-client output buffer to bound. A slow consumer slows the writer rather than piling up in a buffer.
- **Listpack node sizing** (`list-compress-depth`, `stream-node-max-bytes`). The flat store keeps each value in one structure, so there are no listpack node chains to split. The element-count encoding limits above are wired.
- **Collation** (`locale-collate`). The collation tables live in a non-stdlib package, and aki is zero-dependency, so sort stays on byte and numeric order.

There are a handful of always-on or aki-specific entries too: `sanitize-dump-payload` (aki always fully parses a payload), `shutdown-timeout` (`SHUTDOWN` exits with no fork or replica handoff), and the aki-only `active-expire-*` and `acllog-max-entry-bytes` entries.

The full authoritative breakdown lives in the spec.
The short version: if a directive names a subsystem aki does not have, you can set it, tooling will see it, and it will not do anything.

For how the durability directives behave in practice, see the [persistence guide](/guides/persistence/).
