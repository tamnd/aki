---
title: "CLI"
description: "The aki binary and its subcommands: server, check, import, dump, bench, and version."
weight: 10
---

aki ships as one static binary.
The same binary runs the server and the offline tools that inspect and convert data files.
This page documents every subcommand and its flags.

Run `aki help` for a short summary, or `aki version` to print the build.

```bash
aki <command> [arguments]
```

The commands are `server`, `check`, `import`, `dump`, `bench`, `version`, and `help`.

## aki server

Start the server.
It opens the data file (creating it on first run), builds the keyspace, and listens on the wire until you stop it with Ctrl-C or `SHUTDOWN`.

```bash
aki server [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--addr` | `127.0.0.1:6379` | TCP listen address as `host:port`. |
| `--unixsocket` | (none) | Path to a unix socket to listen on as well as the TCP address. |
| `--maxclients` | `10000` | Maximum number of connected clients. |
| `--databases` | `16` | Number of logical databases. Used only when the file is created. |
| `--requirepass` | (none) | Password required for the default user. |
| `--aclfile` | (none) | Path to an external ACL file, loaded at startup and written by `ACL SAVE`. |
| `--logfile` | (none) | Log file path. Empty logs to stderr. |
| `--loglevel` | (none) | Minimum log level: `debug`, `verbose`, `notice`, or `warning`. |
| `--dbfile` | `aki.db` | Path to the `.aki` data file. |
| `--load-rdb` | (none) | Import this `dump.rdb` on first open only, when the data file does not exist yet. |
| `--rdb-db` | `-1` (all) | With `--load-rdb`, import only this source database number. |

`--databases` only takes effect when the data file is created.
On a later start aki reads the layout back from the file header, so the flag is ignored.

`--load-rdb` is a first-open seed.
If the data file already exists, aki refuses the flag rather than silently skipping the import.

```bash
aki server --addr 127.0.0.1:6379 --dbfile data.aki --requirepass s3cret
```

Seed a fresh database from a Redis dump on first start:

```bash
aki server --dbfile data.aki --load-rdb dump.rdb
```

## aki check

Inspect an `.aki` file and validate its structure.
It opens the file, walks the B-trees, checks the freelist and page accounting, verifies value headers, and reports any TTL problems.

```bash
aki check <file> [flags]
aki check --rdb <file>
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--file` | (none) | Path to the `.aki` file. You can also pass it positionally. |
| `--rdb` | (none) | Validate a Redis `dump.rdb` instead of an `.aki` file. |
| `--fix` | `false` | Repair safe issues, such as clearing impossibly-future TTLs. |
| `--verbose` | `false` | Print per-database detail. |

The exit code reports the worst severity found: `0` healthy, `1` warnings, `2` errors, `3` critical (file not usable).

```bash
aki check data.aki
aki check --rdb dump.rdb
aki check data.aki --fix --verbose
```

## aki import

Import a Redis `dump.rdb` (or a JSONL export) into an `.aki` file, or ship it straight to a running instance.

```bash
aki import <file> [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--format` | `detect` | Input format: `rdb`, `jsonl`, or `detect`. |
| `--target` | `aki.aki` | Path to the `.aki` file to write. |
| `--addr` | (none) | Ship the keys to this running instance instead of writing a file. |
| `--auth` | (none) | Password to send to the `--addr` instance. |
| `--db` | `-1` (all) | Import only this source database. |
| `--replace` | `false` | Overwrite keys that already exist. |
| `--dry-run` | `false` | Parse and count without writing. |

With `detect`, aki picks the format from the leading bytes: the `REDIS` magic means RDB, a leading `{` means JSONL.
AOF input is not supported yet.

```bash
aki import dump.rdb --target data.aki
aki import dump.rdb --addr 127.0.0.1:6379 --auth s3cret --replace
aki import dump.rdb --dry-run
```

## aki dump

Export a keyspace to a Redis `dump.rdb` (or JSONL).
It reads from an offline `.aki` file with `--file`, or from a running instance over the wire with `--addr`.

```bash
aki dump (--file <.aki> | --addr host:port) [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--format` | `rdb` | Output format: `rdb` or `jsonl`. |
| `--output` | `dump.rdb` | Output file path. |
| `--db` | `-1` (all) | Export only this database. |
| `--file` | (none) | Read directly from this `.aki` file, offline. |
| `--addr` | (none) | Connect to a running instance over the wire. |
| `--auth` | (none) | Password to send to the `--addr` instance. |
| `--databases` | `16` | With `--addr`, how many databases to scan. |

Use either `--file` or `--addr`, not both.

```bash
aki dump --file data.aki --output backup.rdb
aki dump --addr 127.0.0.1:6379 --auth s3cret --output live.rdb
aki dump --file data.aki --format jsonl --output data.jsonl
```

## aki bench

Run a load test against a server and report latency.

```bash
aki bench run [flags]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--addr` | `127.0.0.1:6379` | Server address. |
| `--socket` | (none) | Unix socket path, overrides `--addr`. |
| `--tls` | `false` | Dial the server over TLS. |
| `--auth` | (none) | Password to authenticate with. |
| `--clients` | `50` | Number of parallel clients. |
| `--requests` | `1000000` | Total requests per test. |
| `--pipeline` | `1` | Pipeline depth. |
| `--keyspace` | `1000000` | Key cardinality. |
| `--data-size` | `64` | Value size in bytes. |
| `--workload` | `set` | One of `set`, `get`, `mixed`, `cache`, `queue`, `leaderboard`, `session`, `ratelimit`, `stream`. |
| `--ratio` | `9:1` | Read:write ratio for mixed-style workloads. |
| `--access` | `uniform` | Key access pattern: `uniform`, `zipfian`, or `latest`. |
| `--zipf-s` | `1.01` | Zipfian s parameter. |
| `--warmup` | `10000` | Requests to discard at the start. |
| `--duration` | `0` | Run for this long instead of a fixed `--requests`. |
| `--co-correct` | `true` | Coordinated-omission correction. |
| `--hdr-file` | (none) | Write an HdrHistogram percentile table to this path. |
| `--json-out` | (none) | Write the JSON result to this path. |
| `--format` | `text` | Output format: `text`, `json`, or `csv`. |
| `--shard-count` | `0` | Server shard count, recorded in the report. |
| `--quiet` | `false` | Suppress progress output. |

```bash
aki bench run --addr 127.0.0.1:6379 --workload mixed --clients 50 --requests 500000
aki bench run --workload get --pipeline 16 --format json --json-out result.json
```

## aki version

Print the version, commit, and build date.

```bash
aki version
```

```
aki v0.1.0 (commit abc1234, built 2026-06-20)
```

## aki help

Show usage.

```bash
aki help
```
