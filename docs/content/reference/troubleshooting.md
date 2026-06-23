---
title: "Troubleshooting"
description: "Common problems with aki and how to fix them."
weight: 50
---

This page lists the problems you are most likely to hit and the quick fix for each.
Each item is a short problem and fix pair.

## Connection refused

The client cannot reach the server.

Check the server is running and that you are dialing the right address.
aki listens on `127.0.0.1:6379` by default.
If you started it with a different `--addr`, point the client at the same host and port.

```bash
redis-cli -h 127.0.0.1 -p 6379 PING
```

## NOAUTH or WRONGPASS

The server requires a password and the client did not send the right one.

`NOAUTH` means no password was sent, `WRONGPASS` means the wrong one was.
Start the server with `--requirepass`, then authenticate the client.

```bash
redis-cli -a s3cret PING
# or, after connecting:
redis-cli AUTH s3cret
```

## Permission denied on the data file

aki cannot read or write its files.

The user running aki needs write access to the `.aki` file and to its directory, because the `-wal` and `-shm` sidecars are created next to it.
Fix the ownership or permissions on both the file and the directory.

## Address already in use

Another process already holds the port.

Something else is on `6379` (often another Redis or aki). Stop it, or start aki on a different address.

```bash
aki server --addr 127.0.0.1:6380
```

## The three files travel together

A copied database is missing recent writes.

`data.aki`, `data.aki-wal`, and `data.aki-shm` are one unit.
Do not copy just the `.aki` file: the most recent commits may still be in the `-wal`, and you will lose them.
Either copy all three together, or checkpoint first so the log is folded into the main file, then copy the single `.aki`.

## A file looks corrupt

You suspect the data file is damaged.

Run `aki check` on it.
It validates the header, the B-trees, the freelist, page accounting, and value headers, and reports the worst severity it finds.

```bash
aki check data.aki
aki check data.aki --fix --verbose
```

## An import fails

A `dump.rdb` will not import cleanly.

Validate the dump before importing it.

```bash
aki check --rdb dump.rdb
```

If the dump is fine, run the import with `--dry-run` first to see the key count without writing anything.

```bash
aki import dump.rdb --dry-run
```

## Where to look

When something is slow or wrong, these are the first places to check.

- Start the server with `--logfile` and `--loglevel debug` to capture more detail.
- Run `INFO` for server, memory, persistence, and replication state.
- Run `SLOWLOG GET` to see the slowest recent commands.
- Use the `LATENCY` family to track latency spikes over time.

```bash
redis-cli INFO
redis-cli SLOWLOG GET 10
redis-cli LATENCY LATEST
```
