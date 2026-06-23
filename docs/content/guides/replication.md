---
title: "Replication"
description: "Run a follower that mirrors a leader, promote it back, and wait for acknowledgements."
weight: 60
---

aki is a single node, the same way SQLite is a single file.
On top of that it offers optional leader/follower replication.
A follower keeps a live copy of a leader so you have a hot standby to read from or to fail over to.
This is not sharding.
Every node holds the whole dataset.

## Make a server follow a leader

`REPLICAOF host port` tells a server to become a follower of the leader at that address.

```bash
redis-cli -p 6380 REPLICAOF 127.0.0.1 6379
```

The follower first does an initial sync to copy the leader's current dataset.
After that it streams the leader's command stream and applies each write as it arrives, so it stays close to the leader in real time.

`SLAVEOF` is the old name for the same command and still works.

```bash
redis-cli -p 6380 SLAVEOF 127.0.0.1 6379
```

## Promote a follower back to a leader

`REPLICAOF NO ONE` stops replication and turns the follower back into a standalone leader.
It keeps the data it has already synced and starts accepting writes of its own.

```bash
redis-cli -p 6380 REPLICAOF NO ONE
```

This is how you promote a standby after the original leader goes down.

## The handshake commands

`REPLCONF` and `PSYNC` are the wire-level commands a follower and leader exchange to negotiate and run the sync.
You do not call them by hand.
They show up if you watch the protocol, but `REPLICAOF` drives the whole handshake for you.

## Wait for acknowledgements

`WAIT numreplicas timeout` blocks until at least `numreplicas` followers have acknowledged all the writes the current connection has sent, or until `timeout` milliseconds pass.
It returns the number of followers that acknowledged.
Use it after a critical write when you want confirmation the write reached replicas before you move on.

```bash
SET account:42:balance 1000
WAIT 1 500
```

`WAITAOF` is the same idea for durability on disk.
It blocks until the write has been fsynced to the append-only file on the local node and on the requested number of replicas.

```bash
WAITAOF 1 1 500
```

## What aki is not

aki is not Redis Cluster.
There is no sharding across nodes and no cluster bus.
Replication copies the full dataset to followers; it does not split keys across machines.
If you need horizontal sharding to spread one dataset over many nodes, that is out of scope for aki.

For durability on a single node, the write-ahead log, group commit, and fsync behavior are covered in the [persistence guide](/guides/persistence/).
Replication and persistence work together: persistence keeps one node crash-safe, and replication keeps a second node ready to take over.
