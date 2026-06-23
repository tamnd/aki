---
title: "Transactions"
description: "How MULTI, EXEC, DISCARD, and WATCH give you queued commands and optimistic locking in aki."
weight: 30
---

aki supports the Redis transaction commands: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, and `UNWATCH`.
They let you queue a group of commands and run them as one atomic unit, with optional optimistic locking so you can do compare-and-set without holding a lock.

## MULTI, EXEC, and DISCARD

`MULTI` starts a transaction.
After it, commands you send are not run right away.
They are queued and the server replies `QUEUED` for each one.

`EXEC` runs the whole queue atomically and returns the replies in order.
`DISCARD` throws the queue away and ends the transaction without running anything.

```bash
redis-cli -x <<'EOF'
MULTI
SET account:1 100
INCRBY account:1 50
EXEC
EOF
```

Inside the session that looks like this.

```
OK
QUEUED
QUEUED
1) OK
2) (integer) 150
```

Nothing else runs between the queued commands, so the two writes land together or not at all.

## WATCH and optimistic locking

`WATCH` gives you a compare-and-set check.
You watch one or more keys before `MULTI`, and if any watched key changes before your `EXEC`, the `EXEC` does nothing and returns a null array.
That tells you another client got there first, so you read the fresh value and try again.
`UNWATCH` clears your watches without running a transaction.

This is optimistic locking.
You do not hold a lock during the read-and-compute step.
You only find out at `EXEC` time whether the value you based your change on is still current.

## A retry loop

Say you want to move an item from a `pending` list to a `done` list, but only if the item is still at the front of `pending`.
You watch the list, read it, build the change, and let `EXEC` confirm nothing moved underneath you.

```bash
redis-cli WATCH pending
# read the value you are about to act on
redis-cli LINDEX pending 0
# queue the move based on what you read
redis-cli MULTI
redis-cli LPOP pending
redis-cli RPUSH done "<the value you read>"
redis-cli EXEC
```

If `EXEC` returns a null array, another client changed `pending` after your `WATCH`.
Start over: `WATCH` again, re-read, re-queue, and `EXEC` again.
A correct client loops on this until `EXEC` returns a non-null reply.

The same pattern works for an atomic counter where `INCR` is not enough, for example when the new value depends on a computation you do client-side between the read and the write.

## How errors behave

There are two kinds of failure, and they behave differently, exactly as in Redis.

A command that cannot even be queued aborts the whole transaction.
If you send a command with the wrong number of arguments or an unknown command name, the server flags the error at queue time, and the later `EXEC` refuses to run anything.

A command that queues fine but fails at run time does not roll back the rest.
The classic case is a type error, like running a list command against a key that holds a string.
That one command returns its error inside the `EXEC` reply, and the other commands in the transaction still run.
aki does not undo a transaction partway through, which matches Redis.

So queue-time errors are caught before anything runs, and run-time errors are reported per command without rolling back the batch.

## Durability

A transaction commits through the same write-ahead log path as any other write.
When `EXEC` returns successfully, the commit is in the log and fsynced according to your durability policy, so a committed `EXEC` survives a crash.
There is no separate, weaker path for transactions.
