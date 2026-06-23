---
title: "Pub/Sub"
description: "Publish and subscribe over channels and patterns, sharded pub/sub, and keyspace notifications."
weight: 40
---

aki speaks the Redis publish/subscribe protocol.
Clients subscribe to channels and other clients publish messages to them.
The server fans each message out to every matching subscriber.
There is no broker state to manage and messages are not stored, so a subscriber only sees messages sent while it is connected.

## Subscribe to exact channels

`SUBSCRIBE` listens on one or more named channels.
`UNSUBSCRIBE` stops listening.
If you call `UNSUBSCRIBE` with no arguments it drops every channel the connection holds.

```bash
SUBSCRIBE news sports
UNSUBSCRIBE news
```

`PUBLISH` sends a message to a channel and returns the number of subscribers that received it.

```bash
PUBLISH news "market opens at 9"
```

## Subscribe to patterns

`PSUBSCRIBE` listens on glob-style patterns instead of exact names.
`news.*` matches `news.tech`, `news.world`, and so on.
`PUNSUBSCRIBE` removes patterns.

```bash
PSUBSCRIBE news.* "user.*.alerts"
PUNSUBSCRIBE news.*
```

A single `PUBLISH` can reach both exact subscribers and pattern subscribers at once.
A client that subscribed with `SUBSCRIBE news.tech` and a client that subscribed with `PSUBSCRIBE news.*` both receive a message published to `news.tech`.

## Sharded pub/sub

Sharded pub/sub keeps a message on the shard that owns the channel rather than broadcasting it everywhere.
The commands mirror the regular ones with an `S` prefix.

```bash
SSUBSCRIBE orders
SPUBLISH orders "new order 1042"
SUNSUBSCRIBE orders
```

On a single aki node these behave like their plain counterparts.
They exist so clients written for sharded pub/sub work without changes.

## Inspect the pub/sub state

`PUBSUB` reports what is currently subscribed.

```bash
PUBSUB CHANNELS          # active channels with at least one subscriber
PUBSUB CHANNELS news.*   # only channels matching the pattern
PUBSUB NUMSUB news       # subscriber count per named channel
PUBSUB NUMPAT            # number of active patterns
PUBSUB SHARDCHANNELS     # active sharded channels
PUBSUB SHARDNUMSUB orders
```

`PUBSUB CHANNELS` lists exact channels only.
Pattern subscriptions show up through `PUBSUB NUMPAT`, not in the channel list.

## RESP2 versus RESP3

Under RESP2 a connection that subscribes enters subscribe mode.
While in that mode it can only run subscribe and unsubscribe commands plus `PING` and `QUIT`.
You typically dedicate one connection to receiving messages.

Under RESP3 messages arrive as push frames, which are a separate frame type from command replies.
The client can tell a pushed message apart from a normal reply, so it can stay subscribed and still run other commands on the same connection.
Switch a connection to RESP3 with `HELLO 3`.

## A two-terminal example

Open one terminal and subscribe.

```bash
redis-cli SUBSCRIBE news
```

It blocks and waits for messages.
In a second terminal, publish.

```bash
redis-cli PUBLISH news "hello"
```

The first terminal prints the message.

```
1) "message"
2) "news"
3) "hello"
```

## Keyspace notifications

aki can publish events when keys change, so you can subscribe to data changes instead of polling.
This is off by default.
Turn it on with the `notify-keyspace-events` config, which takes a string of flag letters that pick which events fire.

```bash
CONFIG SET notify-keyspace-events KEA
```

With notifications on, writing a key publishes to a keyspace channel.

```bash
SUBSCRIBE "__keyspace@0__:mykey"
```

Setting `mykey` then sends a `set` event on that channel.
The `0` is the database number.
See the [configuration reference](/reference/configuration/) for the full list of `notify-keyspace-events` flag letters and what each one selects.
