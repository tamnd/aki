---
title: "Security"
description: "Set a password, define ACL users, control channel access, and front the server with TLS."
weight: 70
---

aki supports the same authentication and access control model as Redis.
There is a password for the default user and a full ACL system for fine-grained users.
This guide covers both, plus how to handle TLS, which aki does not terminate itself.

## A password for the default user

By default aki accepts connections with no password.
Set one with `--requirepass` at startup.

```bash
aki server --dbfile data.aki --requirepass s3cret
```

You can also set it on a running server.

```bash
CONFIG SET requirepass s3cret
```

Once a password is set, clients authenticate with `AUTH` before they run other commands.

```bash
AUTH s3cret
```

`redis-cli` takes the password with `-a`.

```bash
redis-cli -a s3cret
```

`requirepass` configures the built-in `default` user.
For anything beyond one shared password, use ACL users.

## ACL users

ACLs let you create named users, each with their own password and a set of rules for what it can do.
`ACL SETUSER` creates or edits a user.
The rules you can give it include:

- `on` and `off` to enable or disable the user
- `>password` to add a password (and `<password` to remove one)
- `~pattern` to allow keys matching a glob, for example `~cache:*`
- `+command` and `-command` to allow or deny a single command
- `+@category` to allow a whole command category, for example `+@read`
- `&pattern` to allow pub/sub channels matching a glob

Here is a user that can only run `GET` and `SET` on keys under the `app:` prefix.

```bash
ACL SETUSER alice on >alicepw ~app:* +get +set
```

Now `alice` can read and write `app:*` keys and nothing else.

```bash
redis-cli -u redis://alice:alicepw@127.0.0.1:6379
GET app:counter      # allowed
DEL app:counter      # denied, DEL was not granted
GET other:key        # denied, key is outside ~app:*
```

The rest of the ACL commands manage and inspect users.

```bash
ACL LIST             # every user and its rules
ACL GETUSER alice    # the full rule set for one user
ACL WHOAMI           # the user the current connection is authed as
ACL CAT              # list command categories
ACL DELUSER alice    # remove a user
ACL GENPASS          # generate a strong random password
```

`ACL DRYRUN` tests whether a user would be allowed to run a command, without running it.

```bash
ACL DRYRUN alice GET app:counter
ACL DRYRUN alice DEL app:counter
```

`ACL LOG` shows recent denied attempts, which is where you look when a client is being rejected and you want to know why.

```bash
ACL LOG
```

## Channel access defaults

Pub/sub channels are an access-controlled resource too, granted with `&` rules.
The `acl-pubsub-default` config decides what channel access a brand new user starts with.

- `resetchannels` is the Redis 7 default. A new user starts with no channel access and you grant channels explicitly with `&` rules.
- `allchannels` gives a new user the `&*` rule, so it can use any channel out of the box.

```bash
CONFIG SET acl-pubsub-default resetchannels
ACL SETUSER alice on >alicepw ~app:* +get +set &app:events
```

With `resetchannels` in effect, `alice` can use the `app:events` channel and no other.

## External ACL file

You can keep users in a file instead of inline config.
Point the server at it with `--aclfile`.

```bash
aki server --dbfile data.aki --aclfile /etc/aki/users.acl
```

The file is loaded at startup.
`ACL SAVE` rewrites it with the current users, so you can edit users at runtime and persist them back to the file.

```bash
ACL SAVE
```

## TLS

aki ships no TLS transport.
Keeping the binary zero-dependency means it does not embed a TLS stack, so it does not encrypt traffic on the wire by itself.
If you need encryption, terminate TLS in front of aki with a proxy: stunnel, a load balancer, or an SSH tunnel all work.
The proxy handles TLS with clients and forwards plaintext to aki on a local or private network.

The TLS config directives are accepted so existing config files load without errors, but setting them does not start a TLS listener.
Do not rely on them for encryption.

See the [configuration reference](/reference/configuration/) for the full directive list, including the ACL and TLS directives mentioned here.
