---
title: "Scripting"
description: "Run Lua scripts and registered functions on the server, atomically and by hash."
weight: 50
---

aki runs Lua scripts on the server.
The Lua engine is pure Go and ships inside the binary, so there is no external interpreter to install.
A script runs atomically.
While it runs, no other command from any client interleaves with it.
That makes a script a clean way to do a read-modify-write without a transaction or a race.

## EVAL

`EVAL` takes the script body, a `numkeys` count, then the keys, then the rest of the arguments.
Inside the script the keys arrive in the `KEYS` table and the other arguments in the `ARGV` table.
Both tables are 1-indexed, which is how Lua counts.

```bash
EVAL "return redis.call('set', KEYS[1], ARGV[1])" 1 mykey myval
```

`numkeys` is `1` here, so `mykey` goes into `KEYS[1]` and `myval` into `ARGV[1]`.
The split between keys and arguments matters: keys are the data the script touches, and naming them lets ACLs and tooling reason about access.

A script can return values back to the client.

```bash
EVAL "return {KEYS[1], ARGV[1], 42}" 1 k v
```

## redis.call and redis.pcall

`redis.call` runs a command from inside the script.
If that command errors, the error stops the script and propagates to the client.
`redis.pcall` runs a command but catches the error and returns it as a Lua table, so the script can inspect it and decide what to do.

```bash
EVAL "local ok = redis.pcall('incr', KEYS[1]); if ok.err then return 'failed' end; return ok" 1 counter
```

## Cache scripts by hash

Sending the full script body on every call wastes bandwidth.
`SCRIPT LOAD` stores a script and returns its SHA1 hash.
`EVALSHA` runs a stored script by that hash.
The pattern is load once, then call by hash many times.

```bash
SCRIPT LOAD "return redis.call('get', KEYS[1])"
# replies with the SHA1, for example a SHA1 string
EVALSHA <sha1> 1 mykey
```

`SCRIPT EXISTS` checks whether one or more hashes are cached.
`SCRIPT FLUSH` clears the whole script cache.

```bash
SCRIPT EXISTS <sha1>
SCRIPT FLUSH
```

If you call `EVALSHA` with a hash the server does not know, it returns a `NOSCRIPT` error.
The usual response is to fall back to `EVAL` with the full body, which also caches it for next time.

## Read-only variants

`EVAL_RO` and `EVALSHA_RO` run a script but reject any write command inside it.
Use them when a script should only read.
On a replica, or behind an ACL that allows reads only, the read-only variant is the one that is permitted.

```bash
EVAL_RO "return redis.call('get', KEYS[1])" 1 mykey
```

## Functions

Functions are the library API on top of the same Lua engine.
Instead of sending a script each time, you load a library once and call named functions in it.
Libraries persist in the data file and reload when the server restarts.

`FUNCTION LOAD` registers a library.
The library declares its name with a shebang line and registers each callable function with `redis.register_function`.

```bash
FUNCTION LOAD "#!lua name=mylib
redis.register_function('myget', function(keys, args)
  return redis.call('get', keys[1])
end)"
```

`FCALL` calls a registered function.
The call shape matches `EVAL`: function name, `numkeys`, then keys, then arguments.

```bash
FCALL myget 1 mykey
```

`FCALL_RO` is the read-only call, the same idea as `EVAL_RO`.

```bash
FCALL_RO myget 1 mykey
```

The rest of the API manages libraries.

```bash
FUNCTION LIST            # list loaded libraries and their functions
FUNCTION DELETE mylib    # drop one library by name
FUNCTION DUMP            # serialize all libraries to a payload
FUNCTION FLUSH           # remove every library
```

`FUNCTION DUMP` pairs with `FUNCTION RESTORE` to move libraries between servers.
Because libraries are stored in the data file, a server you restart comes back with the same functions it had before.
