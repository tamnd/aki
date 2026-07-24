#!/usr/bin/env python3
"""Generate fixtures.txt: the compat sections, every STRING, BITMAP,
HLL, HASH, SET, ZSET, GEO, LIST, STREAM, and EXPIRY manifest row from
spec doc 12 exercised against a real redis-server and recorded reply by reply. The Go test replays the
file through the sqlo1 dispatch path, so each fixture line is one
diffed manifest row: same args, same wire reply, error texts included.

Usage: start a throwaway server, then run this script.

  redis-server --port 7399 --save '' --appendonly no --daemonize no &
  python3 gen.py 7399 > fixtures.txt

Generated against Redis 8.8.0. Line format:
  S <section name>
  C <json array of args> -> <json reply>
Scalar replies encode as the RESP first byte plus payload (":n",
"+text", "-error", "$" plus bulk payload, "$-1" for a null bulk);
array replies are JSON arrays of the same, nested. Binary payloads
ride as latin-1 code points inside the JSON strings.

Deep HLL parity (envelope bytes, estimator, debug views) lives in
testdata/hll; the HLL section here covers the manifest surface only.
Deterministic rows only: no wall-clock TTL reads after absolute
EXAT/PXAT stamps, no near-limit allocations on the live server.
"""

import json
import socket
import sys


class R:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.f = self.s.makefile("rb")

    def cmd(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            if isinstance(a, str):
                a = a.encode("latin-1")
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.s.sendall(b"".join(out))
        return self.reply()

    def reply(self):
        line = self.f.readline().rstrip(b"\r\n")
        t, rest = line[:1], line[1:]
        if t in b":+-":
            return t.decode() + rest.decode()
        if t == b"$":
            n = int(rest)
            if n < 0:
                return "$-1"
            payload = self.f.read(n + 2)[:n]
            return "$" + payload.decode("latin-1")
        if t == b"*":
            n = int(rest)
            if n < 0:
                return "*-1"
            return [self.reply() for _ in range(n)]
        raise RuntimeError("unexpected reply " + repr(line))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7399
    r = R(port)
    lines = []

    def c(*args):
        rep = r.cmd(*args)
        lines.append("C %s -> %s" % (json.dumps(list(args)), json.dumps(rep)))
        return rep

    def section(name):
        lines.append("S " + name)

    r.cmd("FLUSHALL")

    # ---------------------------------------------------------------
    section("STRING")

    # SET/GET with every flag, including the TTL rules: plain SET
    # discards, KEEPTTL keeps, expirations validate.
    c("SET", "s:k", "v")
    c("GET", "s:k")
    c("GET", "s:missing")
    c("SET", "s:k", "v2", "XX")
    c("SET", "s:k", "v3", "NX")
    c("GET", "s:k")
    c("SET", "s:new", "v", "NX")
    c("SET", "s:missing2", "v", "XX")
    c("GET", "s:missing2")
    c("SET", "s:k", "v4", "GET")
    c("SET", "s:g", "v", "GET")
    c("GET", "s:g")
    c("SET", "s:k", "v", "EX", "100")
    c("TTL", "s:k")
    c("SET", "s:k", "v5", "KEEPTTL")
    c("TTL", "s:k")
    c("SET", "s:k", "v6")
    c("TTL", "s:k")
    c("SET", "s:k", "v", "PX", "100000")
    c("TTL", "s:k")
    c("SET", "s:k", "v", "EXAT", "9999999999")
    c("SET", "s:k", "v", "PXAT", "9999999999000")
    c("SET", "s:k", "v", "EX", "0")
    c("SET", "s:k", "v", "EX", "notanum")
    c("SET", "s:k", "v", "BOGUS")
    c("SET", "s:k")

    c("SETNX", "s:nx", "v")
    c("SETNX", "s:nx", "w")
    c("GET", "s:nx")
    c("SETEX", "s:se", "100", "v")
    c("TTL", "s:se")
    c("SETEX", "s:se", "0", "v")
    c("PSETEX", "s:pse", "100000", "v")
    c("TTL", "s:pse")
    c("PSETEX", "s:pse", "0", "v")

    c("GETDEL", "s:nx")
    c("GET", "s:nx")
    c("GETDEL", "s:missing")
    c("GETEX", "s:se")
    c("TTL", "s:se")
    c("GETEX", "s:se", "PERSIST")
    c("TTL", "s:se")
    c("GETEX", "s:se", "EX", "50")
    c("TTL", "s:se")
    c("GETEX", "s:se", "EX", "0")
    c("GETEX", "s:se", "BOGUS")
    c("GETEX", "s:missing")

    c("STRLEN", "s:k")
    c("STRLEN", "s:missing")

    # Range reads: clamping, negative indexes, and the degenerate
    # windows.
    c("SET", "s:r", "Hello World")
    c("GETRANGE", "s:r", "0", "4")
    c("GETRANGE", "s:r", "-5", "-1")
    c("GETRANGE", "s:r", "0", "-1")
    c("GETRANGE", "s:r", "6", "3")
    c("GETRANGE", "s:r", "50", "100")
    c("GETRANGE", "s:r", "-100", "-50")
    c("GETRANGE", "s:missing", "0", "-1")
    c("GETRANGE", "s:r", "x", "1")
    c("SUBSTR", "s:r", "0", "4")

    c("APPEND", "s:app", "abc")
    c("APPEND", "s:app", "def")
    c("GET", "s:app")

    # SETRANGE: zero-fill on extension, in-place overwrite, the
    # no-create rule for empty writes, and both error guards.
    c("SETRANGE", "s:sr", "5", "hello")
    c("GET", "s:sr")
    c("SETRANGE", "s:sr", "0", "XY")
    c("GET", "s:sr")
    c("SETRANGE", "s:empty", "0", "")
    c("GET", "s:empty")
    c("SETRANGE", "s:sr", "-1", "x")
    c("SETRANGE", "s:big", "536870911", "xx")

    # INCR family: the canonical-integer rule and the overflow text.
    c("INCR", "s:ctr")
    c("INCRBY", "s:ctr", "41")
    c("DECR", "s:ctr")
    c("DECRBY", "s:ctr", "40")
    c("SET", "s:ctr2", "9223372036854775807")
    c("INCR", "s:ctr2")
    c("DECRBY", "s:ctr2", "-1")
    c("SET", "s:ctr3", "notanint")
    c("INCR", "s:ctr3")
    c("SET", "s:ctr4", " 11")
    c("INCR", "s:ctr4")
    c("SET", "s:ctr5", "011")
    c("INCR", "s:ctr5")
    c("INCRBY", "s:ctr", "x")

    # Only exactly-representable increments here: on values where
    # double and x87 long double arithmetic differ, redis builds
    # disagree with each other (macOS prints the trimmed %.17Lf of a
    # 64-bit long double, the Linux gate rivals compute in 80-bit),
    # so those rows cannot be pinned portably. See the README.
    c("INCRBYFLOAT", "s:f", "10.5")
    c("INCRBYFLOAT", "s:f", "0.25")
    c("INCRBYFLOAT", "s:f", "-5")
    c("GET", "s:f")
    c("SET", "s:f2", "5.0e3")
    c("INCRBYFLOAT", "s:f2", "200")
    c("INCRBYFLOAT", "s:f2", "x")
    c("SET", "s:f3", "abc")
    c("INCRBYFLOAT", "s:f3", "1")

    # Multi-key rows, batched IO on our side.
    c("MSET", "s:m1", "a", "s:m2", "b")
    c("MGET", "s:m1", "s:m2", "s:missing")
    c("MSET", "s:m1")
    c("MSET", "s:m1", "a", "s:m2")
    c("MSETNX", "s:n1", "a", "s:n2", "b")
    c("MSETNX", "s:n2", "c", "s:n3", "d")
    c("MGET", "s:n1", "s:n2", "s:n3")

    # OBJECT ENCODING at the int/embstr/raw boundaries. The rope
    # encoding sqlo1 reports over the rope boundary is a documented
    # divergence, not fixtured here.
    c("OBJECT", "ENCODING", "s:ctr")
    c("SET", "s:emb", "short")
    c("OBJECT", "ENCODING", "s:emb")
    c("SET", "s:emb44", "x" * 44)
    c("OBJECT", "ENCODING", "s:emb44")
    c("SET", "s:emb45", "x" * 45)
    c("OBJECT", "ENCODING", "s:emb45")
    c("SET", "s:negz", "-0")
    c("OBJECT", "ENCODING", "s:negz")
    c("OBJECT", "ENCODING", "s:missing")

    c("TYPE", "s:k")
    c("TYPE", "s:missing")

    # LCS manifest rows; the full option matrix is pinned in the Go
    # LCS tests, these keep the family table complete here.
    c("SET", "s:lcs1", "ohmytext")
    c("SET", "s:lcs2", "mynewtext")
    c("LCS", "s:lcs1", "s:lcs2")
    c("LCS", "s:lcs1", "s:lcs2", "LEN")
    c("LCS", "s:lcs1", "s:lcs2", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN")

    # ---------------------------------------------------------------
    section("BITMAP")

    c("SETBIT", "b:k", "7", "1")
    c("GETBIT", "b:k", "7")
    c("GETBIT", "b:k", "100")
    c("SETBIT", "b:k", "7", "0")
    c("SETBIT", "b:k", "7", "2")
    c("SETBIT", "b:k", "-1", "1")
    c("SETBIT", "b:k", "4294967296", "1")
    c("GETBIT", "b:missing", "0")

    c("SET", "b:s", "foobar")
    c("BITCOUNT", "b:s")
    c("BITCOUNT", "b:s", "0", "0")
    c("BITCOUNT", "b:s", "1", "1")
    c("BITCOUNT", "b:s", "0", "-5")
    c("BITCOUNT", "b:s", "0", "-50")
    c("BITCOUNT", "b:s", "0", "5", "BYTE")
    c("BITCOUNT", "b:s", "5", "30", "BIT")
    c("BITCOUNT", "b:missing")
    c("BITCOUNT", "b:s", "0")
    c("BITCOUNT", "b:s", "0", "1", "BOGUS")
    c("BITCOUNT", "b:s", "x", "1")

    c("BITPOS", "b:s", "1")
    c("BITPOS", "b:s", "1", "0", "-50")
    c("SET", "b:z", "\x00\xff\xf0")
    c("BITPOS", "b:z", "1", "0")
    c("BITPOS", "b:z", "1", "2")
    c("BITPOS", "b:z", "1", "0", "-1", "BIT")
    c("SET", "b:ones", "\xff\xff\xff")
    c("BITPOS", "b:ones", "0")
    c("BITPOS", "b:ones", "0", "0", "-1")
    c("BITPOS", "b:ones", "0", "0", "-1", "BIT")
    c("BITPOS", "b:missing", "1")
    c("BITPOS", "b:missing", "0")
    c("BITPOS", "b:s", "2")

    c("BITFIELD", "bf:k", "SET", "u8", "0", "255", "GET", "u8", "0")
    c("BITFIELD", "bf:k", "INCRBY", "u8", "0", "10")
    c("BITFIELD", "bf:k", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "250")
    c("BITFIELD", "bf:k", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "10")
    c("BITFIELD", "bf:k", "GET", "i8", "0")
    c("BITFIELD", "bf:k", "GET", "u8", "#1")
    c("BITFIELD", "bf:k", "SET", "u64", "0", "1")
    c("BITFIELD", "bf:k", "SET", "u8", "0")
    c("BITFIELD", "bf:k", "OVERFLOW", "BOGUS", "GET", "u8", "0")
    c("BITFIELD", "bf:missing", "GET", "u16", "0")
    c("BITFIELD_RO", "bf:k", "GET", "u8", "0")
    c("BITFIELD_RO", "bf:k", "SET", "u8", "0", "1")

    c("SET", "b:x", "abc")
    c("SET", "b:y", "abd")
    c("BITOP", "AND", "b:dand", "b:x", "b:y")
    c("GET", "b:dand")
    c("BITOP", "OR", "b:dor", "b:x", "b:y")
    c("GET", "b:dor")
    c("BITOP", "XOR", "b:dx", "b:x", "b:y")
    c("GET", "b:dx")
    c("BITOP", "NOT", "b:dn", "b:x")
    c("GET", "b:dn")
    c("BITOP", "NOT", "b:dn", "b:x", "b:y")
    c("BITOP", "BOGUS", "b:d", "b:x")
    c("BITOP", "XOR", "b:dz", "b:missing1", "b:missing2")
    c("GET", "b:dz")
    c("SET", "b:short", "ab")
    c("BITOP", "XOR", "b:dxl", "b:x", "b:short")
    c("GET", "b:dxl")

    # ---------------------------------------------------------------
    section("HLL")

    c("PFADD", "h:a", "x", "y", "z")
    c("PFCOUNT", "h:a")
    c("PFADD", "h:b", "y", "z", "w")
    c("PFMERGE", "h:m", "h:a", "h:b")
    c("PFCOUNT", "h:m")
    c("PFCOUNT", "h:a", "h:b")
    c("SET", "h:plain", "x")
    c("PFCOUNT", "h:plain")
    c("PFADD", "h:plain", "x")
    c("PFDEBUG", "ENCODING", "h:a")
    c("TYPE", "h:a")

    # ---------------------------------------------------------------
    section("HASH")

    # Point surface: create/update counts, misses, arity.
    c("HSET", "hs:k", "f1", "v1")
    c("HSET", "hs:k", "f1", "w", "f2", "v2")
    c("HGET", "hs:k", "f1")
    c("HGET", "hs:k", "nofield")
    c("HGET", "hs:missing", "f")
    c("HSET", "hs:k")
    c("HSET", "hs:k", "f1")
    c("HSETNX", "hs:k", "f1", "x")
    c("HGET", "hs:k", "f1")
    c("HSETNX", "hs:k", "f3", "v3")
    c("HMSET", "hs:k", "f4", "v4", "f5", "v5")
    c("HMGET", "hs:k", "f1", "f4", "nofield")
    c("HMGET", "hs:missing", "a", "b")
    c("HEXISTS", "hs:k", "f1")
    c("HEXISTS", "hs:k", "nofield")
    c("HEXISTS", "hs:missing", "f")
    c("HSTRLEN", "hs:k", "f1")
    c("HSTRLEN", "hs:k", "nofield")
    c("HSTRLEN", "hs:missing", "f")
    c("HLEN", "hs:k")
    c("HLEN", "hs:missing")
    c("HDEL", "hs:k", "f4", "f5", "nofield")
    c("HLEN", "hs:k")
    c("TYPE", "hs:k")
    c("OBJECT", "ENCODING", "hs:k")

    # Deleting the last field kills the key.
    c("HSET", "hs:d", "a", "1", "b", "2")
    c("HDEL", "hs:d", "a", "b")
    c("HLEN", "hs:d")
    c("TYPE", "hs:d")

    # Empty field and value are legal.
    c("HSET", "hs:e", "", "")
    c("HGET", "hs:e", "")
    c("HSTRLEN", "hs:e", "")
    c("HDEL", "hs:e", "")

    # Counters.
    c("HSET", "hs:n", "cnt", "10")
    c("HINCRBY", "hs:n", "cnt", "5")
    c("HINCRBY", "hs:n", "cnt", "-20")
    c("HINCRBY", "hs:n", "fresh", "3")
    c("HINCRBY", "hs:newn", "f", "2")
    c("HGET", "hs:newn", "f")
    c("HSET", "hs:n", "txt", "abc")
    c("HINCRBY", "hs:n", "txt", "1")
    c("HINCRBY", "hs:n", "cnt", "notanum")
    c("HSET", "hs:n", "big", "9223372036854775807")
    c("HINCRBY", "hs:n", "big", "1")
    c("HINCRBYFLOAT", "hs:n", "fl", "10.5")
    c("HINCRBYFLOAT", "hs:n", "fl", "0.25")
    c("HINCRBYFLOAT", "hs:n", "fl", "-0.75")
    c("HINCRBYFLOAT", "hs:n", "fl", "5.0e3")
    c("HINCRBYFLOAT", "hs:n", "txt", "1")
    c("HINCRBYFLOAT", "hs:n", "fl", "notanum")

    # Iteration order on the listpack tier: insertion order, updates
    # keep position, re-adds append.
    c("HSET", "hs:it", "a", "1", "b", "2", "c", "3")
    c("HGETALL", "hs:it")
    c("HKEYS", "hs:it")
    c("HVALS", "hs:it")
    c("HGETALL", "hs:missing")
    c("HKEYS", "hs:missing")
    c("HVALS", "hs:missing")
    c("HSET", "hs:it", "a", "9")
    c("HGETALL", "hs:it")
    c("HDEL", "hs:it", "b")
    c("HGETALL", "hs:it")
    c("HSET", "hs:it", "b", "5")
    c("HGETALL", "hs:it")

    # HRANDFIELD, deterministic rows only: misses, count 0, and a
    # one-field hash where every draw is forced.
    c("HRANDFIELD", "hs:missing")
    c("HRANDFIELD", "hs:missing", "3")
    c("HSET", "hs:one", "solo", "val")
    c("HRANDFIELD", "hs:one")
    c("HRANDFIELD", "hs:one", "5")
    c("HRANDFIELD", "hs:one", "-3")
    c("HRANDFIELD", "hs:one", "2", "WITHVALUES")
    c("HRANDFIELD", "hs:one", "0")

    # HSCAN on the listpack tier answers any cursor with everything.
    c("HSCAN", "hs:it", "0")
    c("HSCAN", "hs:it", "0", "NOVALUES")
    c("HSCAN", "hs:it", "0", "MATCH", "a*")
    c("HSCAN", "hs:it", "0", "COUNT", "100")
    c("HSCAN", "hs:it", "42")
    c("HSCAN", "hs:missing", "0")

    # A big hash crosses the encoding boundary through the value-size
    # wall, which both sides share in spirit: values over 64 B kick
    # redis to hashtable, and 30 of them blow sqlo1's 2 KiB inline
    # byte cap. The count threshold is deliberately not asserted here;
    # sqlo1 segments at 129 fields while redis 8.8's default listpack
    # cap is 512, a documented standing divergence (see README).
    big = ["HSET", "hs:big"]
    for i in range(30):
        big += ["f%03d" % i, ("v%03d" % i) + "x" * 97]
    c(*big)
    c("OBJECT", "ENCODING", "hs:big")
    c("HLEN", "hs:big")
    c("HGET", "hs:big", "f010")

    # HGETEX and HGETDEL. Absolute stamps keep the replies
    # deterministic; the relative HTTL readback rounds to seconds.
    c("HSET", "hs:x", "a", "va", "b", "vb")
    c("HGETEX", "hs:x", "FIELDS", "2", "a", "nofield")
    c("HGETEX", "hs:x", "PXAT", "9999999999000", "FIELDS", "1", "a")
    c("HPEXPIRETIME", "hs:x", "FIELDS", "2", "a", "b")
    c("HGETEX", "hs:x", "PERSIST", "FIELDS", "1", "a")
    c("HTTL", "hs:x", "FIELDS", "1", "a")
    c("HGETEX", "hs:x", "EX", "100", "FIELDS", "1", "a")
    c("HTTL", "hs:x", "FIELDS", "1", "a")
    c("HGETEX", "hs:x", "EX", "notanum", "FIELDS", "1", "a")
    c("HGETDEL", "hs:x", "FIELDS", "2", "b", "nofield")
    c("HEXISTS", "hs:x", "b")
    c("HGETDEL", "hs:x", "FIELDS", "1", "a")
    c("TYPE", "hs:x")

    # The HEXPIRE family. Missing key, missing field, the condition
    # table over absolute stamps, past-time deletes, key death.
    c("HSET", "hs:t", "f1", "v1", "f2", "v2")
    c("HEXPIRE", "hs:missing", "100", "FIELDS", "1", "f")
    c("HTTL", "hs:missing", "FIELDS", "1", "f")
    c("HPERSIST", "hs:missing", "FIELDS", "1", "f")
    c("HEXPIRE", "hs:t", "100", "FIELDS", "2", "f1", "nofield")
    c("HTTL", "hs:t", "FIELDS", "3", "f1", "f2", "nofield")
    c("HPTTL", "hs:t", "FIELDS", "2", "f2", "nofield")
    c("OBJECT", "ENCODING", "hs:t")
    c("HPERSIST", "hs:t", "FIELDS", "2", "f1", "f2")
    c("HEXPIREAT", "hs:t", "9999999999", "FIELDS", "1", "f1")
    c("HEXPIRETIME", "hs:t", "FIELDS", "2", "f1", "f2")
    c("HPEXPIREAT", "hs:t", "9999999999000", "FIELDS", "1", "f1")
    c("HPEXPIRETIME", "hs:t", "FIELDS", "1", "f1")
    c("HEXPIRE", "hs:t", "100", "NX", "FIELDS", "2", "f1", "f2")
    c("HPERSIST", "hs:t", "FIELDS", "1", "f2")
    c("HPEXPIREAT", "hs:t", "9999999999500", "XX", "FIELDS", "2", "f1", "f2")
    c("HPEXPIREAT", "hs:t", "9999999999000", "GT", "FIELDS", "1", "f1")
    c("HPEXPIREAT", "hs:t", "9999999999500", "GT", "FIELDS", "1", "f1")
    c("HPEXPIREAT", "hs:t", "9999999999900", "GT", "FIELDS", "1", "f1")
    c("HPEXPIREAT", "hs:t", "9999999999900", "LT", "FIELDS", "1", "f1")
    c("HPEXPIREAT", "hs:t", "9999999999000", "LT", "FIELDS", "1", "f1")
    c("HEXPIRE", "hs:t", "100", "GT", "FIELDS", "1", "f2")
    c("HEXPIRE", "hs:t", "100", "LT", "FIELDS", "1", "f2")
    c("HPEXPIREAT", "hs:t", "1", "FIELDS", "1", "f2")
    c("HEXISTS", "hs:t", "f2")
    c("HEXPIRE", "hs:t", "0", "FIELDS", "1", "f1")
    c("TYPE", "hs:t")

    # A TTL on the hashtable tier.
    c("HEXPIRE", "hs:big", "100", "FIELDS", "1", "f000")
    c("HTTL", "hs:big", "FIELDS", "1", "f000")
    c("OBJECT", "ENCODING", "hs:big")

    # The grammar's error table.
    c("HSET", "hs:err", "f", "v")
    c("HEXPIRE", "hs:err", "100")
    c("HEXPIRE", "hs:err", "100", "FIELDS", "1")
    c("HTTL", "hs:err", "FIELDS", "1")
    c("HPERSIST", "hs:err", "FIELDS", "1")
    c("HEXPIRE", "hs:err", "notanum", "FIELDS", "1", "f")
    c("HEXPIRE", "hs:err", "-1", "FIELDS", "1", "f")
    c("HEXPIRE", "hs:err", "70368744177664", "FIELDS", "1", "f")
    c("HPEXPIREAT", "hs:err", "70368744177664", "FIELDS", "1", "f")
    c("HEXPIRE", "hs:err", "100", "BADCOND", "FIELDS", "1", "f")
    c("HEXPIRE", "hs:err", "100", "NX", "NOTFIELDS", "1", "f")
    c("HTTL", "hs:err", "NOTFIELDS", "1", "f")
    c("HEXPIRE", "hs:err", "100", "FIELDS", "0", "f")
    c("HEXPIRE", "hs:err", "100", "FIELDS", "x", "f")
    c("HTTL", "hs:err", "FIELDS", "0", "f")
    c("HPERSIST", "hs:err", "FIELDS", "x", "f")
    c("HEXPIRE", "hs:err", "100", "FIELDS", "2", "f")
    c("HTTL", "hs:err", "FIELDS", "1", "f", "g")

    # Type walls both ways.
    c("SET", "hs:str", "v")
    c("HGET", "hs:str", "f")
    c("HSET", "hs:str", "f", "v")
    c("HDEL", "hs:str", "f")
    c("HGETALL", "hs:str")
    c("HRANDFIELD", "hs:str")
    c("HSCAN", "hs:str", "0")
    c("HEXPIRE", "hs:str", "100", "FIELDS", "1", "f")
    c("HTTL", "hs:str", "FIELDS", "1", "f")
    c("HGETEX", "hs:str", "FIELDS", "1", "f")
    c("HGETDEL", "hs:str", "FIELDS", "1", "f")
    c("GET", "hs:k")

    # ---------------------------------------------------------------
    section("SET")
    # Sets are unordered, and the two sides genuinely emit different
    # orders where the representations differ (redis walks its
    # listpack or sorted intset, sqlo1 emits in fh order once
    # segmented and in insertion order inline), so multi-member
    # replies are pinned only where the orders provably agree:
    # listpack-tier insertion order, intsets inserted ascending, and
    # single-member results. Everything wider goes through the
    # integer commands, the STORE variants, and SMISMEMBER probes.

    # Point surface: create counts, dup handling, misses, arity.
    c("SADD", "st:k", "a")
    c("SADD", "st:k", "a", "b", "c")
    c("SADD", "st:k", "b")
    c("SCARD", "st:k")
    c("SCARD", "st:missing")
    c("SADD", "st:k")
    c("SISMEMBER", "st:k", "a")
    c("SISMEMBER", "st:k", "nope")
    c("SISMEMBER", "st:missing", "a")
    c("SMISMEMBER", "st:k", "a", "nope", "b")
    c("SMISMEMBER", "st:missing", "a", "b")
    c("SMISMEMBER", "st:k")
    c("SREM", "st:k", "c", "nope")
    c("SREM", "st:missing", "a")
    c("SCARD", "st:k")
    c("TYPE", "st:k")

    # Removing the last member kills the key.
    c("SADD", "st:d", "x")
    c("SREM", "st:d", "x")
    c("SCARD", "st:d")
    c("TYPE", "st:d")

    # The empty member is legal.
    c("SADD", "st:e", "")
    c("SISMEMBER", "st:e", "")
    c("SREM", "st:e", "")

    # SMEMBERS order on the listpack tier: insertion order, removals
    # keep it, re-adds append. Both sides share the rule.
    c("SADD", "st:it", "one", "two", "three")
    c("SMEMBERS", "st:it")
    c("SREM", "st:it", "two")
    c("SMEMBERS", "st:it")
    c("SADD", "st:it", "two")
    c("SMEMBERS", "st:it")
    c("SMEMBERS", "st:missing")
    c("OBJECT", "ENCODING", "st:it")

    # intset rows insert ascending so the orders agree: redis stores
    # an intset numerically sorted while sqlo1 keeps insertion order
    # (see the README). "011" is not canonical, so it breaks the
    # intset the same way on both sides.
    c("SADD", "st:int", "1", "2", "30")
    c("OBJECT", "ENCODING", "st:int")
    c("SMEMBERS", "st:int")
    c("SADD", "st:int2", "7", "011")
    c("OBJECT", "ENCODING", "st:int2")
    c("SMEMBERS", "st:int2")

    # The hashtable tier through the member-size wall, which both
    # sides share in spirit: members over 64 B kick redis out of its
    # listpack, and 30 of them blow sqlo1's 2 KiB inline byte cap.
    # The count threshold is deliberately not asserted; sqlo1
    # segments at 129 members while redis 8.8's defaults are 512 for
    # both intsets and listpacks, a documented standing divergence.
    big = ["SADD", "st:big"]
    for i in range(30):
        big.append(("m%03d" % i) + "x" * 97)
    c(*big)
    c("OBJECT", "ENCODING", "st:big")
    c("SCARD", "st:big")
    c("SISMEMBER", "st:big", "m010" + "x" * 97)

    # SPOP and SRANDMEMBER, deterministic rows only: misses, count 0,
    # and one-member sets where every draw is forced. Distribution
    # lives in the spop lab.
    c("SPOP", "st:missing")
    c("SPOP", "st:missing", "3")
    c("SRANDMEMBER", "st:missing")
    c("SRANDMEMBER", "st:missing", "3")
    c("SADD", "st:one", "solo")
    c("SRANDMEMBER", "st:one")
    c("SRANDMEMBER", "st:one", "5")
    c("SRANDMEMBER", "st:one", "-3")
    c("SRANDMEMBER", "st:one", "0")
    c("SPOP", "st:one", "0")
    c("SPOP", "st:one")
    c("TYPE", "st:one")
    c("SADD", "st:one2", "solo")
    c("SPOP", "st:one2", "99")
    c("TYPE", "st:one2")
    c("SPOP", "st:k", "-1")
    c("SPOP", "st:k", "x")
    c("SRANDMEMBER", "st:k", "x")

    # SMOVE doors: the move, the dup landing, misses, self-move, and
    # the source dying with its last member.
    c("SADD", "st:src", "m", "n")
    c("SADD", "st:dst", "n")
    c("SMOVE", "st:src", "st:dst", "m")
    c("SISMEMBER", "st:src", "m")
    c("SISMEMBER", "st:dst", "m")
    c("SMOVE", "st:src", "st:dst", "n")
    c("SCARD", "st:dst")
    c("TYPE", "st:src")
    c("SMOVE", "st:missing", "st:dst", "m")
    c("SMOVE", "st:dst", "st:dst", "m")
    c("SMOVE", "st:dst", "st:dst", "ghost")
    c("SMOVE", "st:dst", "st:fresh", "m")
    c("SMEMBERS", "st:fresh")

    # Algebra: integer replies and single-member results pin byte for
    # byte; wider results go through the STORE variants and
    # membership probes.
    c("SADD", "st:a1", "a", "b", "c")
    c("SADD", "st:a2", "b", "c", "d")
    c("SADD", "st:a3", "c", "d", "e")
    c("SINTER", "st:a1", "st:a2", "st:a3")
    c("SINTER", "st:a1", "st:missing")
    c("SINTER", "st:missing", "st:a1")
    c("SDIFF", "st:a1", "st:a2")
    c("SDIFF", "st:missing", "st:a1")
    c("SUNION", "st:missing", "st:missing2")
    c("SINTER")
    c("SUNION")
    c("SDIFF")
    c("SINTERCARD", "2", "st:a1", "st:a2")
    c("SINTERCARD", "2", "st:a1", "st:a2", "LIMIT", "1")
    c("SINTERCARD", "2", "st:a1", "st:a2", "LIMIT", "0")
    c("SINTERCARD", "0", "st:a1")
    c("SINTERCARD", "2", "st:a1", "st:a2", "LIMIT", "-1")
    c("SINTERCARD", "2", "st:a1")

    c("SINTERSTORE", "st:di", "st:a1", "st:a2")
    c("SMISMEMBER", "st:di", "a", "b", "c", "d")
    c("SUNIONSTORE", "st:du", "st:a1", "st:a3")
    c("SCARD", "st:du")
    c("SMISMEMBER", "st:du", "a", "b", "c", "d", "e", "f")
    c("SDIFFSTORE", "st:dd", "st:a1", "st:a3")
    c("SMISMEMBER", "st:dd", "a", "b", "c")
    c("TYPE", "st:di")
    c("OBJECT", "ENCODING", "st:di")

    # An empty result deletes the destination; the SINTERSTORE absent
    # short circuit reaches it before later keys.
    c("SINTERSTORE", "st:di", "st:a1", "st:missing")
    c("TYPE", "st:di")
    c("SCARD", "st:di")

    # A stored destination drops its TTL and overwrites any type.
    c("SADD", "st:ttl", "x")
    c("EXPIRE", "st:ttl", "600")
    c("SUNIONSTORE", "st:ttl", "st:a1")
    c("TTL", "st:ttl")
    c("SET", "st:sdest", "v")
    c("SUNIONSTORE", "st:sdest", "st:a1")
    c("TYPE", "st:sdest")
    c("SINTERSTORE", "st:x")
    c("SUNIONSTORE", "st:x")
    c("SDIFFSTORE", "st:x")

    # SSCAN answers any cursor with everything on the small tiers;
    # one-member sets keep the reply order-free.
    c("SADD", "st:sc", "only")
    c("SSCAN", "st:sc", "0")
    c("SSCAN", "st:sc", "0", "MATCH", "z*")
    c("SSCAN", "st:sc", "0", "MATCH", "on*")
    c("SSCAN", "st:sc", "0", "COUNT", "100")
    c("SSCAN", "st:sc", "42")
    c("SSCAN", "st:missing", "0")
    c("SSCAN", "st:sc", "0", "NOVALUES")

    # Type walls both ways.
    c("SET", "st:str", "v")
    c("SADD", "st:str", "m")
    c("SREM", "st:str", "m")
    c("SCARD", "st:str")
    c("SISMEMBER", "st:str", "m")
    c("SMISMEMBER", "st:str", "m")
    c("SMEMBERS", "st:str")
    c("SPOP", "st:str")
    c("SRANDMEMBER", "st:str")
    c("SSCAN", "st:str", "0")
    c("SMOVE", "st:str", "st:k", "m")
    c("SMOVE", "st:k", "st:str", "a")
    c("SINTER", "st:str", "st:k")
    c("SINTERSTORE", "st:d2", "st:str", "st:k")
    c("GET", "st:k")

    # ---------------------------------------------------------------
    section("ZSET")
    # zsets are ordered by (score, member) on both sides at every
    # tier, so multi-member replies pin byte for byte throughout, the
    # big difference from the SET section. Random draws stay
    # deterministic-rows-only, and cursor order is pinned only where
    # both sides walk a sorted small tier.

    # Point surface: create/update counts, CH, the flag table, INCR.
    c("ZADD", "z:k", "1", "a")
    c("ZADD", "z:k", "2", "b", "3", "c")
    c("ZADD", "z:k", "5", "a")
    c("ZADD", "z:k", "CH", "6", "a", "4", "d")
    c("ZADD", "z:k", "NX", "9", "a", "1", "e")
    c("ZSCORE", "z:k", "a")
    c("ZADD", "z:k", "XX", "2", "a", "7", "nope")
    c("ZSCORE", "z:k", "a")
    c("ZSCORE", "z:k", "nope")
    c("ZSCORE", "z:missing", "a")
    c("ZADD", "z:k", "GT", "1", "a")
    c("ZSCORE", "z:k", "a")
    c("ZADD", "z:k", "GT", "8", "a")
    c("ZADD", "z:k", "LT", "9", "a")
    c("ZADD", "z:k", "LT", "3", "a")
    c("ZSCORE", "z:k", "a")
    c("ZADD", "z:k", "INCR", "2", "a")
    c("ZADD", "z:k", "NX", "INCR", "1", "a")
    c("ZADD", "z:k", "XX", "INCR", "0.5", "a")
    c("ZADD", "z:k", "GT", "INCR", "-5", "a")
    c("ZADD", "z:missing", "XX", "INCR", "1", "m")

    # Grammar and flag conflicts.
    c("ZADD", "z:k")
    c("ZADD", "z:k", "1")
    c("ZADD", "z:k", "1", "a", "2")
    c("ZADD", "z:k", "NX", "XX", "1", "a")
    c("ZADD", "z:k", "GT", "NX", "1", "a")
    c("ZADD", "z:k", "GT", "LT", "1", "a")
    c("ZADD", "z:k", "INCR", "1", "a", "2", "b")
    c("ZADD", "z:k", "notanum", "a")
    c("ZADD", "z:k", "nan", "a")

    c("ZMSCORE", "z:k", "a", "nope", "b")
    c("ZMSCORE", "z:missing", "a")
    c("ZMSCORE", "z:k")
    c("ZCARD", "z:k")
    c("ZCARD", "z:missing")

    c("ZINCRBY", "z:k", "1.5", "b")
    c("ZINCRBY", "z:k", "-10", "fresh")
    c("ZINCRBY", "z:k", "notanum", "b")
    c("ZINCRBY", "z:missing2", "3", "m")
    c("TYPE", "z:missing2")
    c("DEL", "z:missing2")
    c("ZREM", "z:k", "fresh", "nope")
    c("ZREM", "z:missing", "a")
    c("ZREM", "z:k")

    # Removing the last member kills the key.
    c("ZADD", "z:d", "1", "x")
    c("ZREM", "z:d", "x")
    c("ZCARD", "z:d")
    c("TYPE", "z:d")

    # Score printing: integral doubles trim, -0 canonicalizes to 0,
    # infinities round-trip, inf minus inf is the NaN door.
    c("ZADD", "z:f", "3.0", "i", "2.5", "h", "-0", "z", "inf", "p", "-inf", "n")
    c("ZSCORE", "z:f", "i")
    c("ZSCORE", "z:f", "z")
    c("ZSCORE", "z:f", "p")
    c("ZSCORE", "z:f", "n")
    c("ZINCRBY", "z:f", "+inf", "n")
    c("ZADD", "z:f", "INCR", "-inf", "p")
    c("ZRANGE", "z:f", "0", "-1", "WITHSCORES")

    # Rank math, the WITHSCORE forms included.
    c("ZRANK", "z:f", "n")
    c("ZRANK", "z:f", "p")
    c("ZRANK", "z:f", "nope")
    c("ZRANK", "z:missing", "m")
    c("ZREVRANK", "z:f", "n")
    c("ZREVRANK", "z:f", "p")
    c("ZRANK", "z:f", "h", "WITHSCORE")
    c("ZRANK", "z:f", "nope", "WITHSCORE")
    c("ZRANK", "z:missing", "m", "WITHSCORE")
    c("ZREVRANK", "z:f", "i", "WITHSCORE")
    c("ZRANK", "z:f", "h", "BOGUS")

    # The range family: index, score, and lex forms with REV and
    # LIMIT, plus the door table.
    c("ZADD", "z:r", "1", "a", "2", "b", "2", "c", "3", "d", "4", "e")
    c("ZRANGE", "z:r", "0", "-1")
    c("ZRANGE", "z:r", "1", "3", "WITHSCORES")
    c("ZRANGE", "z:r", "-2", "-1")
    c("ZRANGE", "z:r", "3", "1")
    c("ZRANGE", "z:r", "10", "20")
    c("ZRANGE", "z:r", "0", "-1", "REV")
    c("ZRANGE", "z:r", "1", "2", "REV", "WITHSCORES")
    c("ZRANGE", "z:missing", "0", "-1")
    c("ZRANGE", "z:r", "2", "3", "BYSCORE")
    c("ZRANGE", "z:r", "(2", "+inf", "BYSCORE")
    c("ZRANGE", "z:r", "-inf", "(3", "BYSCORE", "WITHSCORES")
    c("ZRANGE", "z:r", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2")
    c("ZRANGE", "z:r", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "-1")
    c("ZRANGE", "z:r", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", "3")
    c("ZRANGE", "z:r", "3", "1", "BYSCORE")
    c("ZRANGE", "z:r", "notanum", "3", "BYSCORE")
    c("ZADD", "z:lex", "0", "a", "0", "b", "0", "c", "0", "d")
    c("ZRANGE", "z:lex", "-", "+", "BYLEX")
    c("ZRANGE", "z:lex", "[b", "(d", "BYLEX")
    c("ZRANGE", "z:lex", "(a", "[c", "BYLEX")
    c("ZRANGE", "z:lex", "+", "-", "BYLEX", "REV")
    c("ZRANGE", "z:lex", "-", "+", "BYLEX", "LIMIT", "1", "2")
    c("ZRANGE", "z:lex", "b", "d", "BYLEX")
    c("ZRANGE", "z:lex", "-", "+", "BYLEX", "WITHSCORES")
    c("ZRANGE", "z:r", "0", "-1", "LIMIT", "0", "1")
    c("ZRANGE", "z:r", "0", "-1", "BOGUS")
    c("ZRANGEBYSCORE", "z:r", "2", "3")
    c("ZRANGEBYSCORE", "z:r", "(1", "+inf", "WITHSCORES", "LIMIT", "0", "2")
    c("ZREVRANGEBYSCORE", "z:r", "3", "(1")
    c("ZREVRANGEBYSCORE", "z:r", "+inf", "-inf", "LIMIT", "1", "2", "WITHSCORES")
    c("ZRANGEBYLEX", "z:lex", "[a", "(c")
    c("ZREVRANGEBYLEX", "z:lex", "[c", "-")
    c("ZREVRANGE", "z:r", "0", "2", "WITHSCORES")
    c("ZCOUNT", "z:r", "2", "3")
    c("ZCOUNT", "z:r", "(2", "+inf")
    c("ZCOUNT", "z:r", "-inf", "+inf")
    c("ZCOUNT", "z:missing", "0", "10")
    c("ZCOUNT", "z:r", "x", "3")
    c("ZLEXCOUNT", "z:lex", "-", "+")
    c("ZLEXCOUNT", "z:lex", "[b", "(d")
    c("ZLEXCOUNT", "z:lex", "b", "+")

    # ZRANGESTORE, the empty-result delete included.
    c("ZRANGESTORE", "z:dst", "z:r", "0", "2")
    c("ZRANGE", "z:dst", "0", "-1", "WITHSCORES")
    c("ZRANGESTORE", "z:dst", "z:r", "(4", "+inf", "BYSCORE")
    c("TYPE", "z:dst")
    c("ZRANGESTORE", "z:dst2", "z:r", "+inf", "-inf", "BYSCORE", "REV", "LIMIT", "0", "2")
    c("ZRANGE", "z:dst2", "0", "-1")
    c("ZRANGESTORE", "z:dst2", "z:missing", "0", "-1")
    c("TYPE", "z:dst2")

    # Pops, the blocking forms on immediate service only.
    c("ZADD", "z:p", "1", "a", "2", "b", "3", "c", "4", "d")
    c("ZPOPMIN", "z:p")
    c("ZPOPMAX", "z:p")
    c("ZPOPMIN", "z:p", "2")
    c("ZPOPMIN", "z:p", "0")
    c("ZPOPMIN", "z:p", "5")
    c("TYPE", "z:p")
    c("ZPOPMIN", "z:missing")
    c("ZPOPMAX", "z:missing", "3")
    c("ZPOPMIN", "z:k", "-1")
    c("ZPOPMIN", "z:k", "x")
    c("ZADD", "z:p2", "1", "a", "2", "b")
    c("ZMPOP", "2", "z:missing", "z:p2", "MIN")
    c("ZMPOP", "1", "z:p2", "MAX", "COUNT", "5")
    c("TYPE", "z:p2")
    c("ZMPOP", "1", "z:missing", "MIN")
    c("ZMPOP", "0", "MIN")
    c("ZMPOP", "1", "z:k", "BOGUS")
    c("ZMPOP", "x", "z:k", "MIN")
    c("ZMPOP", "1", "z:k", "MIN", "COUNT", "0")
    c("ZADD", "z:b", "1", "a", "2", "b", "3", "c")
    c("BZPOPMIN", "z:b", "0")
    c("BZPOPMAX", "z:missing", "z:b", "0.1")
    c("BZMPOP", "0", "1", "z:b", "MIN")
    c("TYPE", "z:b")
    c("BZPOPMIN", "z:k", "-1")
    c("BZPOPMIN", "z:k", "notanum")
    c("BZMPOP", "0", "0", "MIN")

    # ZRANDMEMBER, deterministic rows only: misses, count 0, and a
    # one-member zset where every draw is forced.
    c("ZRANDMEMBER", "z:missing")
    c("ZRANDMEMBER", "z:missing", "3")
    c("ZADD", "z:one", "7", "solo")
    c("ZRANDMEMBER", "z:one")
    c("ZRANDMEMBER", "z:one", "5")
    c("ZRANDMEMBER", "z:one", "-3")
    c("ZRANDMEMBER", "z:one", "0")
    c("ZRANDMEMBER", "z:one", "2", "WITHSCORES")
    c("ZRANDMEMBER", "z:one", "-2", "WITHSCORES")
    c("ZRANDMEMBER", "z:one", "x")

    # The ZREMRANGE family, whole-window key death included.
    c("ZADD", "z:rr", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
    c("ZREMRANGEBYRANK", "z:rr", "0", "1")
    c("ZRANGE", "z:rr", "0", "-1")
    c("ZREMRANGEBYSCORE", "z:rr", "(3", "+inf")
    c("ZRANGE", "z:rr", "0", "-1", "WITHSCORES")
    c("ZREMRANGEBYRANK", "z:rr", "0", "-1")
    c("TYPE", "z:rr")
    c("ZREMRANGEBYRANK", "z:missing", "0", "-1")
    c("ZREMRANGEBYSCORE", "z:missing", "-inf", "+inf")
    c("ZREMRANGEBYSCORE", "z:k", "x", "1")
    c("ZADD", "z:rl", "0", "a", "0", "b", "0", "c")
    c("ZREMRANGEBYLEX", "z:rl", "[a", "(c")
    c("ZRANGE", "z:rl", "0", "-1")
    c("ZREMRANGEBYLEX", "z:rl", "x", "+")

    # Algebra: WITHSCORES pins the aggregation, sets join at score 1.
    c("ZADD", "z:a1", "1", "a", "2", "b", "3", "c")
    c("ZADD", "z:a2", "10", "b", "20", "c", "30", "d")
    c("ZUNION", "2", "z:a1", "z:a2")
    c("ZUNION", "2", "z:a1", "z:a2", "WITHSCORES")
    c("ZUNION", "2", "z:a1", "z:missing", "WITHSCORES")
    c("ZUNION", "2", "z:a1", "z:a2", "WEIGHTS", "2", "0.5", "WITHSCORES")
    c("ZUNION", "2", "z:a1", "z:a2", "AGGREGATE", "MIN", "WITHSCORES")
    c("ZUNION", "2", "z:a1", "z:a2", "AGGREGATE", "MAX", "WITHSCORES")
    c("ZINTER", "2", "z:a1", "z:a2", "WITHSCORES")
    c("ZINTER", "2", "z:a1", "z:missing")
    c("ZDIFF", "2", "z:a1", "z:a2", "WITHSCORES")
    c("ZDIFF", "2", "z:a1", "z:missing", "WITHSCORES")
    c("ZDIFF", "1", "z:a1")
    c("SADD", "z:s1", "a", "x")
    c("ZUNION", "2", "z:a1", "z:s1", "WITHSCORES")
    c("ZINTER", "2", "z:a1", "z:s1", "WITHSCORES")
    c("ZUNION", "0")
    c("ZUNION", "2", "z:a1")
    c("ZUNION", "x", "z:a1")
    c("ZUNION", "1", "z:a1", "WEIGHTS", "1", "2")
    c("ZUNION", "1", "z:a1", "WEIGHTS", "x")
    c("ZUNION", "1", "z:a1", "AGGREGATE", "BOGUS")
    c("ZINTERCARD", "2", "z:a1", "z:a2")
    c("ZINTERCARD", "2", "z:a1", "z:a2", "LIMIT", "1")
    c("ZINTERCARD", "2", "z:a1", "z:a2", "LIMIT", "0")
    c("ZINTERCARD", "2", "z:a1", "z:a2", "LIMIT", "-1")
    c("ZINTERCARD", "0", "z:a1")

    # The STORE forms, dest overwrite rules included.
    c("ZUNIONSTORE", "z:du", "2", "z:a1", "z:a2")
    c("ZRANGE", "z:du", "0", "-1", "WITHSCORES")
    c("ZUNIONSTORE", "z:du", "2", "z:a1", "z:a2", "WEIGHTS", "0", "1")
    c("ZRANGE", "z:du", "0", "-1", "WITHSCORES")
    c("ZINTERSTORE", "z:di", "2", "z:a1", "z:a2", "AGGREGATE", "MIN")
    c("ZRANGE", "z:di", "0", "-1", "WITHSCORES")
    c("ZDIFFSTORE", "z:dd", "2", "z:a1", "z:a2")
    c("ZRANGE", "z:dd", "0", "-1", "WITHSCORES")
    c("ZINTERSTORE", "z:di", "2", "z:a1", "z:missing")
    c("TYPE", "z:di")
    c("ZADD", "z:ttl", "1", "x")
    c("EXPIRE", "z:ttl", "600")
    c("ZUNIONSTORE", "z:ttl", "1", "z:a1")
    c("TTL", "z:ttl")
    c("SET", "z:sdest", "v")
    c("ZUNIONSTORE", "z:sdest", "1", "z:a1")
    c("TYPE", "z:sdest")

    # ZSCAN: any cursor answers everything on the small tier, and
    # both sides walk it score-sorted, so the multi-member row pins.
    c("ZADD", "z:sc", "1", "only")
    c("ZSCAN", "z:sc", "0")
    c("ZSCAN", "z:sc", "0", "MATCH", "z*")
    c("ZSCAN", "z:sc", "0", "MATCH", "on*")
    c("ZSCAN", "z:sc", "0", "COUNT", "100")
    c("ZSCAN", "z:sc", "42")
    c("ZSCAN", "z:missing", "0")
    c("ZSCAN", "z:sc", "0", "NOVALUES")
    c("ZSCAN", "z:r", "0")

    # The encoding boundary: both sides leave listpack past 128
    # members, the one count threshold the families share, and the
    # order contract makes every reply pin across the crossing.
    c("OBJECT", "ENCODING", "z:r")
    big = ["ZADD", "z:big"]
    for i in range(129):
        big += [str(i), "m%03d" % i]
    c(*big)
    c("OBJECT", "ENCODING", "z:big")
    c("ZCARD", "z:big")
    c("ZRANGE", "z:big", "0", "4", "WITHSCORES")
    c("ZRANGE", "z:big", "-3", "-1")
    c("ZRANK", "z:big", "m064")
    c("ZSCORE", "z:big", "m128")
    c("ZRANGEBYSCORE", "z:big", "126", "+inf")
    # The member-size wall: members over 64 B leave redis's listpack,
    # and 30 of them blow sqlo1's inline byte cap.
    wide = ["ZADD", "z:wide"]
    for i in range(30):
        wide += [str(i), ("w%03d" % i) + "x" * 97]
    c(*wide)
    c("OBJECT", "ENCODING", "z:wide")
    c("ZRANGE", "z:wide", "0", "1")

    # Type walls both ways.
    c("SET", "z:str", "v")
    c("ZADD", "z:str", "1", "m")
    c("ZSCORE", "z:str", "m")
    c("ZMSCORE", "z:str", "m")
    c("ZCARD", "z:str")
    c("ZINCRBY", "z:str", "1", "m")
    c("ZREM", "z:str", "m")
    c("ZRANK", "z:str", "m")
    c("ZRANGE", "z:str", "0", "-1")
    c("ZCOUNT", "z:str", "0", "1")
    c("ZPOPMIN", "z:str")
    c("ZRANDMEMBER", "z:str")
    c("ZSCAN", "z:str", "0")
    c("ZREMRANGEBYRANK", "z:str", "0", "-1")
    c("ZUNION", "2", "z:a1", "z:str")
    c("ZUNIONSTORE", "z:dt", "2", "z:a1", "z:str")
    c("ZRANGESTORE", "z:dt", "z:str", "0", "-1")
    c("GET", "z:a1")

    # ---------------------------------------------------------------
    section("GEO")
    # Geo rides the zset planes: scores are the 52-bit interleaved
    # geohash (Z-I6), so ZSCORE readbacks pin the codec across the
    # family boundary and the STORE forms read back exactly. Search
    # rows always carry a sort or land in a dest zset, since unsorted
    # emission order is the cell walk's, engine-defined on both
    # sides. STOREDIST scores stay unread: full-precision distances
    # carry the libm's last ulp (see the README).

    c("GEOADD", "geo:s", "13.361389", "38.115556", "Palermo")
    c("GEOADD", "geo:s", "15.087269", "37.502669", "Catania", "13.583333", "37.316667", "Agrigento")
    c("ZSCORE", "geo:s", "Palermo")
    c("ZSCORE", "geo:s", "Catania")
    c("ZCARD", "geo:s")
    c("TYPE", "geo:s")
    c("OBJECT", "ENCODING", "geo:s")

    # Flags: NX never moves, CH counts moves, XX moves back.
    c("GEOADD", "geo:s", "NX", "13.5", "38.2", "Palermo")
    c("GEOPOS", "geo:s", "Palermo")
    c("GEOADD", "geo:s", "CH", "13.5", "38.2", "Palermo")
    c("ZSCORE", "geo:s", "Palermo")
    c("GEOADD", "geo:s", "XX", "CH", "13.361389", "38.115556", "Palermo")
    c("GEOADD", "geo:s", "XX", "1", "1", "Fresh")
    c("ZCARD", "geo:s")

    # Grammar and validation: every triple validates before any
    # write.
    c("GEOADD", "geo:s")
    c("GEOADD", "geo:s", "13.36")
    c("GEOADD", "geo:s", "13.36", "38.11")
    c("GEOADD", "geo:s", "NX", "XX", "1", "1", "m")
    c("GEOADD", "geo:s", "181", "0", "m")
    c("GEOADD", "geo:s", "0", "86", "m")
    c("GEOADD", "geo:s", "x", "0", "m")
    c("GEOADD", "geo:s", "1", "1", "ok1", "999", "0", "bad")
    c("ZSCORE", "geo:s", "ok1")

    c("GEOPOS", "geo:s", "Palermo", "ghost", "Catania")
    c("GEOPOS", "geo:missing", "m")
    c("GEOPOS", "geo:s")

    c("GEODIST", "geo:s", "Palermo", "Catania")
    c("GEODIST", "geo:s", "Palermo", "Catania", "km")
    c("GEODIST", "geo:s", "Palermo", "Catania", "ft")
    c("GEODIST", "geo:s", "Palermo", "Catania", "mi")
    c("GEODIST", "geo:s", "Palermo", "Palermo")
    c("GEODIST", "geo:s", "Palermo", "ghost")
    c("GEODIST", "geo:missing", "a", "b")
    c("GEODIST", "geo:s", "Palermo", "Catania", "yd")
    c("GEODIST", "geo:s", "Palermo")

    c("GEOHASH", "geo:s", "Palermo", "ghost", "Catania")
    c("GEOHASH", "geo:missing", "m")
    c("GEOHASH", "geo:s")

    # GEOSEARCH: shapes, reply decorations, COUNT, and FROMMEMBER.
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "WITHCOORD", "WITHDIST", "WITHHASH")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "DESC", "WITHDIST")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "2", "ASC")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "10", "ANY", "ASC")
    c("GEOSEARCH", "geo:s", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "WITHDIST")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYBOX", "400", "400", "km", "ASC", "WITHDIST")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "m", "ASC")
    c("GEOSEARCH", "geo:missing", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")

    # The antimeridian wrap: searches crossing lon 180 return
    # far-side members.
    c("GEOADD", "geo:mer", "179.9", "0", "east", "-179.95", "0.05", "west")
    c("GEOSEARCH", "geo:mer", "FROMLONLAT", "179.9", "0", "BYRADIUS", "50", "km", "ASC", "WITHDIST")
    c("GEOSEARCH", "geo:mer", "FROMLONLAT", "-179.95", "0.1", "BYBOX", "60", "60", "km", "ASC", "WITHDIST")

    # The door table, probed in token order.
    c("GEOSEARCH", "geo:s")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "-1", "km")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "x", "km")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "yd")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYBOX", "10", "-5", "km")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "0")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "x")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ANY")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "FROMMEMBER", "Palermo", "BYRADIUS", "1", "km")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37")
    c("GEOSEARCH", "geo:s", "BYRADIUS", "1", "km")
    c("GEOSEARCH", "geo:s", "FROMMEMBER", "ghost", "BYRADIUS", "1", "km")
    c("GEOSEARCH", "geo:missing", "FROMMEMBER", "ghost", "BYRADIUS", "1", "km")
    c("GEOSEARCH", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "DESC", "WITHDIST")

    # GEOSEARCHSTORE: bits store exactly, dests are score-ordered
    # zsets so no-sort forms still pin, empty results delete.
    c("GEOSEARCHSTORE", "geo:dst", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC")
    c("ZRANGE", "geo:dst", "0", "-1", "WITHSCORES")
    c("GEOSEARCHSTORE", "geo:dst", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "1", "m")
    c("TYPE", "geo:dst")
    c("GEOSEARCHSTORE", "geo:dd", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "2", "ASC", "STOREDIST")
    c("ZCARD", "geo:dd")
    c("ZRANGE", "geo:dd", "0", "-1")
    c("GEOSEARCHSTORE", "geo:dd", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "WITHDIST")
    c("SET", "geo:sdest", "v")
    c("GEOSEARCHSTORE", "geo:sdest", "geo:s", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km")
    c("TYPE", "geo:sdest")

    # The GEORADIUS compat forms, STORE arms and the _RO walls.
    c("GEORADIUS", "geo:s", "15", "37", "200", "km", "ASC", "WITHDIST")
    c("GEORADIUS", "geo:s", "15", "37", "200", "km", "COUNT", "1", "ASC", "WITHCOORD", "WITHHASH")
    c("GEORADIUS_RO", "geo:s", "15", "37", "200", "km", "ASC")
    c("GEORADIUS", "geo:s", "15", "37", "200", "km", "STORE", "geo:rs")
    c("ZRANGE", "geo:rs", "0", "-1", "WITHSCORES")
    c("GEORADIUS", "geo:s", "15", "37", "200", "km", "STOREDIST", "geo:rd")
    c("ZCARD", "geo:rd")
    c("ZRANGE", "geo:rd", "0", "-1")
    c("GEORADIUS", "geo:s", "15", "37", "200", "km", "STORE", "geo:x", "STOREDIST", "geo:y")
    c("TYPE", "geo:x")
    c("TYPE", "geo:y")
    c("GEORADIUS", "geo:s", "15", "37", "200", "km", "WITHDIST", "STORE", "geo:x2")
    c("GEORADIUS_RO", "geo:s", "15", "37", "200", "km", "STORE", "geo:x3")
    c("GEORADIUS", "geo:s", "15", "37")
    c("GEORADIUSBYMEMBER", "geo:s", "Palermo", "250", "km", "ASC", "WITHDIST")
    c("GEORADIUSBYMEMBER", "geo:s", "ghost", "1", "km")
    c("GEORADIUSBYMEMBER_RO", "geo:s", "Palermo", "250", "km", "ASC")
    c("GEORADIUSBYMEMBER_RO", "geo:s", "Palermo", "250", "km", "STOREDIST", "geo:x4")
    c("GEORADIUSBYMEMBER", "geo:s")

    # Type walls.
    c("SET", "geo:str", "v")
    c("GEOADD", "geo:str", "1", "1", "m")
    c("GEOPOS", "geo:str", "m")
    c("GEODIST", "geo:str", "a", "b")
    c("GEOHASH", "geo:str", "m")
    c("GEOSEARCH", "geo:str", "FROMLONLAT", "1", "1", "BYRADIUS", "1", "km")
    c("GEORADIUS", "geo:str", "1", "1", "1", "km")

    # ---------------------------------------------------------------
    section("LIST")
    # Lists are ordered on both sides at every tier, so multi-element
    # replies pin byte for byte throughout, like zsets. The encoding
    # boundary differs: redis 8.8's default list-max-listpack-size -2
    # is a pure byte cap of 8 KiB with no entry-count wall, while
    # sqlo1 goes noded past 128 entries or 2 KiB inline. The fixture
    # crosses through the byte wall both sides share (a wide push
    # over 8 KiB) and does not assert encoding between 129 entries
    # and 8 KiB, where the representations genuinely diverge.

    # Deque surface: push counts, LPUSH reversal, the X-variants that
    # refuse to create.
    c("RPUSH", "l:k", "a", "b", "c")
    c("LPUSH", "l:k", "x", "y")
    c("LRANGE", "l:k", "0", "-1")
    c("LLEN", "l:k")
    c("TYPE", "l:k")
    c("OBJECT", "ENCODING", "l:k")
    c("LPUSHX", "l:k", "z")
    c("RPUSHX", "l:k", "w")
    c("LRANGE", "l:k", "0", "-1")
    c("LPUSHX", "l:missing", "v")
    c("RPUSHX", "l:missing", "v")
    c("TYPE", "l:missing")
    c("LLEN", "l:missing")
    c("RPUSH", "l:k")
    c("LPUSH")

    # Pops: plain, counted, count 0, overpop to key death, both miss
    # shapes (nil bulk plain, null array counted), the count doors.
    c("LPOP", "l:k")
    c("RPOP", "l:k")
    c("LPOP", "l:k", "2")
    c("RPOP", "l:k", "0")
    c("RPOP", "l:k", "10")
    c("TYPE", "l:k")
    c("LPOP", "l:missing")
    c("RPOP", "l:missing")
    c("LPOP", "l:missing", "3")
    c("LPOP", "l:missing", "-1")
    c("RPOP", "l:missing", "x")

    # Positional surface: LINDEX both signs and both out-of-range
    # walls, LSET in place, LRANGE's window grammar.
    c("RPUSH", "l:p", "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9")
    c("LINDEX", "l:p", "0")
    c("LINDEX", "l:p", "5")
    c("LINDEX", "l:p", "-1")
    c("LINDEX", "l:p", "-10")
    c("LINDEX", "l:p", "10")
    c("LINDEX", "l:p", "-11")
    c("LINDEX", "l:missing", "0")
    c("LINDEX", "l:p", "x")
    c("LSET", "l:p", "0", "E0")
    c("LSET", "l:p", "-1", "E9")
    c("LSET", "l:p", "4", "mid")
    c("LRANGE", "l:p", "0", "-1")
    c("LSET", "l:p", "10", "v")
    c("LSET", "l:missing", "0", "v")
    c("LRANGE", "l:p", "2", "5")
    c("LRANGE", "l:p", "-3", "-1")
    c("LRANGE", "l:p", "-100", "100")
    c("LRANGE", "l:p", "5", "2")
    c("LRANGE", "l:p", "3", "3")
    c("LRANGE", "l:p", "-1", "-3")
    c("LRANGE", "l:missing", "0", "-1")

    # LTRIM: interior window, identity, empty window kills the key,
    # missing key is OK.
    c("RPUSH", "l:t", "t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9")
    c("LTRIM", "l:t", "2", "-3")
    c("LRANGE", "l:t", "0", "-1")
    c("LTRIM", "l:t", "0", "-1")
    c("LLEN", "l:t")
    c("LTRIM", "l:t", "5", "1")
    c("TYPE", "l:t")
    c("LTRIM", "l:missing", "0", "-1")
    c("LTRIM", "l:p", "x", "1")

    # LINSERT: both sides of the first pivot hit, the
    # case-insensitive token, the three miss shapes.
    c("RPUSH", "l:s", "a", "b", "c", "b", "a")
    c("LINSERT", "l:s", "BEFORE", "b", "B1")
    c("LINSERT", "l:s", "AFTER", "c", "C1")
    c("LRANGE", "l:s", "0", "-1")
    c("LINSERT", "l:s", "before", "a", "A1")
    c("LRANGE", "l:s", "0", "-1")
    c("LINSERT", "l:s", "BEFORE", "ghost", "x")
    c("LINSERT", "l:missing", "BEFORE", "a", "x")
    c("LINSERT", "l:s", "SIDEWAYS", "a", "x")
    c("LLEN", "l:s")

    # LREM: the count grammar forward, backward, and remove-all,
    # misses stay read-only, removing everything kills the key.
    c("RPUSH", "l:r", "d", "z", "d", "z", "d", "z", "d")
    c("LREM", "l:r", "2", "d")
    c("LRANGE", "l:r", "0", "-1")
    c("LREM", "l:r", "-1", "z")
    c("LRANGE", "l:r", "0", "-1")
    c("LREM", "l:r", "0", "z")
    c("LRANGE", "l:r", "0", "-1")
    c("LREM", "l:r", "5", "ghost")
    c("LREM", "l:missing", "0", "x")
    c("LREM", "l:r", "0", "d")
    c("TYPE", "l:r")
    c("LREM", "l:s", "x", "a")

    # LPOS: the RANK/COUNT/MAXLEN grammar and its option errors.
    c("RPUSH", "l:q", "a", "b", "c", "a", "b", "c", "a")
    c("LPOS", "l:q", "a")
    c("LPOS", "l:q", "a", "RANK", "2")
    c("LPOS", "l:q", "a", "RANK", "-1")
    c("LPOS", "l:q", "a", "COUNT", "2")
    c("LPOS", "l:q", "a", "COUNT", "0")
    c("LPOS", "l:q", "a", "RANK", "-1", "COUNT", "0")
    c("LPOS", "l:q", "c", "MAXLEN", "2")
    c("LPOS", "l:q", "c", "MAXLEN", "3")
    c("LPOS", "l:q", "ghost")
    c("LPOS", "l:q", "ghost", "COUNT", "2")
    c("LPOS", "l:missing", "a")
    c("LPOS", "l:missing", "a", "COUNT", "1")
    c("LPOS", "l:q", "a", "RANK", "0")
    c("LPOS", "l:q", "a", "COUNT", "-1")
    c("LPOS", "l:q", "a", "MAXLEN", "-1")
    c("LPOS", "l:q", "a", "BOGUS", "1")

    # LMOVE and RPOPLPUSH: all four end pairs, the same-key rotation,
    # the same-end identity, a single-element move killing its
    # source, and the direction door.
    c("RPUSH", "l:mv", "one", "two", "three")
    c("LMOVE", "l:mv", "l:mvd", "LEFT", "RIGHT")
    c("LMOVE", "l:mv", "l:mvd", "RIGHT", "LEFT")
    c("LRANGE", "l:mv", "0", "-1")
    c("LRANGE", "l:mvd", "0", "-1")
    c("LMOVE", "l:mvd", "l:mvd", "LEFT", "RIGHT")
    c("LRANGE", "l:mvd", "0", "-1")
    c("LMOVE", "l:mvd", "l:mvd", "LEFT", "LEFT")
    c("LRANGE", "l:mvd", "0", "-1")
    c("LMOVE", "l:missing", "l:mvd", "LEFT", "LEFT")
    c("LMOVE", "l:mvd", "l:mv2", "UP", "LEFT")
    c("RPOPLPUSH", "l:mvd", "l:mv2")
    c("LRANGE", "l:mv2", "0", "-1")
    c("RPOPLPUSH", "l:missing", "l:mv2")
    c("RPUSH", "l:single", "solo")
    c("LMOVE", "l:single", "l:mvd", "LEFT", "RIGHT")
    c("TYPE", "l:single")

    # LMPOP: first non-empty key wins, COUNT and overpop, key death,
    # the numkeys and direction doors.
    c("RPUSH", "l:m1", "a", "b", "c")
    c("LMPOP", "2", "l:missing", "l:m1", "LEFT")
    c("LMPOP", "1", "l:m1", "RIGHT", "COUNT", "5")
    c("TYPE", "l:m1")
    c("LMPOP", "1", "l:missing", "LEFT")
    c("LMPOP", "0", "LEFT")
    c("LMPOP", "1", "l:missing", "BOGUS")
    c("LMPOP", "x", "l:missing", "LEFT")
    c("LMPOP", "1", "l:missing", "LEFT", "COUNT", "0")
    c("LMPOP", "2", "l:missing", "LEFT")

    # The blocking forms on immediate service only, plus the timeout
    # doors that answer before blocking.
    c("RPUSH", "l:b", "a", "b", "c", "d", "e", "f")
    c("BLPOP", "l:b", "0")
    c("BRPOP", "l:missing", "l:b", "0.1")
    c("BLMPOP", "0", "1", "l:b", "LEFT")
    c("BLMOVE", "l:b", "l:bd", "LEFT", "RIGHT", "0")
    c("BRPOPLPUSH", "l:b", "l:bd", "0.1")
    c("LRANGE", "l:bd", "0", "-1")
    c("LRANGE", "l:b", "0", "-1")
    c("BLPOP", "l:b", "-1")
    c("BLPOP", "l:b", "notanum")
    c("BLMOVE", "l:b", "l:bd", "LEFT", "RIGHT", "-1")
    c("BLMPOP", "0", "0", "LEFT")

    # The encoding walls both sides share: 128 small entries stay
    # listpack, and a 300-element push of 40 B values (12 KiB, past
    # redis's byte cap and sqlo1's count cap) is quicklist. The noded
    # tier answers the same windows the inline tier did, LSET, LREM,
    # and LTRIM included.
    l128 = []
    for i in range(128):
        l128 += ["s%03d" % i]
    c("RPUSH", "l:l128", *l128)
    c("OBJECT", "ENCODING", "l:l128")
    c("LLEN", "l:l128")
    big = []
    for i in range(300):
        big += [("v%03d" % i).ljust(40, "x")]
    c("RPUSH", "l:big", *big)
    c("OBJECT", "ENCODING", "l:big")
    c("LLEN", "l:big")
    c("LRANGE", "l:big", "0", "4")
    c("LRANGE", "l:big", "-5", "-1")
    c("LRANGE", "l:big", "120", "135")
    c("LINDEX", "l:big", "129")
    c("LINDEX", "l:big", "-130")
    c("LSET", "l:big", "200", "SWAP")
    c("LINDEX", "l:big", "200")
    c("LPOS", "l:big", ("v250").ljust(40, "x"), "RANK", "-1")
    c("LREM", "l:big", "0", ("v250").ljust(40, "x"))
    c("LLEN", "l:big")
    c("LTRIM", "l:big", "10", "-11")
    c("LLEN", "l:big")
    c("LRANGE", "l:big", "0", "2")
    c("LRANGE", "l:big", "-3", "-1")

    # Type walls: every list command against a string key, and a
    # wrong-typed move destination leaving the source untouched.
    c("SET", "l:str", "v")
    c("LPUSH", "l:str", "x")
    c("RPUSHX", "l:str", "x")
    c("LPOP", "l:str")
    c("RPOP", "l:str", "2")
    c("LLEN", "l:str")
    c("LINDEX", "l:str", "0")
    c("LSET", "l:str", "0", "v")
    c("LRANGE", "l:str", "0", "-1")
    c("LTRIM", "l:str", "0", "-1")
    c("LINSERT", "l:str", "BEFORE", "a", "b")
    c("LREM", "l:str", "0", "a")
    c("LPOS", "l:str", "a")
    c("LMOVE", "l:str", "l:d", "LEFT", "LEFT")
    c("RPUSH", "l:src", "keepme")
    c("LMOVE", "l:src", "l:str", "LEFT", "LEFT")
    c("LRANGE", "l:src", "0", "-1")
    c("LMPOP", "2", "l:str", "l:src", "LEFT")
    c("BLPOP", "l:str", "0")

    # ---------------------------------------------------------------
    section("STREAM")
    # Streams pin on explicit IDs only: an auto ID carries the wall
    # clock and can never replay. Replies that carry wall-clock or
    # geometry fields stay out too and live in the wire tests
    # instead: XINFO STREAM renders the radix-tree fields (sqlo1
    # reports its own run geometry there), XINFO CONSUMERS and XINFO
    # STREAM FULL carry idle and delivery times, and the XPENDING
    # extended form renders per-entry idle ms. XTRIM's ~ variant cuts
    # at node boundaries whose geometry is engine-defined, so only
    # exact trims pin; the ~ grammar doors still do.

    # XADD: explicit IDs, bare ms as ms-0, ms-* sequence fill, the
    # ordering refusals, the 0-0 floor, NOMKSTREAM, arity and field
    # pairing doors.
    c("XADD", "x:k", "1-1", "f", "v")
    c("XADD", "x:k", "1-2", "a", "1", "b", "2")
    c("XADD", "x:k", "2", "f", "v")
    c("XADD", "x:k", "2-*", "f", "v")
    c("XADD", "x:k", "1-5", "f", "v")
    c("XADD", "x:k", "2-0", "f", "v")
    c("XADD", "x:k", "0-0", "f", "v")
    c("XADD", "x:floor", "0-1", "f", "v")
    c("XADD", "x:k", "notanid", "f", "v")
    c("XADD", "x:k", "3-1", "f")
    c("XADD", "x:k")
    c("XADD", "x:missing", "NOMKSTREAM", "1-1", "f", "v")
    c("TYPE", "x:missing")
    c("XADD", "x:k", "NOMKSTREAM", "3-1", "f", "v")
    c("XLEN", "x:k")
    c("TYPE", "x:k")
    c("OBJECT", "ENCODING", "x:k")
    c("XLEN", "x:missing")
    c("XLEN")

    # XRANGE and XREVRANGE: full window, explicit and bare-ms bounds,
    # exclusive bounds, COUNT, the empty window, the miss, and the
    # bound grammar doors.
    for xid in ["1-1", "2-1", "2-2", "3-1", "4-1", "5-1"]:
        c("XADD", "x:r", xid, "i", xid)
    c("XRANGE", "x:r", "-", "+")
    c("XRANGE", "x:r", "-", "+", "COUNT", "2")
    c("XRANGE", "x:r", "2-1", "4-1")
    c("XRANGE", "x:r", "2", "4")
    c("XRANGE", "x:r", "(2-1", "(5-1")
    c("XRANGE", "x:r", "(2", "+")
    c("XRANGE", "x:r", "3-1", "3-1")
    c("XRANGE", "x:r", "+", "-")
    c("XRANGE", "x:missing", "-", "+")
    c("XREVRANGE", "x:r", "+", "-")
    c("XREVRANGE", "x:r", "+", "-", "COUNT", "2")
    c("XREVRANGE", "x:r", "4-1", "2-1")
    c("XREVRANGE", "x:r", "(5-1", "(2-1")
    c("XREVRANGE", "x:r", "-", "+")
    c("XREVRANGE", "x:missing", "+", "-")
    c("XRANGE", "x:r", "notanid", "+")
    c("XRANGE", "x:r", "(-", "+")
    c("XRANGE", "x:r", "-", "+", "COUNT", "x")
    c("XRANGE", "x:r", "-")

    # Trims: exact MAXLEN and MINID through XTRIM and the XADD arms,
    # trim to empty keeps the key, the LIMIT-needs-~ door and the
    # option grammar.
    for i in range(1, 11):
        c("XADD", "x:t", "%d-1" % i, "f", "v")
    c("XTRIM", "x:t", "MAXLEN", "7")
    c("XLEN", "x:t")
    c("XRANGE", "x:t", "-", "+", "COUNT", "1")
    c("XTRIM", "x:t", "MINID", "6")
    c("XRANGE", "x:t", "-", "+", "COUNT", "1")
    c("XTRIM", "x:t", "MINID", "8-1")
    c("XTRIM", "x:t", "MAXLEN", "100")
    c("XTRIM", "x:t", "MAXLEN", "0")
    c("XLEN", "x:t")
    c("TYPE", "x:t")
    c("XADD", "x:t2", "MAXLEN", "3", "5-1", "f", "v")
    c("XADD", "x:t2", "MAXLEN", "3", "6-1", "f", "v")
    c("XADD", "x:t2", "MAXLEN", "3", "7-1", "f", "v")
    c("XADD", "x:t2", "MAXLEN", "3", "8-1", "f", "v")
    c("XLEN", "x:t2")
    c("XRANGE", "x:t2", "-", "+", "COUNT", "1")
    c("XADD", "x:t2", "MINID", "7", "9-1", "f", "v")
    c("XRANGE", "x:t2", "-", "+", "COUNT", "1")
    c("XTRIM", "x:missing", "MAXLEN", "5")
    c("XTRIM", "x:t", "MAXLEN", "notanum")
    c("XTRIM", "x:t", "MINID", "notanid")
    c("XTRIM", "x:t", "MAXLEN", "5", "LIMIT", "10")
    c("XTRIM", "x:t", "BOGUS", "5")
    c("XTRIM", "x:t")

    # XSETID: forward and equal moves, the smaller-than-top refusal,
    # the missing-key refusal, the option arms, and generation
    # resuming above the moved ID.
    c("XADD", "x:sid", "3-3", "f", "v")
    c("XSETID", "x:sid", "5-5")
    c("XADD", "x:sid", "5-5", "f", "v")
    c("XADD", "x:sid", "5-6", "f", "v")
    c("XSETID", "x:sid", "2-2")
    c("XSETID", "x:sid", "9-9", "ENTRIESADDED", "42", "MAXDELETEDID", "4-4")
    c("XSETID", "x:missing", "1-1")
    c("XSETID", "x:sid", "notanid")
    c("XSETID", "x:sid")

    # XDEL: the key check precedes the ID parse, one bad ID aborts
    # the call, duplicates count once, a bare ms misses, and a stream
    # deleted to empty keeps its key and its last generated ID.
    for xid in ["1-1", "2-2", "3-3"]:
        c("XADD", "x:d", xid, "f", "v")
    c("XDEL", "x:missing", "notanid")
    c("XDEL", "x:d", "notanid")
    c("XDEL", "x:d", "1-1", "notanid")
    c("XLEN", "x:d")
    c("XDEL", "x:d", "1-1", "1-1", "9-9")
    c("XLEN", "x:d")
    c("XDEL", "x:d", "2")
    c("XDEL", "x:d", "3-3", "2-2")
    c("XLEN", "x:d")
    c("TYPE", "x:d")
    c("XADD", "x:d", "3-3", "f", "v")
    c("XADD", "x:d", "3-4", "f", "v")
    c("XDEL", "x:d")

    # Groups: create with and without MKSTREAM, BUSYGROUP, SETID,
    # consumer create and delete, destroy, and the subcommand doors.
    c("XADD", "x:g", "1-1", "f", "a")
    c("XADD", "x:g", "2-1", "f", "b")
    c("XADD", "x:g", "3-1", "f", "c")
    c("XADD", "x:g", "4-1", "f", "d")
    c("XGROUP", "CREATE", "x:g", "grp", "0")
    c("XGROUP", "CREATE", "x:g", "grp", "0")
    c("XGROUP", "CREATE", "x:missing", "grp", "0")
    c("XGROUP", "CREATE", "x:mk", "grp", "$", "MKSTREAM")
    c("TYPE", "x:mk")
    c("XLEN", "x:mk")
    c("XGROUP", "CREATECONSUMER", "x:g", "grp", "c1")
    c("XGROUP", "CREATECONSUMER", "x:g", "grp", "c1")
    c("XGROUP", "CREATECONSUMER", "x:g", "nogrp", "c1")
    c("XGROUP", "DELCONSUMER", "x:g", "grp", "c1")
    c("XGROUP", "DELCONSUMER", "x:g", "grp", "ghost")
    c("XGROUP", "SETID", "x:g", "grp", "2-1")
    c("XGROUP", "SETID", "x:g", "grp", "0")
    c("XGROUP", "SETID", "x:g", "nogrp", "0")
    c("XGROUP", "DESTROY", "x:mk", "grp")
    c("XGROUP", "DESTROY", "x:mk", "grp")
    c("XGROUP", "BOGUS", "x:g", "grp")
    c("XGROUP", "CREATE", "x:g", "grp2", "notanid")

    # Delivery and acks: > against history, COUNT, NOACK, the
    # XPENDING summary form, ack idempotence, and the NOGROUP walls.
    c("XREADGROUP", "GROUP", "grp", "c1", "COUNT", "2", "STREAMS", "x:g", ">")
    c("XREADGROUP", "GROUP", "grp", "c2", "STREAMS", "x:g", ">")
    c("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "x:g", ">")
    c("XPENDING", "x:g", "grp")
    c("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "x:g", "0")
    c("XREADGROUP", "GROUP", "grp", "c1", "COUNT", "1", "STREAMS", "x:g", "3-1")
    c("XACK", "x:g", "grp", "1-1")
    c("XACK", "x:g", "grp", "1-1")
    c("XACK", "x:g", "grp", "9-9")
    c("XACK", "x:g", "grp", "2-1", "3-1")
    c("XPENDING", "x:g", "grp")
    c("XACK", "x:g", "nogrp", "1-1")
    c("XACK", "x:missing", "grp", "1-1")
    c("XADD", "x:g", "5-1", "f", "e")
    c("XREADGROUP", "GROUP", "grp", "c3", "NOACK", "STREAMS", "x:g", ">")
    c("XPENDING", "x:g", "grp")
    c("XREADGROUP", "GROUP", "grp", "c3", "STREAMS", "x:g", ">")
    c("XREADGROUP", "GROUP", "nogrp", "c1", "STREAMS", "x:g", ">")
    c("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "x:g", "$")
    c("XREADGROUP", "GROUP", "grp", "c1", "STREAMS", "x:g", "x:g", ">")
    c("XREADGROUP", "STREAMS", "x:g", ">")
    c("XREADGROUP", "GROUP", "grp", "c1")

    # Claims: XCLAIM entry and JUSTID forms at idle 0, the silent
    # skip of unknown IDs, FORCE minting, XAUTOCLAIM's cursor walk
    # and its deleted-entry drain, and the numeric doors.
    c("XPENDING", "x:g", "grp")
    c("XCLAIM", "x:g", "grp", "c9", "0", "4-1")
    c("XCLAIM", "x:g", "grp", "c9", "0", "4-1", "JUSTID")
    c("XCLAIM", "x:g", "grp", "c9", "0", "9-9")
    c("XCLAIM", "x:g", "grp", "c9", "0", "2-1", "FORCE", "JUSTID")
    c("XPENDING", "x:g", "grp")
    c("XCLAIM", "x:g", "nogrp", "c9", "0", "4-1")
    c("XCLAIM", "x:missing", "grp", "c9", "0", "4-1")
    c("XCLAIM", "x:g", "grp", "c9", "notanum", "4-1")
    c("XCLAIM", "x:g", "grp", "c9", "0", "notanid")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "0", "0")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "0", "0", "COUNT", "1", "JUSTID")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "0", "5-0")
    c("XDEL", "x:g", "2-1")
    c("XAUTOCLAIM", "x:g", "grp", "ac2", "0", "0")
    c("XPENDING", "x:g", "grp")
    c("XAUTOCLAIM", "x:g", "nogrp", "ac", "0", "0")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "0", "0", "COUNT", "0")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "notanum", "0")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "0", "notanid")
    c("XAUTOCLAIM", "x:g", "grp", "ac", "0")

    # XINFO GROUPS carries only counts, IDs, and lag, so it pins;
    # destroy sweeps the group and its pending state.
    c("XINFO", "GROUPS", "x:g")
    c("XINFO", "GROUPS", "x:missing")
    c("XGROUP", "DESTROY", "x:g", "grp")
    c("XINFO", "GROUPS", "x:g")
    c("XGROUP", "CREATE", "x:g", "grp", "0")
    c("XPENDING", "x:g", "grp")
    c("XGROUP", "DESTROY", "x:g", "grp")

    # XREAD: non-blocking reads, multi-stream with a miss, the frozen
    # $ answering nothing new, the + newest-entry form, immediate
    # service under BLOCK, and the grammar doors.
    c("XREAD", "COUNT", "2", "STREAMS", "x:r", "0")
    c("XREAD", "STREAMS", "x:r", "3-1")
    c("XREAD", "STREAMS", "x:r", "x:k", "0", "0")
    c("XREAD", "STREAMS", "x:r", "x:missing", "0", "0")
    c("XREAD", "STREAMS", "x:missing", "0")
    c("XREAD", "STREAMS", "x:r", "$")
    c("XREAD", "STREAMS", "x:r", "+")
    c("XREAD", "BLOCK", "0", "STREAMS", "x:r", "0")
    c("XREAD", "BLOCK", "notanum", "STREAMS", "x:r", "0")
    c("XREAD", "BLOCK", "-1", "STREAMS", "x:r", "0")
    c("XREAD", "STREAMS", "x:r", "x:k", "0")
    c("XREAD", "STREAMS")
    c("XREAD", "COUNT", "x", "STREAMS", "x:r", "0")

    # Type walls: every stream command against a string key.
    c("SET", "x:str", "v")
    c("XADD", "x:str", "1-1", "f", "v")
    c("XLEN", "x:str")
    c("XRANGE", "x:str", "-", "+")
    c("XREVRANGE", "x:str", "+", "-")
    c("XTRIM", "x:str", "MAXLEN", "5")
    c("XSETID", "x:str", "1-1")
    c("XDEL", "x:str", "1-1")
    c("XGROUP", "CREATE", "x:str", "g", "0")
    c("XREADGROUP", "GROUP", "g", "c", "STREAMS", "x:str", ">")
    c("XACK", "x:str", "g", "1-1")
    c("XPENDING", "x:str", "g")
    c("XCLAIM", "x:str", "g", "c", "0", "1-1")
    c("XAUTOCLAIM", "x:str", "g", "c", "0", "0")
    c("XINFO", "GROUPS", "x:str")
    c("XREAD", "STREAMS", "x:str", "0")

    # ---------------------------------------------------------------
    section("EXPIRY")

    # The replay server runs on a frozen clock (epoch second 1000), so
    # absolute stamps here are either tiny, past on both sides, or far
    # future for both: above the 2026 wall clock and below sqlo1's
    # 42-bit header horizon in May 2109 (see the README divergence).
    # Relative rows only read back through whole-second TTL, which is
    # stable within the second on the live server and exact on the
    # frozen one.

    # The whole read family: missing key, then a fresh persistent key.
    c("TTL", "e:missing")
    c("PTTL", "e:missing")
    c("EXPIRETIME", "e:missing")
    c("PEXPIRETIME", "e:missing")
    c("PERSIST", "e:missing")
    c("SET", "e:k", "v")
    c("TTL", "e:k")
    c("PTTL", "e:k")
    c("EXPIRETIME", "e:k")
    c("PEXPIRETIME", "e:k")

    # The set family answers 0 on a missing key without minting it.
    c("EXPIRE", "e:nokey", "100")
    c("PEXPIRE", "e:nokey", "100000")
    c("EXPIREAT", "e:nokey", "4000000000")
    c("PEXPIREAT", "e:nokey", "4000000000000")
    c("GET", "e:nokey")

    # Relative sets read back through TTL in the same second, and the
    # fractional row pins the round-to-nearest reply: 200ms of extra
    # life does not reach the next second.
    c("EXPIRE", "e:k", "100")
    c("TTL", "e:k")
    c("PEXPIRE", "e:k", "200000")
    c("TTL", "e:k")
    c("PEXPIRE", "e:k", "100200")
    c("TTL", "e:k")

    # Absolute stamps read back exactly, seconds truncating from ms.
    c("EXPIREAT", "e:k", "4000000000")
    c("EXPIRETIME", "e:k")
    c("PEXPIRETIME", "e:k")
    c("PEXPIREAT", "e:k", "4000000000500")
    c("PEXPIRETIME", "e:k")
    c("EXPIRETIME", "e:k")

    # The condition table over absolute stamps: a persistent key reads
    # as infinite, so XX and GT refuse it and LT and NX take it.
    c("SET", "e:c", "v")
    c("EXPIREAT", "e:c", "4000000000", "XX")
    c("EXPIREAT", "e:c", "4000000000", "GT")
    c("EXPIREAT", "e:c", "4000000000", "LT")
    c("EXPIRETIME", "e:c")
    c("EXPIREAT", "e:c", "3000000000", "NX")
    c("EXPIREAT", "e:c", "3000000000", "GT")
    c("EXPIRETIME", "e:c")
    c("EXPIREAT", "e:c", "3000000000", "LT")
    c("EXPIRETIME", "e:c")
    c("EXPIREAT", "e:c", "3500000000", "GT")
    c("EXPIRETIME", "e:c")
    c("EXPIREAT", "e:c", "3500000000", "GT")
    c("EXPIREAT", "e:c", "3500000000", "LT")
    c("EXPIREAT", "e:c", "3600000000", "XX")
    c("EXPIRETIME", "e:c")
    c("PERSIST", "e:c")
    c("EXPIREAT", "e:c", "3800000000", "NX")
    c("EXPIRETIME", "e:c")
    # A repeated flag is the same gate twice, not a syntax error, and
    # the ms form gates against the same stamp cross-unit.
    c("EXPIREAT", "e:c", "3900000000", "GT", "GT")
    c("EXPIRETIME", "e:c")
    c("PEXPIREAT", "e:c", "3900000000500", "GT")
    c("PEXPIRETIME", "e:c")
    c("EXPIRETIME", "e:c")
    # A relative EXPIRE under an XX GT pair loses to the far stamp.
    c("EXPIRE", "e:c", "100", "XX", "GT")
    c("PEXPIRETIME", "e:c")

    # The flag doors.
    c("EXPIRE", "e:c", "100", "NX", "XX")
    c("EXPIRE", "e:c", "100", "NX", "GT")
    c("EXPIRE", "e:c", "100", "NX", "LT")
    c("EXPIRE", "e:c", "100", "GT", "LT")
    c("EXPIRE", "e:c", "100", "LT", "GT")
    c("EXPIRE", "e:c", "100", "BOGUS")
    c("EXPIRE", "e:c", "notanum")
    c("EXPIRE", "e:c")
    c("EXPIRE")
    c("TTL")
    c("TTL", "e:c", "extra")
    c("PERSIST")
    c("EXPIRETIME")

    # The overflow doors: not-an-integer past int64, and invalid
    # expire when the unit multiply or the now-add overflows.
    c("EXPIRE", "e:c", "99999999999999999999")
    c("EXPIRE", "e:c", "9999999999999999")
    c("PEXPIRE", "e:c", "9223372036854775807")
    c("EXPIREAT", "e:c", "9223372036854775807")
    c("PEXPIREAT", "e:c", "-9223372036854775808")

    # A past deadline deletes and answers 1, in every unit shape.
    c("SET", "e:d1", "v")
    c("EXPIREAT", "e:d1", "1")
    c("GET", "e:d1")
    c("TTL", "e:d1")
    c("SET", "e:d2", "v")
    c("PEXPIRE", "e:d2", "-1")
    c("GET", "e:d2")
    c("SET", "e:d3", "v")
    c("EXPIRE", "e:d3", "0")
    c("GET", "e:d3")
    c("SET", "e:d4", "v")
    c("PEXPIREAT", "e:d4", "1")
    c("GET", "e:d4")
    # The gates run before the delete, so a refused past deadline
    # leaves the key alone and LT still fires on a persistent key.
    c("SET", "e:d5", "v")
    c("EXPIRE", "e:d5", "-5", "XX")
    c("GET", "e:d5")
    c("EXPIRE", "e:d5", "-5", "GT")
    c("GET", "e:d5")
    c("EXPIRE", "e:d5", "-5", "LT")
    c("GET", "e:d5")

    # PERSIST walks 0 without TTL, 1 dropping one, 0 again.
    c("SET", "e:p", "v")
    c("PERSIST", "e:p")
    c("EXPIRE", "e:p", "100")
    c("TTL", "e:p")
    c("PERSIST", "e:p")
    c("TTL", "e:p")
    c("PERSIST", "e:p")

    # Expiry is type-blind: collections take the same surface.
    c("RPUSH", "e:l", "a")
    c("EXPIRE", "e:l", "100")
    c("TTL", "e:l")
    c("HSET", "e:h", "f", "v")
    c("PEXPIREAT", "e:h", "4000000000000")
    c("PEXPIRETIME", "e:h")
    c("PERSIST", "e:h")
    c("PEXPIRETIME", "e:h")

    # DBSIZE over the whole fixture keyspace: the count only matches
    # if every key-death row above agreed, so this is the cross-check
    # that both sides ended the run with the same keys.
    c("DBSIZE")
    c("DBSIZE", "extra")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
