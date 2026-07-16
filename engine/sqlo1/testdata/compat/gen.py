#!/usr/bin/env python3
"""Generate fixtures.txt: the compat sections, every STRING, BITMAP,
HLL, HASH, and SET manifest row from spec doc 12 exercised against a
real redis-server and recorded reply by reply. The Go test replays the
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

    print("\n".join(lines))


if __name__ == "__main__":
    main()
