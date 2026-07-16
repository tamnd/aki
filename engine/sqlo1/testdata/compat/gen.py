#!/usr/bin/env python3
"""Generate fixtures.txt: the T1 compat sections, every STRING,
BITMAP, and HLL manifest row from spec doc 12 exercised against a real
redis-server and recorded reply by reply. The Go test replays the file
through the sqlo1 dispatch path, so each fixture line is one diffed
manifest row: same args, same wire reply, error texts included.

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

    print("\n".join(lines))


if __name__ == "__main__":
    main()
