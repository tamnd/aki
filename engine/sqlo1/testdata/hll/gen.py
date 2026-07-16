#!/usr/bin/env python3
"""Generate fixtures.txt by replaying deterministic PF command
sequences against a real redis-server and recording every reply and
value snapshot. The Go test replays the same file against the sqlo1
server, so the fixture pins byte parity of the HYLL envelope, the
estimator (through the cached cardinality PFCOUNT writes back), and
every error text.

Usage: start a throwaway server, then run this script.

  redis-server --port 7399 --save '' --appendonly no --daemonize no &
  python3 gen.py 7399 > fixtures.txt

Generated against Redis 8.8.0. Line format:
  C <json array of args> -> <json string of the reply>
  V <key> <hex of the value bytes>
Replies encode as the RESP first byte plus payload: ":n", "+text",
"-error", or "$" plus the bulk payload ("$-1" for a null bulk).
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
                a = a.encode()
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
            return [self.reply() for _ in range(n)]
        raise RuntimeError("unexpected reply " + repr(line))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7399
    r = R(port)
    lines = []

    def c(*args):
        rep = r.cmd(*args)
        assert isinstance(rep, str), (args, rep)
        lines.append("C %s -> %s" % (json.dumps(list(args)), json.dumps(rep)))
        return rep

    def v(key):
        rep = r.cmd("GET", key)
        assert rep.startswith("$") and rep != "$-1", (key, rep)
        raw = rep[1:].encode("latin-1")
        lines.append("V %s %s" % (key, raw.hex()))

    r.cmd("FLUSHALL")

    # A bare create: PFADD with no elements makes the key and counts
    # as an update.
    c("PFADD", "hll:empty")
    v("hll:empty")
    c("PFADD", "hll:empty")
    v("hll:empty")
    c("PFCOUNT", "hll:empty")
    v("hll:empty")

    # Small sparse: one batch add, count (writes the cache back),
    # then the debug views.
    c("PFADD", "hll:a", *["e%d" % i for i in range(20)])
    v("hll:a")
    c("PFCOUNT", "hll:a")
    v("hll:a")
    c("PFDEBUG", "ENCODING", "hll:a")
    c("PFDEBUG", "DECODE", "hll:a")
    c("PFADD", "hll:a")
    c("PFADD", "hll:a", "e0", "e1")

    # One-at-a-time adds exercise the in-place upgrade, splice, and
    # merge rules far harder than one batch.
    for i in range(64):
        c("PFADD", "hll:inc", "x%d" % i)
    v("hll:inc")
    for i in range(64):
        c("PFADD", "hll:inc", "x%d" % i)
    v("hll:inc")

    # Mid-size sparse, still under the promotion ceiling.
    c("PFADD", "hll:b", *["b%d" % i for i in range(200)])
    v("hll:b")
    c("PFCOUNT", "hll:b")
    v("hll:b")
    c("PFDEBUG", "ENCODING", "hll:b")

    # Past the ceiling: promotion to dense.
    for lo in range(0, 5000, 500):
        c("PFADD", "hll:dense", *["d%d" % i for i in range(lo, lo + 500)])
    v("hll:dense")
    c("PFCOUNT", "hll:dense")
    v("hll:dense")
    c("PFDEBUG", "ENCODING", "hll:dense")
    c("PFDEBUG", "DECODE", "hll:dense")

    # Forced conversion of a sparse value.
    c("PFADD", "hll:td", *["t%d" % i for i in range(30)])
    c("PFDEBUG", "TODENSE", "hll:td")
    c("PFDEBUG", "TODENSE", "hll:td")
    v("hll:td")
    c("PFCOUNT", "hll:td")
    v("hll:td")

    # Merges: sparse-only into a fresh key stays sparse; into an
    # existing sparse key; any dense input goes dense.
    c("PFMERGE", "hll:m1", "hll:a", "hll:b")
    v("hll:m1")
    c("PFCOUNT", "hll:m1")
    v("hll:m1")
    c("PFDEBUG", "ENCODING", "hll:m1")
    c("PFMERGE", "hll:b", "hll:inc")
    v("hll:b")
    c("PFMERGE", "hll:m2", "hll:dense", "hll:a")
    v("hll:m2")
    c("PFCOUNT", "hll:m2")
    v("hll:m2")
    c("PFMERGE", "hll:m3")
    v("hll:m3")

    # Union count across mixed encodings.
    c("PFCOUNT", "hll:a", "hll:b", "hll:dense")
    c("PFCOUNT", "hll:a", "hll:missing")

    # Missing keys.
    c("PFCOUNT", "hll:missing")
    c("PFDEBUG", "ENCODING", "hll:missing")

    # Error texts, pinned verbatim.
    c("SET", "plain", "notanhll")
    c("PFADD", "plain", "x")
    c("PFCOUNT", "plain")
    c("PFMERGE", "hll:m4", "plain")
    c("PFMERGE", "plain", "hll:a")
    c("PFDEBUG", "DECODE", "hll:dense")
    c("PFDEBUG", "NOSUCH", "hll:a")
    c("PFDEBUG", "GETREG", "hll:a", "extra")
    c("PFSELFTEST")

    print("\n".join(lines))


if __name__ == "__main__":
    main()
