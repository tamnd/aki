#!/usr/bin/env python3
import csv
import pathlib
import re
import sys


root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")
cells = root / "cells"
done = (root / "done.list").read_text().splitlines()


def parse(path):
    out = {}
    target = None
    for line in path.read_text().splitlines():
        if line.startswith("["):
            target = line.strip("[]")
            out[target] = {}
        elif target and (m := re.match(r"(VmRSS|VmHWM):\s+(\d+) kB", line)):
            out[target][m.group(1)] = int(m.group(2))
        elif target and line.startswith("dbsize="):
            out[target]["dbsize"] = int(line.split("=", 1)[1])
        elif target and line.startswith("used_memory:"):
            out[target]["used_memory"] = int(line.split(":", 1)[1].strip())
    return out


writer = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
writer.writerow(["cell", "aki_rss_kib", "lean_rival_rss_kib", "rss_ratio",
                 "aki_hwm_kib", "lean_rival_hwm_kib", "hwm_ratio",
                 "aki_used_memory", "lean_rival_used_memory", "ledger_ratio",
                 "strict_memory_verdict", "historical_2x_verdict"])
for cell in done:
    paths = list(cells.glob(f"{cell}.rep3.memory.txt")) or list(cells.glob(f"{cell}.final.memory.txt"))
    if not paths:
        continue
    d = parse(paths[0])
    if any(t not in d for t in ("aki", "redis", "valkey")):
        continue
    lean_rss = min(d[t]["VmRSS"] for t in ("redis", "valkey"))
    lean_hwm = min(d[t]["VmHWM"] for t in ("redis", "valkey"))
    lean_used = min(d[t].get("used_memory", 0) for t in ("redis", "valkey"))
    rss_ratio = d["aki"]["VmRSS"] / lean_rss
    hwm_ratio = d["aki"]["VmHWM"] / lean_hwm
    ledger_ratio = d["aki"].get("used_memory", 0) / lean_used if lean_used else 0
    strict = "PASS" if rss_ratio <= 1 and hwm_ratio <= 1 else "MISS"
    historical = "PASS" if rss_ratio < 2 and hwm_ratio < 2 else "MISS"
    writer.writerow([cell, d["aki"]["VmRSS"], lean_rss, f"{rss_ratio:.3f}",
                     d["aki"]["VmHWM"], lean_hwm, f"{hwm_ratio:.3f}",
                     d["aki"].get("used_memory", ""), lean_used, f"{ledger_ratio:.3f}",
                     strict, historical])
