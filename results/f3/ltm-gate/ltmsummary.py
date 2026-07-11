#!/usr/bin/env python3
# Summarize an ltm-gate run (results/f3/ltm-gate/ltmgate.sh, issue tamnd/aki#542).
#
# For every cell it reports, per server, the honest larger-than-memory numbers:
# data-bearing throughput (value_ops_per_sec, which excludes the nils a capped
# rival returns for evicted keys), the dataset-coverage fraction from
# aki-bench's post-window probe, the peak resident set (VmHWM read from /proc by
# the runner, since aki-bench in connect mode cannot read a rival's PID), and
# bytes per retrievable key: peak memory over the keys the server can still
# serve. That last figure is the fair memory-efficiency number. A rival that
# evicted three quarters of the dataset looks cheap per stored key but expensive
# per retrievable key, because most of what it stored is gone.
#
# The median across the three timed reps is reported for throughput and
# coverage; the peak is the max VmHWM seen across the reps.
#
# Usage: ltmsummary.py <cells-dir>

import glob
import json
import os
import re
import sys
from statistics import median

SERVERS = ("aki", "redis", "valkey")


def load_reps(cell_dir, cell):
    """Return the list of per-rep aki-bench comparison dicts for one cell."""
    reps = []
    for path in sorted(glob.glob(os.path.join(cell_dir, cell + ".ab.rep*.json"))):
        try:
            with open(path) as fh:
                reps.append(json.load(fh))
        except (OSError, json.JSONDecodeError):
            pass
    return reps


def peak_kb(meta_text):
    """Max VmHWM in kB per server across every hwm[...] line in the meta file.

    Lines look like: hwm[post-rep0] aki=532480kB redis=537216kB valkey=...
    A missing or unparsable field contributes nothing.
    """
    peak = {s: 0 for s in SERVERS}
    for line in meta_text.splitlines():
        if not line.startswith("hwm["):
            continue
        for s in SERVERS:
            m = re.search(rf"\b{s}=(\d+)kB", line)
            if m:
                peak[s] = max(peak[s], int(m.group(1)))
    return peak


def med(values):
    vals = [v for v in values if v is not None]
    return median(vals) if vals else None


def fmt_bytes_per_key(peak_bytes, fraction, keyspace):
    if not peak_bytes or not fraction or not keyspace:
        return "-"
    retrievable = fraction * keyspace
    if retrievable <= 0:
        return "-"
    return f"{peak_bytes / retrievable:,.0f}"


def summarize_cell(cell_dir, cell):
    reps = load_reps(cell_dir, cell)
    if not reps:
        return None
    meta_path = os.path.join(cell_dir, cell + ".meta")
    meta_text = ""
    if os.path.exists(meta_path):
        with open(meta_path) as fh:
            meta_text = fh.read()
    peaks = peak_kb(meta_text)

    keyspace = 0
    rows = {}
    for s in SERVERS:
        vops, cov = [], []
        skipped = True
        for r in reps:
            e = r.get(s, {})
            if e.get("skipped"):
                continue
            skipped = False
            vops.append(e.get("value_ops_per_sec"))
            cov.append(e.get("coverage_fraction"))
            keyspace = keyspace or e.get("coverage_keyspace", 0)
        if skipped:
            rows[s] = None
            continue
        peak_bytes = peaks[s] * 1024 if peaks[s] else 0
        frac = med(cov)
        rows[s] = {
            "vops": med(vops),
            "cov": frac,
            "peak_mb": peak_bytes / (1 << 20) if peak_bytes else None,
            "bpk": fmt_bytes_per_key(peak_bytes, frac, keyspace),
        }
    return rows


def print_cell(cell, rows):
    print(f"\n== {cell} ==")
    print(f"{'server':<8}{'vops/sec':>14}{'cov%':>8}{'peak MB':>10}{'B/retr key':>14}")
    for s in SERVERS:
        row = rows.get(s)
        if row is None:
            print(f"{s:<8}{'skipped':>14}")
            continue
        vops = f"{row['vops']:,.0f}" if row["vops"] is not None else "-"
        cov = f"{row['cov'] * 100:.1f}" if row["cov"] is not None else "-"
        peak = f"{row['peak_mb']:.0f}" if row["peak_mb"] is not None else "-"
        print(f"{s:<8}{vops:>14}{cov:>8}{peak:>10}{row['bpk']:>14}")
    aki = rows.get("aki")
    for rival in ("redis", "valkey"):
        rv = rows.get(rival)
        if aki and rv and aki["vops"] and rv["vops"]:
            # Compare data-bearing throughput only. A ratio is only fair when the
            # coverage is comparable; flag it when it is not.
            ratio = aki["vops"] / rv["vops"]
            note = ""
            if aki["cov"] is not None and rv["cov"] is not None and abs(aki["cov"] - rv["cov"]) > 0.02:
                note = f"  (coverage differs: aki {aki['cov']*100:.0f}% vs {rival} {rv['cov']*100:.0f}%; compare coverage first)"
            print(f"  aki vops vs {rival}: {ratio:.2f}x{note}")


def main():
    if len(sys.argv) != 2:
        print("usage: ltmsummary.py <cells-dir>", file=sys.stderr)
        sys.exit(2)
    cell_dir = sys.argv[1]
    cells = sorted(
        {
            os.path.basename(p).split(".ab.rep")[0]
            for p in glob.glob(os.path.join(cell_dir, "*.ab.rep*.json"))
        }
    )
    if not cells:
        print(f"no aki-bench json under {cell_dir}", file=sys.stderr)
        sys.exit(1)
    for cell in cells:
        rows = summarize_cell(cell_dir, cell)
        if rows:
            print_cell(cell, rows)


if __name__ == "__main__":
    main()
