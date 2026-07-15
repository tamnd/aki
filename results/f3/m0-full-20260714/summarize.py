#!/usr/bin/env python3
import csv
import json
import pathlib
import statistics
import sys


root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")
cells = root / "cells"
done = (root / "done.list").read_text().splitlines()


def median(xs):
    return statistics.median(xs) if xs else 0.0


def json_row(cell):
    docs = [json.loads(p.read_text()) for p in sorted(cells.glob(f"{cell}.rep[123].json"))]
    if not docs:
        return None
    rates = {t: [d[t]["value_ops_per_sec"] for d in docs] for t in ("aki", "redis", "valkey")}
    p99 = {t: [d[t]["p99_us"] for d in docs] for t in ("aki", "redis", "valkey")}
    errors = sum(d["aki"].get("errors", 0) + d["aki"].get("err_replies", 0) for d in docs)
    a, r, v = (median(rates[t]) for t in ("aki", "redis", "valkey"))
    binding = max(r, v)
    p99_factor = max(
        d["aki"]["p99_us"] / min(d["redis"]["p99_us"], d["valkey"]["p99_us"])
        for d in docs
    )
    coverage = []
    for d in docs:
        coverage.append(d["aki"].get("coverage_fraction", ""))
    coverage_text = ""
    if any(x != "" for x in coverage):
        coverage_text = f"{median([float(x) for x in coverage if x != '']):.4f}"
    passed = a >= 2 * binding and p99_factor <= 1.25 and errors == 0
    return [cell, "aki-bench", a, r, v, a / binding if binding else 0, p99_factor,
            errors, coverage_text, "PASS" if passed else "MISS"]


def rb_csv(path):
    rows = list(csv.reader(path.open()))
    row = next(r for r in rows if r and r[0].lower() != "test")
    return float(row[1]), float(row[6])


def rb_row(cell):
    rates = {t: [] for t in ("aki", "redis", "valkey")}
    p99 = {t: [] for t in ("aki", "redis", "valkey")}
    for target in rates:
        for rep in (1, 2, 3):
            pair = [rb_csv(cells / f"{cell}.{target}.rep{rep}-{side}.csv") for side in ("a", "b")]
            rates[target].append(sum(x[0] for x in pair))
            p99[target].append(max(x[1] for x in pair))
    a, r, v = (median(rates[t]) for t in ("aki", "redis", "valkey"))
    binding = max(r, v)
    p99_factor = max(
        p99["aki"][i] / min(p99["redis"][i], p99["valkey"][i]) for i in range(3)
    )
    passed = a >= 2 * binding and p99_factor <= 1.25
    return [cell, "redis-benchmark", a, r, v, a / binding if binding else 0,
            p99_factor, 0, "", "PASS" if passed else "MISS"]


rows = []
for cell in done:
    row = json_row(cell)
    if row is None and list(cells.glob(f"{cell}.aki.rep1-*.csv")):
        row = rb_row(cell)
    if row is not None:
        rows.append(row)

out = csv.writer(sys.stdout, delimiter="\t", lineterminator="\n")
out.writerow(["cell", "harness", "aki_ops_s", "redis_ops_s", "valkey_ops_s",
              "binding_ratio", "p99_factor", "aki_errors", "aki_coverage", "verdict"])
for row in rows:
    out.writerow(row[:2] + [f"{x:.0f}" for x in row[2:5]] +
                 [f"{row[5]:.3f}", f"{row[6]:.3f}"] + row[7:])
