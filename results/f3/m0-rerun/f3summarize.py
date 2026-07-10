#!/usr/bin/env python3
# Summarize the m0-rerun cells into summary.txt.
# rival number = min(aki-bench median, redis-benchmark median) per protocol.
import json, glob, os, re, statistics, csv

G = "/root/f3gate/m0-rerun"
CELLS = ["set_64b_1m", "get_64b_1m", "incr_1m", "set_1k_1m", "get_1k_1m",
         "get_64b_1m_zipf", "hot_set", "set_4k_sustained"]
KEYS = {"set_64b_1m": 1000000, "get_64b_1m": 1000000, "incr_1m": 1000000,
        "set_1k_1m": 1000000, "get_1k_1m": 1000000, "get_64b_1m_zipf": 1000000,
        "hot_set": 1, "set_4k_sustained": 1000000}

def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None

def fnum(x, nd=0):
    if x is None: return "-"
    return f"{x:,.{nd}f}"

def ab_reps(cell):
    reps = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.ab.rep*.json")):
        try:
            reps.append(json.load(open(f)))
        except Exception:
            pass
    return reps

def rb_reps(cell, tgt):
    rows = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.rb.{tgt}.rep*.csv")):
        try:
            for r in csv.reader(open(f)):
                if r and r[0] != "test":
                    rows.append({"rps": float(r[1]), "p99_ms": float(r[6])})
        except Exception:
            pass
    return rows

def meta_val(cell, pat):
    try:
        txt = open(f"{G}/cells/{cell}.meta").read()
    except Exception:
        return None
    m = re.findall(pat, txt)
    return m[-1] if m else None

lines = []
hdr = (f"{'cell':<18} {'aki_ops':>10} {'redis_ops':>10} {'valkey_ops':>10} {'ratio':>6} "
       f"{'aki_p99us':>9} {'rds_p99us':>9} {'vky_p99us':>9} "
       f"{'aki_rss':>9} {'rds_rss':>9} {'vky_rss':>9} "
       f"{'aki_B/k':>8} {'rds_B/k':>8} {'vky_B/k':>8}  notes")
lines.append(hdr)
lines.append("-" * len(hdr))

for cell in CELLS:
    reps = ab_reps(cell)
    if not reps:
        lines.append(f"{cell:<18} NO DATA")
        continue
    notes = []
    def tgt_stat(t, field):
        return med([r.get(t, {}).get(field) for r in reps if not r.get(t, {}).get("skipped")])
    def ops_stat(t):
        v = tgt_stat(t, "value_ops_per_sec")
        return v if v else tgt_stat(t, "ops_per_sec")
    aki_ab = ops_stat("aki")
    redis_ab = ops_stat("redis")
    valkey_ab = ops_stat("valkey")
    aki_p99 = tgt_stat("aki", "p99_us")
    redis_p99 = tgt_stat("redis", "p99_us")
    valkey_p99 = tgt_stat("valkey", "p99_us")
    rb_r = med([x["rps"] for x in rb_reps(cell, "redis")])
    rb_v = med([x["rps"] for x in rb_reps(cell, "valkey")])
    rb_a = med([x["rps"] for x in rb_reps(cell, "aki")])
    redis_n = min([x for x in (redis_ab, rb_r) if x is not None], default=None)
    valkey_n = min([x for x in (valkey_ab, rb_v) if x is not None], default=None)
    if rb_r is not None and redis_ab is not None:
        notes.append(f"rds ab={redis_ab/1e6:.2f}M rb={rb_r/1e6:.2f}M")
    if rb_v is not None and valkey_ab is not None:
        notes.append(f"vky ab={valkey_ab/1e6:.2f}M rb={rb_v/1e6:.2f}M")
    if rb_a is not None and aki_ab is not None:
        notes.append(f"aki rb={rb_a/1e6:.2f}M")
    ratio = None
    if aki_ab and redis_n and valkey_n:
        ratio = min(aki_ab / redis_n, aki_ab / valkey_n)
    # rss from meta post-ab
    rssline = meta_val(cell, r"rss\[post-ab\] (.*)")
    rss = dict(re.findall(r"(\w+)=(\S+)", rssline)) if rssline else {}
    # bytes/key: rivals from json, aki from meta used_memory post-rep0 (or post-ab)
    rbpk = tgt_stat("redis", "bytes_per_key")
    vbpk = tgt_stat("valkey", "bytes_per_key")
    abpk = tgt_stat("aki", "bytes_per_key")
    if abpk is None:
        um = meta_val(cell, r"used_memory\[post-rep0\] aki=(\d+)") or meta_val(cell, r"used_memory\[post-ab\] aki=(\d+)")
        if um:
            abpk = int(um) / KEYS[cell]
    # generator-bound / crash flags
    mtxt = ""
    try:
        mtxt = open(f"{G}/cells/{cell}.meta").read()
    except Exception:
        pass
    if "CRASH" in mtxt: notes.append("CRASH")
    gb = [i for i, r in enumerate(reps) if r.get("generator_bound") is True]
    if gb: notes.append(f"GEN-BOUND reps {gb}")
    lines.append(
        f"{cell:<18} {fnum(aki_ab):>10} {fnum(redis_n):>10} {fnum(valkey_n):>10} "
        f"{(f'{ratio:.2f}x' if ratio else '-'):>6} "
        f"{fnum(aki_p99, 1):>9} {fnum(redis_p99, 1):>9} {fnum(valkey_p99, 1):>9} "
        f"{rss.get('aki', '-'):>9} {rss.get('redis', '-'):>9} {rss.get('valkey', '-'):>9} "
        f"{fnum(abpk, 1):>8} {fnum(rbpk, 1):>8} {fnum(vbpk, 1):>8}  {'; '.join(notes)}")

out = "\n".join(lines) + "\n"
open(f"{G}/summary.txt", "w").write(out)
print(out)
