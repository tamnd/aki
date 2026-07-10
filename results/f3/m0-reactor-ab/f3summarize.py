#!/usr/bin/env python3
# Summarize the reactor A/B matrix into summary.txt.
# Per cell and per aki arm (single, pair, reactor): the ab ratio compares the
# arm's aki-bench number against the rivals measured in the SAME aki-bench
# invocation; the rb ratio compares redis-benchmark numbers with themselves;
# the headline is the min of the two per-harness ratios. Harnesses never mix.
import json, glob, re, statistics, csv, math

G = "/root/f3gate/reactor-ab/matrix"
ARMS = ["single", "pair", "reactor"]
CELLS = [f"{wl}_{sz}_{p}_{c}"
         for p in ("p16", "p1")
         for c in ("c512", "c50")
         for wl in ("get", "set")
         for sz in ("64b", "1k")]

def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None

def fnum(x, nd=0):
    return "-" if x is None else f"{x:,.{nd}f}"

def frat(x):
    return "  -  " if x is None else f"{x:.2f}x"

def ab_reps(cell, arm):
    reps = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.ab.{arm}.rep*.json")):
        try: reps.append(json.load(open(f)))
        except Exception: pass
    return reps

def rb_rps(cell, tgt):
    rows = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.rb.{tgt}.rep*.csv")):
        try:
            for r in csv.reader(open(f)):
                if r and r[0] != "test":
                    rows.append((float(r[1]), float(r[6])))
        except Exception: pass
    if not rows: return None, None
    return med([x[0] for x in rows]), med([x[1] for x in rows])

def meta_txt(cell):
    try: return open(f"{G}/cells/{cell}.meta").read()
    except Exception: return ""

def rss_arm(cell, arm):
    # the arm's RSS right after its own value-bearing aki-bench reps
    vals = [int(v) for v in re.findall(rf"rss\[post-ab-{arm}-rep\d\].*?\b{arm}=(\d+)kB", meta_txt(cell))]
    return max(vals) / 1024 if vals else None  # MiB

def rss_rival(cell, name):
    vals = [int(v) for v in re.findall(rf"rss\[post-ab-\S+\].*?\b{name}=(\d+)kB", meta_txt(cell))]
    return max(vals) / 1024 if vals else None  # MiB

lines = []
lines.append("f3 reactor A/B matrix (m10-pullforward slice 7)")
lines.append("per arm: ab ratio = min over rivals from the same invocation; rb ratio = min over rivals, rb-vs-rb; HEAD = min(ab, rb)")
lines.append("")
hdr = (f"{'cell':<17} {'arm':<8} {'aki_ab':>10} {'rds_ab':>10} {'vky_ab':>10} {'r_ab':>6} "
       f"{'aki_rb':>10} {'rds_rb':>10} {'vky_rb':>10} {'r_rb':>6} {'HEAD':>6} "
       f"{'p99us':>8} {'rss':>7} {'r_rss':>6}")
lines.append(hdr)
lines.append("-" * len(hdr))

for cell in CELLS:
    r_rb, _ = rb_rps(cell, "redis")
    v_rb, _ = rb_rps(cell, "valkey")
    riv_rss = [rss_rival(cell, "redis"), rss_rival(cell, "valkey")]
    riv_rss = min([x for x in riv_rss if x], default=None)
    seen = False
    for arm in ARMS:
        reps = ab_reps(cell, arm)
        def v(t, f):
            return med([r.get(t, {}).get(f) for r in reps if not r.get(t, {}).get("skipped")])
        a_ab, rd_ab, vk_ab = v("aki", "value_ops_per_sec"), v("redis", "value_ops_per_sec"), v("valkey", "value_ops_per_sec")
        a_rb, _p99rb = rb_rps(cell, arm)
        ratio_ab = min(a_ab / rd_ab, a_ab / vk_ab) if (a_ab and rd_ab and vk_ab) else None
        ratio_rb = min(a_rb / r_rb, a_rb / v_rb) if (a_rb and r_rb and v_rb) else None
        head = min([x for x in (ratio_ab, ratio_rb) if x], default=None)
        p99 = v("aki", "p99_us")
        rss = rss_arm(cell, arm)
        r_rss = rss / riv_rss if (rss and riv_rss) else None
        if reps or a_rb: seen = True
        lines.append(f"{cell:<17} {arm:<8} {fnum(a_ab):>10} {fnum(rd_ab):>10} {fnum(vk_ab):>10} {frat(ratio_ab):>6} "
                     f"{fnum(a_rb):>10} {fnum(r_rb):>10} {fnum(v_rb):>10} {frat(ratio_rb):>6} {frat(head):>6} "
                     f"{fnum(p99):>8} {fnum(rss):>7} {frat(r_rss):>6}")
        # distinct-keys sanity per arm
        dk = v("aki", "distinct_keys_est")
        draws = v("aki", "key_draws")
        keys = 1000000
        if dk and draws:
            exp = keys * (1.0 - math.exp(draws * math.log(1.0 - 1.0 / keys)))
            if exp and abs(dk - exp) / exp > 0.02:
                lines.append(f"  DISTINCT-KEYS check {cell}/{arm}: est {dk:,.0f} vs expected {exp:,.0f}")
    if not seen:
        lines.append(f"{cell:<17} NO DATA")
    for pat in ("CRASH", "FATAL", "COVERAGE-NOTE"):
        for m in re.findall(rf"{pat}.*", meta_txt(cell)):
            lines.append(f"  NOTE {cell}: {m}")

out = "\n".join(lines) + "\n"
open(f"{G}/summary.txt", "w").write(out)
print(out)
