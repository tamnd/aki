#!/usr/bin/env python3
# Summarize m0-run3 into summary.txt.
# Ratios computed BOTH ways per cell: ab-vs-ab and rb-vs-rb; the headline is
# the min of the two per-harness ratios (never rival min across harnesses:
# redis-benchmark generator-binds low and mixing harnesses flatters aki).
import json, glob, re, statistics, csv, math

G = "/root/f3gate/m0-run3"
STD = ["set_64b_1m", "incr_1m", "set_1k_1m", "get_64b_1m", "get_1k_1m"]
LTM = ["ltm_get_uniform", "ltm_get_zipf", "ltm_set_uniform"]
KEYS = {c: 1000000 for c in STD}
LTM_KEYS = 2000000

def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None

def fnum(x, nd=0):
    return "-" if x is None else f"{x:,.{nd}f}"

def ab_reps(cell):
    reps = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.ab.rep*.json")):
        try: reps.append(json.load(open(f)))
        except Exception: pass
    return reps

def rb_reps(cell, tgt):
    rows = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.rb.{tgt}.rep*.csv")):
        try:
            for r in csv.reader(open(f)):
                if r and r[0] != "test":
                    rows.append({"rps": float(r[1]), "p99_ms": float(r[6])})
        except Exception: pass
    return rows

def meta_txt(cell):
    try: return open(f"{G}/cells/{cell}.meta").read()
    except Exception: return ""

def meta_val(cell, pat):
    m = re.findall(pat, meta_txt(cell))
    return m[-1] if m else None

def tgt_get(reps, t, field):
    return med([r.get(t, {}).get(field) for r in reps if not r.get(t, {}).get("skipped")])

def expected_distinct(k, draws):
    if not k or not draws: return None
    return k * (1.0 - math.exp(draws * math.log(1.0 - 1.0 / k)))

lines = []
lines.append("f3 M0 run3 — aki #573 (hot-set LTM residency) + aki-bench #42 (honest per-connection key streams)")
lines.append("headline ratio per cell = min(ab-vs-ab, rb-vs-rb); each harness ratio = min over rivals")
lines.append("")

hdr = (f"{'cell':<14} {'aki_ab':>10} {'rds_ab':>10} {'vky_ab':>10} {'r_ab':>6} "
       f"{'aki_rb':>10} {'rds_rb':>10} {'vky_rb':>10} {'r_rb':>6} {'HEAD':>6} "
       f"{'aki_p99us':>9} {'aki_rss':>9} {'rds_rss':>9} {'vky_rss':>9} "
       f"{'aki_B/k':>8} {'rds_B/k':>8}  notes")
lines.append(hdr)
lines.append("-" * len(hdr))

cov_lines = []
for cell in STD:
    reps = ab_reps(cell)
    if not reps:
        lines.append(f"{cell:<14} NO DATA")
        continue
    notes = []
    def ops(t):
        v = tgt_get(reps, t, "value_ops_per_sec")
        return v if v else tgt_get(reps, t, "ops_per_sec")
    a_ab, r_ab, v_ab = ops("aki"), ops("redis"), ops("valkey")
    a_rb = med([x["rps"] for x in rb_reps(cell, "aki")])
    r_rb = med([x["rps"] for x in rb_reps(cell, "redis")])
    v_rb = med([x["rps"] for x in rb_reps(cell, "valkey")])
    ratio_ab = min(a_ab / r_ab, a_ab / v_ab) if (a_ab and r_ab and v_ab) else None
    ratio_rb = min(a_rb / r_rb, a_rb / v_rb) if (a_rb and r_rb and v_rb) else None
    head = min([x for x in (ratio_ab, ratio_rb) if x is not None], default=None)
    aki_p99 = tgt_get(reps, "aki", "p99_us")
    rssline = meta_val(cell, r"rss\[post-ab\] (.*)")
    rss = dict(re.findall(r"(\w+)=(\S+)", rssline)) if rssline else {}
    rbpk = tgt_get(reps, "redis", "bytes_per_key")
    abpk = tgt_get(reps, "aki", "bytes_per_key")
    if abpk is None:
        um = meta_val(cell, r"used_memory\[post-rep0\] aki=(\d+)") or meta_val(cell, r"used_memory\[post-ab\] aki=(\d+)")
        if um: abpk = int(um) / KEYS[cell]
    mtxt = meta_txt(cell)
    if "CRASH" in mtxt: notes.append("CRASH")
    gb = [i for i, r in enumerate(reps) if r.get("generator_bound") is True]
    if gb: notes.append(f"GEN-BOUND reps {gb}")
    if "COVERAGE-NOTE" in mtxt: notes.append("COVERAGE-WARN")
    # distinct-keys check vs k(1-(1-1/k)^n), aki row, per rep
    for i, r in enumerate(reps):
        e = r.get("aki", {})
        dk, draws = e.get("distinct_keys_est"), e.get("key_draws")
        if dk and draws:
            want = expected_distinct(KEYS[cell], draws)
            dev = (dk - want) / want * 100
            cov_lines.append(f"  {cell} rep{i} aki: distinct_keys_est={dk:,} draws={draws:,} expected={want:,.0f} dev={dev:+.1f}%")
    lines.append(
        f"{cell:<14} {fnum(a_ab):>10} {fnum(r_ab):>10} {fnum(v_ab):>10} "
        f"{(f'{ratio_ab:.2f}x' if ratio_ab else '-'):>6} "
        f"{fnum(a_rb):>10} {fnum(r_rb):>10} {fnum(v_rb):>10} "
        f"{(f'{ratio_rb:.2f}x' if ratio_rb else '-'):>6} "
        f"{(f'{head:.2f}x' if head else '-'):>6} "
        f"{fnum(aki_p99,1):>9} {rss.get('aki','-'):>9} {rss.get('redis','-'):>9} {rss.get('valkey','-'):>9} "
        f"{fnum(abpk,1):>8} {fnum(rbpk,1):>8}  {'; '.join(notes)}")

lines.append("")
lines.append("distinct_keys_est check (aki windows, uniform std cells):")
lines.extend(cov_lines if cov_lines else ["  none recorded"])
cov_warns = []
for cell in STD:
    for m in re.findall(r"COVERAGE-NOTE (.*)", meta_txt(cell)):
        cov_warns.append(f"  {cell}: {m}")
lines.append("coverage warnings printed by aki-bench:")
lines.extend(cov_warns if cov_warns else ["  none"])

# ---- LTM section ----
lines.append("")
lines.append("LTM strings (2M x 1032B ~2GB raw; aki 4x128MiB resident cap + vlog; rivals maxmemory 512mb allkeys-lfu)")
lines.append("aki-bench sole harness (rb would count rival nils as ops). ratio = value-bearing ops vs best rival.")
h2 = (f"{'cell':<16} {'tgt':<7} {'vops/s':>10} {'ops/s':>10} {'hit%':>6} {'nil%':>6} {'p99us':>8} {'ratio':>7}")
lines.append(h2)
lines.append("-" * len(h2))
for cell in LTM:
    reps = ab_reps(cell)
    if not reps:
        lines.append(f"{cell:<16} NO DATA")
        continue
    vals = {}
    for t in ("aki", "redis", "valkey"):
        vops = tgt_get(reps, t, "value_ops_per_sec") or tgt_get(reps, t, "ops_per_sec")
        ops_ = tgt_get(reps, t, "ops_per_sec")
        hit = tgt_get(reps, t, "hit_ratio")
        nils = med([ (r.get(t,{}).get("nil_replies",0) / r.get(t,{}).get("ops",1)) for r in reps if r.get(t,{}).get("ops")])
        p99 = tgt_get(reps, t, "p99_us")
        vals[t] = vops
        ratio = ""
        if t == "aki":
            best = max([vals.get(x) for x in ("redis","valkey") if vals.get(x)] or [None])
        lines.append(f"{cell:<16} {t:<7} {fnum(vops):>10} {fnum(ops_):>10} "
                     f"{(f'{hit*100:.1f}' if hit is not None else '-'):>6} "
                     f"{(f'{nils*100:.1f}' if nils is not None else '-'):>6} {fnum(p99,1):>8} {ratio:>7}")
    a, r, v = vals.get("aki"), vals.get("redis"), vals.get("valkey")
    if a and r and v:
        lines.append(f"{'':<16} value-bearing ratio: {min(a/r, a/v):.2f}x (vs best rival)")
    # aki residency counters: deltas across the ab phase from meta info snapshots
    mtxt = meta_txt(cell)
    def info_series(field):
        out = []
        tag = None
        for ln in mtxt.splitlines():
            m = re.match(r"--- f3info\[(\S+)\]", ln)
            if m: tag = m.group(1); continue
            m = re.match(rf"{field}:(\d+)", ln)
            if m and tag: out.append((tag, int(m.group(1))))
        return out
    vr = info_series("vlog_reads")
    pr = info_series("ltm_promotes")
    dm = info_series("ltm_demotes")
    if vr:
        lines.append(f"{'':<16} aki vlog_reads by snap: " + " ".join(f"{t}={n:,}" for t, n in vr))
    if pr or dm:
        lines.append(f"{'':<16} aki ltm_promotes: " + " ".join(f"{t}={n:,}" for t, n in pr)
                     + " | ltm_demotes: " + " ".join(f"{t}={n:,}" for t, n in dm))
    # aki resident hit ratio over the ab phase (GET ops incl. warm not separable;
    # use measured ops x3 reps as the denominator floor)
    if vr and len(vr) >= 2:
        dvr = vr[-1][1] - vr[0][1]
        tot_ops = sum(r.get("aki", {}).get("ops", 0) for r in reps)
        if tot_ops:
            lines.append(f"{'':<16} aki resident-hit approx: 1 - dvlog_reads/measured_ops = {1 - dvr/tot_ops:.4f} (dvlog_reads={dvr:,}, measured ops={tot_ops:,}; warm traffic not in denominator)")
    for m in re.findall(r"rival_info\[post-rep2\]\[(\w+)\] (.*)", mtxt):
        lines.append(f"{'':<16} {m[0]} INFO end: {m[1]}")
    # RSS from 1s samples
    try:
        samp = open(f"{G}/cells/{cell}.rss.samples").read()
        mx = {}
        for t in ("aki", "redis", "valkey"):
            xs = [int(x) for x in re.findall(rf"{t}=(\d+)", samp)]
            if xs: mx[t] = max(xs)
        if mx:
            cap = 512 * 1024
            am = mx.get("aki")
            rm = max([mx.get("redis", 0), mx.get("valkey", 0)]) or None
            s = f"{'':<16} RSS max (1s samples): " + " ".join(f"{t}={v/1024:.0f}MiB" for t, v in mx.items())
            if am: s += f" | aki/cap(512MiB)={am/cap:.2f}x"
            if am and rm: s += f" | aki/best-rival-rss={am/rm:.2f}x (memory bar: <2x)"
            lines.append(s)
    except Exception:
        pass
    if "CRASH" in mtxt: lines.append(f"{'':<16} CRASH flagged")
    lines.append("")

out = "\n".join(lines) + "\n"
open(f"{G}/summary.txt", "w").write(out)
print(out)
