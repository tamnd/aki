#!/usr/bin/env python3
# Summarize the f3 arena-rss A/B matrix into summary.txt.
# Arms: osingle/oreactor = before (main 49ab548), nsingle/nreactor = after (branch).
# Throughput per arm: ab ratio = min over rivals measured in the SAME aki-bench
# invocation; rb ratio = redis-benchmark-vs-redis-benchmark; HEAD = min(ab, rb).
# Harnesses never mix.
# RSS per arm: peak VmRSS the arm reaches across the cell's phases (working set
# under gate load) and the post-rb residual (idle floor), each divided by the
# rival's peak VmRSS in the same cell. The memory bar is peak-vs-peak < 2.0x.
import json, glob, re, statistics, csv, os, sys

G = os.environ.get("G", "/root/f3gate/rss-ab/matrix")
CELLS = ["get_64b_p16_c512", "set_64b_p16_c512",
         "get_1k_p16_c512", "set_1k_p16_c512",
         "get_64b_p1_c512", "set_64b_p1_c512"]
ARMS = ["osingle", "oreactor", "nsingle", "nreactor"]
RIVALS = ["redis", "valkey"]

def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None

def fnum(x, nd=0):
    return "-" if x is None else f"{x:,.{nd}f}"

def frat(x):
    return "  -  " if x is None else f"{x:.2f}x"

def meta_txt(cell):
    try: return open(f"{G}/cells/{cell}.meta").read()
    except Exception: return ""

def ab_reps(cell, arm):
    reps = []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.ab.{arm}.rep*.json")):
        try: reps.append(json.load(open(f)))
        except Exception: pass
    return reps

def rb_rps(cell, tgt):
    rps, p99 = [], []
    for f in sorted(glob.glob(f"{G}/cells/{cell}.rb.{tgt}.rep*.csv")):
        try:
            for r in csv.reader(open(f)):
                if r and r[0] != "test":
                    rps.append(float(r[1])); p99.append(float(r[6]))
        except Exception: pass
    return med(rps), med(p99)

def rss_series(cell, name, kind="rss"):
    # all VmRSS (or VmHWM) samples for a server name across the cell's phases
    txt = meta_txt(cell)
    out = {}
    for line in txt.splitlines():
        m = re.match(rf"{kind}\[([^\]]+)\]", line)
        if not m: continue
        tag = m.group(1)
        vm = re.search(rf"\b{name}=(\d+)kB", line)
        if vm: out[tag] = int(vm.group(1)) / 1024.0  # MiB
    return out

def peak_ab_rss(cell, name):
    # headline: peak VmRSS across the aki-bench (gate-shape) phases only,
    # harness-consistent with results/f3/m0-reactor-ab.md (the rb preload
    # transiently inflates buffers and is excluded).
    s = rss_series(cell, name, "rss")
    ab = {k: v for k, v in s.items() if k == "launch" or k.startswith("post-ab")}
    return max(ab.values()) if ab else None

def peak_all_rss(cell, name):
    s = rss_series(cell, name, "rss")
    return max(s.values()) if s else None

def launch_rss(cell, name):
    s = rss_series(cell, name, "rss")
    return s.get("launch")

def loaded_rss(cell, name):
    # each server's OWN loaded-idle RSS: the snapshot taken right after that
    # server ran its redis-benchmark reps (1M keys resident, others flushed).
    # This is the "same data in-memory fit" measurement.
    s = rss_series(cell, name, "rss")
    return s.get(f"post-rb-{name}")

def peak_hwm(cell, name):
    # VmHWM is monotonic; the max across the cell is the most RAM the box ever
    # had to hand this server, the transient peak the LTM pitch must beat too.
    s = rss_series(cell, name, "hwm")
    return max(s.values()) if s else None

def ledger(cell, arm, tgt):
    # median used_memory (bytes) and distinct keys from the aki-bench json,
    # per target, taken from the arm's own invocations.
    reps = ab_reps(cell, arm)
    um = med([r.get(tgt, {}).get("used_memory") for r in reps if not r.get(tgt, {}).get("skipped")])
    dk = med([r.get(tgt, {}).get("distinct_keys_est") for r in reps if not r.get(tgt, {}).get("skipped")])
    return um, dk

lines = []
lines.append("f3 arena-rss A/B matrix (issue #542, lab 20)")
lines.append("before = osingle/oreactor (main 49ab548, heap arena); after = nsingle/nreactor (branch, mapped arena + leased buffers)")
lines.append("throughput HEAD = min(ab-ratio, rb-ratio), each min over both rivals")
lines.append("memory bar (tightened): aki loaded-idle RSS <= 1.0x leanest rival for the same 1M-key dataset; ideal 0.5x")
lines.append("")

# --- throughput table ---
th = (f"{'cell':<17} {'arm':<9} {'aki_ab':>10} {'r_ab':>6} {'aki_rb':>10} {'r_rb':>6} "
      f"{'HEAD':>6} {'p99us':>8}")
lines.append("THROUGHPUT")
lines.append(th)
lines.append("-" * len(th))
for cell in CELLS:
    rd_rb, _ = rb_rps(cell, "redis")
    vk_rb, _ = rb_rps(cell, "valkey")
    for arm in ARMS:
        reps = ab_reps(cell, arm)
        def v(t, f):
            return med([r.get(t, {}).get(f) for r in reps if not r.get(t, {}).get("skipped")])
        a_ab, rd_ab, vk_ab = v("aki", "value_ops_per_sec"), v("redis", "value_ops_per_sec"), v("valkey", "value_ops_per_sec")
        a_rb, p99rb = rb_rps(cell, arm)
        ratio_ab = min(a_ab/rd_ab, a_ab/vk_ab) if (a_ab and rd_ab and vk_ab) else None
        ratio_rb = min(a_rb/rd_rb, a_rb/vk_rb) if (a_rb and rd_rb and vk_rb) else None
        head = min([x for x in (ratio_ab, ratio_rb) if x], default=None)
        p99 = v("aki", "p99_us")
        lines.append(f"{cell:<17} {arm:<9} {fnum(a_ab):>10} {frat(ratio_ab):>6} {fnum(a_rb):>10} {frat(ratio_rb):>6} "
                     f"{frat(head):>6} {fnum(p99):>8}")
    lines.append("")

# --- loaded-idle RSS table: the "same data in-memory fit" bar (<= 1.0x) ---
# loadRSS = the server's own post-rb snapshot (1M keys resident, idle).
# rssB/key = (loadRSS - launchRSS) / distinct; ledgB/key = used_memory/distinct.
# bar = loadRSS / leanest-rival loadRSS; goal <= 1.0x (ideal 0.5x).
rh = (f"{'cell':<17} {'server':<9} {'loadRSS':>8} {'steadyBar':>13} {'peakHWM':>8} {'peakBar':>13} "
      f"{'rssB/key':>9} {'ledgB/key':>10}")
lines.append("MEMORY = same-data in-memory fit (MiB). bars vs leanest rival, goal <= 1.0x each.")
lines.append("loadRSS = own post-rb idle (1M keys resident); peakHWM = max VmHWM in cell (transient peak).")
lines.append("ledgB/key = internal used_memory per key; rssB/key = resident growth per key.")
lines.append(rh)
lines.append("-" * len(rh))
for cell in CELLS:
    def brow(name):
        # ledger target: rivals keyed by name, aki arms keyed "aki";
        # read from an arm invocation that actually ran (nsingle for rivals).
        ld = loaded_rss(cell, name)
        lc = launch_rss(cell, name)
        pk = peak_hwm(cell, name)
        if name in RIVALS:
            um, dk = ledger(cell, "nsingle", name)
        else:
            um, dk = ledger(cell, name, "aki")
        rssbk = ((ld - lc) * (1 << 20) / dk) if (ld is not None and lc is not None and dk) else None
        ledgbk = (um / dk) if (um and dk) else None
        return ld, pk, rssbk, ledgbk
    rd_ld, rd_pk, _, _ = brow("redis")
    vk_ld, vk_pk, _, _ = brow("valkey")
    lean_ld = min([x for x in (rd_ld, vk_ld) if x], default=None)
    lean_pk = min([x for x in (rd_pk, vk_pk) if x], default=None)
    def bar(x, lean):
        return "PASS" if (x and lean and x/lean <= 1.0) else ("FAIL" if (x and lean) else "-")
    def rat(x, lean):
        return frat(x/lean) if (x and lean) else "  -  "
    for name in ["redis", "valkey"]:
        ld, pk, rssbk, ledgbk = brow(name)
        lines.append(f"{cell:<17} {name:<9} {fnum(ld):>8} {'-':>13} {fnum(pk):>8} {'-':>13} {fnum(rssbk):>9} {fnum(ledgbk,1):>10}")
    for arm in ARMS:
        ld, pk, rssbk, ledgbk = brow(arm)
        sb = f"{rat(ld,lean_ld)} {bar(ld,lean_ld)}"
        pb = f"{rat(pk,lean_pk)} {bar(pk,lean_pk)}"
        lines.append(f"{cell:<17} {arm:<9} {fnum(ld):>8} {sb:>13} {fnum(pk):>8} {pb:>13} {fnum(rssbk):>9} {fnum(ledgbk,1):>10}")
    lines.append("")

# --- notes ---
for cell in CELLS:
    for pat in ("CRASH", "FATAL", "COVERAGE"):
        for m in re.findall(rf"{pat}.*", meta_txt(cell)):
            lines.append(f"NOTE {cell}: {m}")

out = "\n".join(lines) + "\n"
open(f"{G}/summary.txt", "w").write(out)
print(out)
