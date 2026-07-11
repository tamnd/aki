#!/usr/bin/env python3
# M1 set exit gate runner. Launches f3srv + redis + valkey pinned to cores 0-7,
# drives each cell with aki-bench in connect mode (client on cores 8-15), 3 reps,
# FLUSHALL+preload between reps (aki-bench does this per invocation), min-of-reps
# ratio, VmHWM per server via /proc with clear_refs reset per rep.
import json, os, signal, subprocess, sys, time, glob

BIN = "/root/f3gate/m1-gate/bin"
OUT = "/root/f3gate/m1-gate/cells"
AKI = BIN + "/aki-bench"
F3 = BIN + "/f3srv"
REDIS = "/root/bin/redis-server"
VALKEY = "/root/bin/valkey-server"
CLI = "/root/bin/redis-cli"
P_AKI, P_RED, P_VAL = 7301, 7302, 7303
os.makedirs(OUT, exist_ok=True)

procs = {}

def killall():
    subprocess.run("pkill -f 'f3srv|redis-server|valkey-server'", shell=True)
    time.sleep(2)

def launch(shards):
    killall()
    rd = "/tmp/rediswd"; vd = "/tmp/valkeywd"
    subprocess.run(f"rm -rf {rd} {vd}; mkdir -p {rd} {vd}", shell=True)
    procs['aki'] = subprocess.Popen(["taskset","-c","0-7",F3,"-addr",f":{P_AKI}","-shards",str(shards)],
                                    stdout=open("/tmp/f3.log","w"), stderr=subprocess.STDOUT)
    procs['redis'] = subprocess.Popen(["taskset","-c","0-7",REDIS,"--port",str(P_RED),"--save","","--appendonly","no",
                                       "--io-threads","4","--dir",rd,"--maxmemory","0"],
                                      stdout=open("/tmp/r.log","w"), stderr=subprocess.STDOUT)
    procs['valkey'] = subprocess.Popen(["taskset","-c","0-7",VALKEY,"--port",str(P_VAL),"--save","","--appendonly","no",
                                        "--io-threads","4","--dir",vd,"--maxmemory","0"],
                                       stdout=open("/tmp/v.log","w"), stderr=subprocess.STDOUT)
    # wait for all three to answer PING
    for _ in range(40):
        ok = True
        for p in (P_AKI,P_RED,P_VAL):
            r = subprocess.run([CLI,"-p",str(p),"ping"], capture_output=True, text=True)
            if "PONG" not in r.stdout: ok=False
        if ok:
            time.sleep(1); return True
        time.sleep(0.5)
    print("LAUNCH FAILED shards=%d" % shards);
    subprocess.run("tail -5 /tmp/r.log /tmp/v.log", shell=True)
    return False

def pid(name): return procs[name].pid

def vmhwm(p):
    try:
        for line in open(f"/proc/{p}/status"):
            if line.startswith("VmHWM:"): return int(line.split()[1])  # kB
    except: pass
    return 0

def clear_refs():
    for name in ('aki','redis','valkey'):
        try: open(f"/proc/{pid(name)}/clear_refs","w").write("5\n")
        except: pass

def run_cell(cell):
    fn = f"{OUT}/{cell['name']}.json"
    if os.path.exists(fn):
        print("skip (done)", cell['name']); return
    reps = []
    for rep in range(3):
        clear_refs()
        jf = f"/tmp/cell_{cell['name']}_{rep}.json"
        args = [AKI,"-workload",cell['wl'],"-members",str(cell['members']),
                "-connections",str(cell['conns']),"-pipeline",str(cell['pipe']),
                "-aki-addr",f"127.0.0.1:{P_AKI}","-redis-addr",f"127.0.0.1:{P_RED}",
                "-valkey-addr",f"127.0.0.1:{P_VAL}","-cpu-split=false","-json",jf]
        if cell['mode']=='req':
            args += ["-duration","0","-requests",str(cell['val'])]
        else:
            args += ["-duration",f"{cell['val']}s"]
        env = dict(os.environ);
        t0=time.time()
        r = subprocess.run(["taskset","-c","8-15"]+args, capture_output=True, text=True, env=env)
        dt=time.time()-t0
        mem = {n: vmhwm(pid(n)) for n in ('aki','redis','valkey')}
        try:
            d = json.load(open(jf))
        except Exception as e:
            print("  rep",rep,"PARSE FAIL",e, r.stderr[-300:]); continue
        rep_rec = {"rep":rep,"dt":round(dt,1),"mem_kb":mem}
        for tgt in ('aki','redis','valkey'):
            td = d.get(tgt,{})
            rep_rec[tgt] = {"ops":td.get('ops_per_sec'),"p50":td.get('p50_us'),
                            "p99":td.get('p99_us'),"p999":td.get('p999_us'),
                            "skipped":td.get('skipped'),"n":td.get('ops')}
        reps.append(rep_rec)
        a=rep_rec['aki']['ops']; rr=rep_rec['redis']['ops']; vv=rep_rec['valkey']['ops']
        rat = None
        if a and rr and vv: rat = round(min(a/rr,a/vv),3)
        print(f"  {cell['name']} rep{rep} {dt:.1f}s aki={a} redis={rr} valkey={vv} ratio={rat}")
    out = {"cell":cell,"reps":reps}
    json.dump(out, open(fn,"w"), indent=1)

# ---- cell matrix ----
C512=512; P16=16
def cell(name,wl,members,mode='dur',val=6,conns=C512,pipe=P16,group='main'):
    return {"name":name,"wl":wl,"members":members,"mode":mode,"val":val,"conns":conns,"pipe":pipe,"group":group}

MAIN = [
    # cardinality bands 1/10/10k/1M, point ops + draws + enumeration (non-destructive => duration)
    cell("sismember_c1","sismember",1), cell("sismember_c10","sismember",10),
    cell("sismember_c10k","sismember",10000), cell("sismember_c1m","sismember",1000000,val=8),
    cell("saddmember_c1","saddmember",1), cell("saddmember_c10","saddmember",10),
    cell("saddmember_c10k","saddmember",10000), cell("saddmember_c1m","saddmember",1000000,val=8),
    cell("scard_c10k","scard",10000), cell("scard_c1m","scard",1000000,val=8),
    cell("smismember_c10k","smismember",10000), cell("smismember_c1m","smismember",1000000,val=8),
    cell("srandmember_c1","srandmember",1), cell("srandmember_c10","srandmember",10),
    cell("srandmember_c10k","srandmember",10000), cell("srandmember_c1m","srandmember",1000000,val=8),
    cell("srandcount_c10k","srandmembercount",10000), cell("srandcount_c1m","srandmembercount",1000000,val=8),
    cell("smembers_c1","smembers",1), cell("smembers_c10","smembers",10),
    cell("smembers_c10k","smembers",10000,conns=50,pipe=1),
    cell("smembers_c1m","smembers",1000000,val=8,conns=16,pipe=1),
    cell("sscan_c10k","sscan",10000), cell("sscan_c1m","sscan",1000000,val=8,conns=50,pipe=4),
    # destructive point op, requests-capped so set stays populated
    cell("srem_c1m","srem",2000000,mode='req',val=1000000),
    cell("smove_c1m","smove",2000000,mode='req',val=1000000),
    # SPOP headline: native 10k row and hot 4M row, requests-capped
    cell("spop_native10k","spop",200000,mode='req',val=100000),
    cell("spop_hot4m","spop",4000000,mode='req',val=2000000),
    # SPOP P1 (pipeline 1) hot
    cell("spop_hot4m_p1","spop",4000000,mode='req',val=2000000,pipe=1,conns=50),
    # band-transition sweep on a non-destructive probe (sismember)
    cell("band_100","sismember",100,val=4), cell("band_500","sismember",500,val=4),
    cell("band_2000","sismember",2000,val=4), cell("band_130k","sismember",130000,val=4),
    cell("band_300k","sismember",300000,val=4), cell("band_2m","sismember",2000000,val=6),
]
ALGEBRA = [
    # non-store algebra returns the FULL result set to the client, so at 1M members
    # the reply is ~10MB/call: keep concurrency low (conns=8 pipe=1) to bound client
    # buffers, otherwise thousands of in-flight 1M-member replies OOM the box. Ratio is
    # aki vs rivals on the same low-concurrency full-reply path.
    cell("sinter_1m","sinter",1000000,val=8,conns=8,pipe=1,group='alg'),
    cell("sdiff_1m","sdiff",1000000,val=8,conns=8,pipe=1,group='alg'),
    cell("sunion_1m","sunion",1000000,val=8,conns=8,pipe=1,group='alg'),
    # small-cardinality non-store still returns an array reply; at high op rate the
    # per-op array allocation churns the client GC and balloons RSS, so keep these at
    # low concurrency too (conns=8 pipe=1). Ratio is still aki vs rivals on equal terms.
    cell("sinter_256","sinter",256,conns=8,pipe=1,group='alg'),
    cell("sinter_10","sinter",10,conns=8,pipe=1,group='alg'),
    # STORE and CARD return a bounded reply (an integer), but each 1M-set op is O(1M)
    # server work, so a single shard saturates on a handful of connections. High
    # concurrency only piles up multi-second queue latency and drags the drain out for
    # minutes; conns=16 pipe=1 gives the same saturated throughput and finishes fast.
    cell("sintercard_1m","sintercard",1000000,val=8,conns=16,pipe=1,group='alg'),
    cell("sinterstore_1m","sinterstore",1000000,val=8,conns=16,pipe=1,group='alg'),
    cell("sunionstore_1m","sunionstore",1000000,val=8,conns=16,pipe=1,group='alg'),
    cell("sdiffstore_1m","sdiffstore",1000000,val=8,conns=16,pipe=1,group='alg'),
    # 10k STORE ops are cheap enough to keep normal high concurrency.
    cell("sinterstore_10k","sinterstore",10000,group='alg'),
    cell("sunionstore_10k","sunionstore",10000,group='alg'),
    # re-run: sinterstore_10k at 512x16 collapsed on aki (queueing, p99 60s);
    # this low-concurrency cell is the honest saturated-throughput read.
    cell("sinterstore_10k_c16","sinterstore",10000,conns=16,pipe=1,group='alg'),
]

BUDGET=470  # seconds per invocation; exit clean before the ssh window closes
def main():
    which = sys.argv[1] if len(sys.argv)>1 else "main"
    cells = MAIN if which=="main" else ALGEBRA
    todo = [c for c in cells if not os.path.exists(f"{OUT}/{c['name']}.json")]
    if not todo:
        print("ALL CELLS DONE for", which); return
    shards = 4 if which=="main" else 1
    if not launch(shards): sys.exit(1)
    base = {n:vmhwm(pid(n)) for n in ('aki','redis','valkey')}
    json.dump(base, open(f"{OUT}/_baseline_{which}.json","w"))
    print("baseline",which,base, flush=True)
    start=time.time()
    for c in todo:
        if time.time()-start > BUDGET:
            print("BUDGET reached, exiting clean", flush=True); break
        run_cell(c)
    killall()
    print("BATCH DONE", which, flush=True)

if __name__=="__main__": main()
