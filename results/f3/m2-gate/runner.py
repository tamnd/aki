#!/usr/bin/env python3
# f3 M2 (zset) / M3 (list) exit gate runner. Same protocol as m1-gate/runner.py:
# f3srv + redis + valkey pinned to cores 0-7, aki-bench connect mode client on
# cores 8-15, 3 reps, FLUSHALL+preload per aki-bench invocation, min-of-reps
# ratio, VmHWM per server via /proc with clear_refs reset per rep. Resumable via
# per-cell json in cells/. Usage: gate_runner.py {m2|m3} {main|alg}
import json, os, subprocess, sys, time

MS = sys.argv[1] if len(sys.argv) > 1 else "m2"
GROUP = sys.argv[2] if len(sys.argv) > 2 else "main"
G = "/root/f3gate/m2m3"
BIN = G + "/bin"
OUT = f"{G}/{MS}/cells"
AKI = BIN + "/aki-bench"
F3 = BIN + "/f3srv"
REDIS = "/root/bin/redis-server"
VALKEY = "/root/bin/valkey-server"
CLI = "/root/bin/redis-cli"
P_AKI, P_RED, P_VAL = (7311, 7312, 7313) if MS == "m2" else (7321, 7322, 7323)
os.makedirs(OUT, exist_ok=True)
procs = {}

def killall():
    subprocess.run("pkill -f 'f3srv|redis-server|valkey-server'", shell=True)
    time.sleep(2)

def launch(shards):
    killall()
    rd, vd = "/tmp/rd_" + MS, "/tmp/vd_" + MS
    subprocess.run(f"rm -rf {rd} {vd}; mkdir -p {rd} {vd}", shell=True)
    procs['aki'] = subprocess.Popen(["taskset","-c","0-7",F3,"-addr",f":{P_AKI}","-shards",str(shards)],
                                    stdout=open("/tmp/f3_"+MS+".log","w"), stderr=subprocess.STDOUT)
    procs['redis'] = subprocess.Popen(["taskset","-c","0-7",REDIS,"--port",str(P_RED),"--save","","--appendonly","no",
                                       "--io-threads","4","--dir",rd,"--maxmemory","0"],
                                      stdout=open("/tmp/r_"+MS+".log","w"), stderr=subprocess.STDOUT)
    procs['valkey'] = subprocess.Popen(["taskset","-c","0-7",VALKEY,"--port",str(P_VAL),"--save","","--appendonly","no",
                                        "--io-threads","4","--io-threads-do-reads","yes","--dir",vd,"--maxmemory","0"],
                                       stdout=open("/tmp/v_"+MS+".log","w"), stderr=subprocess.STDOUT)
    for _ in range(50):
        ok = all("PONG" in subprocess.run([CLI,"-p",str(p),"ping"],capture_output=True,text=True).stdout
                 for p in (P_AKI,P_RED,P_VAL))
        if ok:
            time.sleep(1); return True
        time.sleep(0.5)
    print("LAUNCH FAILED shards=%d" % shards)
    subprocess.run("tail -5 /tmp/f3_%s.log /tmp/r_%s.log /tmp/v_%s.log" % (MS,MS,MS), shell=True)
    return False

def pid(n): return procs[n].pid

def vmhwm(p):
    try:
        for line in open(f"/proc/{p}/status"):
            if line.startswith("VmHWM:"): return int(line.split()[1])
    except: pass
    return 0

def clear_refs():
    for n in ('aki','redis','valkey'):
        try: open(f"/proc/{pid(n)}/clear_refs","w").write("5\n")
        except: pass

def run_cell(c):
    fn = f"{OUT}/{c['name']}.json"
    if os.path.exists(fn):
        print("skip (done)", c['name']); return
    reps = []
    for rep in range(3):
        clear_refs()
        jf = f"/tmp/cell_{MS}_{c['name']}_{rep}.json"
        args = [AKI,"-workload",c['wl'],"-members",str(c['members']),
                "-connections",str(c['conns']),"-pipeline",str(c['pipe']),
                "-aki-addr",f"127.0.0.1:{P_AKI}","-redis-addr",f"127.0.0.1:{P_RED}",
                "-valkey-addr",f"127.0.0.1:{P_VAL}","-cpu-split=false","-json",jf]
        if c.get('dist'):
            args += ["-dist",c['dist'],"-zipf-s",str(c.get('zipf',0.99))]
        if c['mode'] == 'req':
            args += ["-duration","0","-requests",str(c['val'])]
        else:
            args += ["-duration",f"{c['val']}s"]
        try:
            r = subprocess.run(["taskset","-c","8-15"]+args, capture_output=True, text=True, timeout=240)
        except subprocess.TimeoutExpired:
            subprocess.run(["pkill","-9","-f","aki-bench"]); time.sleep(2)
            print("  rep",rep,"TIMEOUT(240s) killed aki-bench", flush=True); continue
        mem = {n: vmhwm(pid(n)) for n in ('aki','redis','valkey')}
        try:
            d = json.load(open(jf))
        except Exception as e:
            print("  rep",rep,"PARSE FAIL",e,r.stderr[-300:]); continue
        rec = {"rep":rep,"mem_kb":mem}
        for t in ('aki','redis','valkey'):
            td = d.get(t,{})
            rec[t] = {"ops":td.get('ops_per_sec'),"p50":td.get('p50_us'),
                      "p99":td.get('p99_us'),"p999":td.get('p999_us')}
        reps.append(rec)
        a,rr,vv = rec['aki']['ops'],rec['redis']['ops'],rec['valkey']['ops']
        rat = round(min(a/rr,a/vv),3) if a and rr and vv else None
        print(f"  {c['name']} rep{rep} aki={a} redis={rr} valkey={vv} ratio={rat}", flush=True)
    json.dump({"cell":c,"reps":reps}, open(fn,"w"), indent=1)

def cell(name,wl,members,mode='dur',val=6,conns=512,pipe=16,dist=None,zipf=0.99):
    return {"name":name,"wl":wl,"members":members,"mode":mode,"val":val,
            "conns":conns,"pipe":pipe,"dist":dist,"zipf":zipf}

# ---- M2 zset cells ----
M2_MAIN = [
    cell("zscore_c1","zscore",1), cell("zscore_c10","zscore",10),
    cell("zscore_c10k","zscore",10000), cell("zscore_c1m","zscore",1000000,val=8),
    cell("zrank_c10k","zrank",10000), cell("zrank_c1m","zrank",1000000,val=8),
    cell("zrank_zipf_c1m","zrank",1000000,val=8,dist="zipfian",zipf=0.99),  # headline
    cell("zrank_zipf_c10k","zrank",10000,dist="zipfian",zipf=0.99),
    cell("zcard_c10k","zcard",10000), cell("zcard_c1m","zcard",1000000,val=8),
    cell("zmscore_c10k","zmscore",10000), cell("zmscore_c1m","zmscore",1000000,val=8),
    cell("zaddmember_c1","zaddmember",1), cell("zaddmember_c10k","zaddmember",10000),
    cell("zaddmember_c1m","zaddmember",1000000,val=8),
    cell("zincrby_c10k","zincrby",10000), cell("zincrby_c1m","zincrby",1000000,val=8),
    cell("zrange_c10k","zrange",10000,conns=50,pipe=1),          # headline
    cell("zrange_c1m","zrange",1000000,val=8,conns=16,pipe=1),
    cell("zrangebyscore_c10k","zrangebyscore",10000,conns=50,pipe=1),  # headline
    cell("zrangebyscore_c1m","zrangebyscore",1000000,val=8,conns=16,pipe=1),
    cell("zadd_flat_10k","zadd",10000),                          # hold K4
    cell("zadd_flat_1m","zadd",1000000,val=8),
    cell("zrem_hot","zrem",2000000,mode='req',val=1000000),      # p99 shoulder clause
    # band transition across the listpack->skiplist boundary (128 default)
    cell("band_100","zscore",100,val=4), cell("band_500","zscore",500,val=4),
    cell("band_2000","zscore",2000,val=4), cell("band_130k","zscore",130000,val=4),
    cell("band_300k","zscore",300000,val=4),
]
M2_ALG = [
    cell("zunion_1m","zunion",1000000,val=8,conns=8,pipe=1),
    cell("zunion_256","zunion",256,conns=8,pipe=1),
]
# ---- M3 list cells ----
M3_MAIN = [
    cell("llen_c10k","llen",10000), cell("llen_c1m","llen",1000000,val=8),
    cell("lindex_c10k","lindex",10000), cell("lindex_c1m","lindex",1000000,val=8),
    cell("lset_c10k","lset",10000), cell("lset_c1m","lset",1000000,val=8),
    cell("lpos_c10k","lpos",10000), cell("lpos_c1m","lpos",1000000,val=8),  # v1 miss
    cell("lrange_c10k","lrange",10000,conns=50,pipe=1),
    cell("lrange_c1m","lrange",1000000,val=8,conns=16,pipe=1),
    cell("rpushtail_hot","rpushtail",2000000,mode='req',val=1000000),   # hold K5 push
    cell("lpushhead_hot","lpushhead",2000000,mode='req',val=1000000),
    cell("lpop_hot","lpop",2000000,mode='req',val=1000000),             # v1 miss
    cell("rpoplpush_hot","rpoplpush",2000000,mode='req',val=1000000),   # v1 miss
    cell("linsert_hot","linsert",200000,mode='req',val=400000),         # v1 worst cell
    cell("lrem_hot","lrem",2000000,mode='req',val=1000000),             # first valid cell
    cell("band_100","lindex",100,val=4), cell("band_500","lindex",500,val=4),
    cell("band_2000","lindex",2000,val=4), cell("band_130k","lindex",130000,val=4),
]

BUDGET = 460
def main():
    cells = {("m2","main"):M2_MAIN,("m2","alg"):M2_ALG,
             ("m3","main"):M3_MAIN,("m3","alg"):[]}[(MS,GROUP)]
    todo = [c for c in cells if not os.path.exists(f"{OUT}/{c['name']}.json")]
    if not todo:
        print("ALL CELLS DONE", MS, GROUP); return
    shards = 1 if GROUP == "alg" else 4
    if not launch(shards): sys.exit(1)
    base = {n:vmhwm(pid(n)) for n in ('aki','redis','valkey')}
    json.dump(base, open(f"{OUT}/_baseline_{GROUP}.json","w"))
    print("baseline",MS,GROUP,base, flush=True)
    start = time.time()
    for c in todo:
        if time.time()-start > BUDGET:
            print("BUDGET reached, exiting clean", flush=True); break
        run_cell(c)
    killall()
    print("BATCH DONE", MS, GROUP, flush=True)

if __name__ == "__main__": main()
