// Lab: arena footprint under overwrite churn vs the reclaim threshold (spec
// 2064/f3, M0 gate follow-up, lab 23; follows lab 20's named open lever).
//
// The question: the 64B/1M-key gate cell clears the throughput bar with room
// to spare (results/GamingPC/calibration/p16-512-1Mkey-vs-frozen.md: SET 3.42x,
// GET 2.57x vs CF16-tuned rivals), but fails the memory bar. Under the write
// heavy SET gate (n=10-15M SET into a 1M keyspace, so ~10-15 overwrites per key)
// the arena fills with superseded records and rides ~174-195MB resident on the
// box, against redis 151MB and valkey 126MB for the same dataset. Lab 20 fixed
// the GC-pacing breach (the arena maps anonymously now) and its frozen verdict
// named the remaining lever: at a value this small "the arena's dead-record
// slack and per-shard reservation outweigh a value that small". A box sweep of
// (shards x arena-mib) confirmed shard count and arena size are not the lever
// (2 shards fails the throughput bar; a big-enough arena rides ~174MB whatever
// its size, because the resident figure is dead-record slack, not reservation).
// The suspect is the reclaim threshold: a segment is a compaction victim only
// once dead*den >= fill*num, frozen at 1/4 by lab 10 as the throughput-neutral
// footprint knee. With the throughput gate now cleared by 2.5-3.4x, aki has CPU
// surplus to spend on a tighter threshold that keeps less dead slack resident.
//
// VERDICT: NEGATIVE, the threshold is not the lever (see README). On the box at
// the gate config (8 shards x 256MiB) every threshold rides ~128MB VmHWM with
// dead=0.0MB: the between-drain compaction keeps up with the overwrite churn so
// no dead slack survives to a measurement point, and the store alone (96MB arena
// live + ~32MB heap) is already UNDER redis's 151MB. The box's 190-228MB gate
// figure is store + per-connection buffers (64KiB read + 64KiB reply each): a
// c512->c50 probe moves VmHWM 228MB->145MB. The memory lever is per-connection
// buffer sizing at high fan-out, not the arena. Lab 24 takes that up.
//
// Method: `go run .` sweeps the threshold {1/2, 1/4 shipped, 1/8, 1/16} at the
// gate shape (default 4 shards, 32MiB arena each, 1M keys of 16B key + 64B
// value, ~15 overwrite rounds), each cell in its own re-exec'd child process so
// VmHWM is per-config and never carries a prior cell's high-water mark. Each
// child fills then overwrites, calling CompactArena at a drain-boundary cadence
// the way the shard worker does between drain passes, then prints VmHWM and the
// arena live/alloc ledger (dead = alloc - live) and the write loop's ops/sec.
// The rival bar (redis 151MB, valkey 126MB VmHWM for this dataset) is the line
// the resident figure has to drop under while the ops/sec holds.
//
// See README.md for predictions (filed before the box run), numbers, verdict.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/f3/store"
)

var (
	cell      = flag.String("cell", "", "run one threshold cell num/den (child mode); empty drives the sweep")
	shards    = flag.Int("shards", 8, "shard stores, the box gate default")
	arenaMiB  = flag.Int("arena-mib", 256, "arena MiB per shard, the box gate default")
	keys      = flag.Int("keys", 1<<20, "distinct keys, the gate -r keyspace")
	rounds    = flag.Int("rounds", 15, "write rounds of `keys` writes each (~gate n)")
	drainEach = flag.Int("drain", 4096, "writes per store between CompactArena passes")
)

// thresholds swept: dead-fraction num/den, a segment is a victim past dead*den>=fill*num.
var thresholds = [][2]uint64{{1, 2}, {1, 4}, {1, 8}, {1, 16}}

type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

func makeKey(buf []byte, n uint64) []byte {
	binary.LittleEndian.PutUint64(buf[0:8], n)
	binary.LittleEndian.PutUint64(buf[8:16], n*0x9e3779b97f4a7c15)
	return buf[:16]
}

// rss reads VmHWM (peak) in KiB from /proc/self/status, rusage maxrss elsewhere.
func vmHWM() uint64 {
	f, err := os.Open("/proc/self/status")
	if err == nil {
		defer func() { _ = f.Close() }()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if v, ok := strings.CutPrefix(sc.Text(), "VmHWM:"); ok {
				n, _ := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
				return n
			}
		}
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	hwm := uint64(ru.Maxrss)
	if runtime.GOOS == "darwin" {
		hwm /= 1024 // darwin bytes, linux KiB
	}
	return hwm
}

// runCell fills and overwrites under one reclaim threshold and reports the
// resident peak, the arena ledger, and the write ops/sec.
func runCell(num, den uint64) {
	arenaBytes := *arenaMiB << 20
	stores := make([]*store.Store, *shards)
	for i := range stores {
		stores[i] = store.New(arenaBytes, 0)
		stores[i].TuneArenaReclaim(num, den)
	}
	kbuf := make([]byte, 16)
	val := make([]byte, 64)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	since := make([]int, *shards)
	start := time.Now()
	var writes int64
	for rep := 0; rep < *rounds; rep++ {
		r := xorshift(0x2064 + uint64(rep))
		for i := 0; i < *keys; i++ {
			n := r.next() % uint64(*keys)
			sh := n % uint64(*shards)
			if err := stores[sh].Set(makeKey(kbuf, n), val); err != nil {
				fmt.Fprintln(os.Stderr, "set:", err)
				os.Exit(1)
			}
			writes++
			since[sh]++
			// The worker compacts between drain passes and reads ArenaTight
			// every pass as backpressure; emulate both: the periodic
			// between-drain reclaim and the tight-arena trigger.
			if since[sh] >= *drainEach || stores[sh].ArenaTight() {
				stores[sh].CompactArena()
				since[sh] = 0
			}
		}
	}
	el := time.Since(start)
	// One last reclaim at the idle boundary, as the worker would when the pipe drains.
	for _, s := range stores {
		s.CompactArena()
	}

	var live, alloc, keysLive uint64
	for _, s := range stores {
		m := s.Mem()
		live += m.ArenaLiveBytes
		alloc += m.ArenaAllocBytes
		keysLive += m.Keys
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	mb := func(b uint64) float64 { return float64(b) / (1 << 20) }
	fmt.Printf("thresh=%d/%-2d VmHWM=%6.1fMB arenaLive=%6.1fMB arenaAlloc=%6.1fMB dead=%6.1fMB heapSys=%6.1fMB keys=%d ops/s=%8.0f\n",
		num, den, mb(vmHWM()*1024), mb(live), mb(alloc), mb(alloc-live), mb(ms.HeapSys),
		keysLive, float64(writes)/el.Seconds())
}

func main() {
	flag.Parse()
	if *cell != "" {
		parts := strings.SplitN(*cell, "/", 2)
		num, _ := strconv.ParseUint(parts[0], 10, 64)
		den, _ := strconv.ParseUint(parts[1], 10, 64)
		runCell(num, den)
		return
	}
	// Driver: one child per threshold so VmHWM is isolated per cell.
	fmt.Printf("arena-overwrite footprint: %d shards x %dMiB, %d keys, %d rounds (~%dM writes)\n",
		*shards, *arenaMiB, *keys, *rounds, (*rounds**keys)/1e6)
	fmt.Printf("rival bar for this dataset (box VmHWM): redis 151MB, valkey 126MB\n")
	for _, t := range thresholds {
		cmd := exec.Command(os.Args[0],
			"-cell", fmt.Sprintf("%d/%d", t[0], t[1]),
			"-shards", strconv.Itoa(*shards), "-arena-mib", strconv.Itoa(*arenaMiB),
			"-keys", strconv.Itoa(*keys), "-rounds", strconv.Itoa(*rounds),
			"-drain", strconv.Itoa(*drainEach))
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "cell:", err)
			os.Exit(1)
		}
	}
}
