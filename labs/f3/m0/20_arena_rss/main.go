// Lab: arena backing vs GC pacing vs RSS (spec 2064/f3, M0 gate follow-up,
// lab 20, issue #542).
//
// The question: the M10 reactor campaign read 3.2-4.2x rival RSS on every
// 64B gate cell, on every driver arm, and the per-rep meta files show the
// shape of it: post-flush residuals climbing 163MB, 310MB, 396MB across reps
// while redis returns to 14MB, with the arena's own MADV_DONTNEED verifiably
// handing about 93MB back at each flush. So the growth is not arena pages.
// The suspect is heap accounting: a make([]byte) arena counts its full
// reservation as live heap, and the gate config (4 shards x 512MiB) makes
// that 2GiB, which puts the GC pacing goal past 4GiB and inflates the
// scavenger's retention target to match. Transient garbage, per-connection
// buffers, index tables rebuilt after Reset, per-op allocs, is then never
// collected and never returned, and it reads as permanent RSS. The fix under
// test maps the arena anonymously so the heap holds only the substrate
// objects (doc 04 section 6.2: the arena is accounted by our ledger, not by
// the Go heap).
//
// Method: `go run .` emulates the gate cell in process: 4 stores with
// 512MiB arenas each (the -val 1024 shape takes 1024MiB, matching the gate's
// --arena-mib), 1M keys of 16B key + 64B value spread across them, three
// reps of fill, conn-garbage churn (512 connections' worth of 64KiB read +
// 64KiB reply buffers allocated, touched, and dropped, cycled 8 times to
// stand in for the run's allocation traffic), then Reset on every store (the
// FLUSHALL). At each phase boundary it prints VmRSS and VmHWM from
// /proc/self/status (rusage maxrss off Linux), the runtime's HeapAlloc,
// HeapSys, HeapReleased, and NumGC, and the stores' own ledger. A/B is by
// commit: build at the parent of the arena-map change and at the change, run
// both on the same box, and read the post-flush RSS trajectory.
//
// See README.md for the predictions (filed before the box run), the
// numbers, and the verdict.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/tamnd/aki/engine/f3/store"
)

const (
	shards   = 4       // shard.DefaultShards on the 8-cpu gate mask
	keys     = 1 << 20 // 1M, the gate's -r keyspace
	conns    = 512     // the gate's high-conn shape
	connBuf  = 64 << 10
	churnCyc = 8 // conn-buffer alloc/drop cycles per rep
	reps     = 3 // the gate's rep count, flush between
)

var valSize = flag.Int("val", 64, "value bytes (64 or 1024, the gate cells)")

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

// rss reads VmRSS and VmHWM in KiB from /proc/self/status; off Linux only
// the high-water mark is available, via rusage.
func rss() (vmRSS, vmHWM uint64) {
	f, err := os.Open("/proc/self/status")
	if err == nil {
		defer func() { _ = f.Close() }()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
				vmRSS, _ = strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			}
			if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
				vmHWM, _ = strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			}
		}
		return vmRSS, vmHWM
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	hwm := uint64(ru.Maxrss)
	if runtime.GOOS == "darwin" {
		hwm /= 1024 // darwin reports bytes, linux KiB
	}
	return 0, hwm
}

func snap(phase string, stores []*store.Store) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var used, ledger uint64
	for _, s := range stores {
		u, _ := s.ArenaBytes()
		used += u
		ledger += s.Mem().UsedMemory()
	}
	vmRSS, vmHWM := rss()
	fmt.Printf("%-22s rss=%6.1fMB hwm=%6.1fMB heapAlloc=%7.1fMB heapSys=%7.1fMB released=%7.1fMB gc=%3d arenaUsed=%6.1fMB ledger=%6.1fMB\n",
		phase,
		float64(vmRSS)/1024, float64(vmHWM)/1024,
		float64(ms.HeapAlloc)/(1<<20), float64(ms.HeapSys)/(1<<20),
		float64(ms.HeapReleased)/(1<<20),
		ms.NumGC,
		float64(used)/(1<<20), float64(ledger)/(1<<20))
}

func main() {
	flag.Parse()
	arenaBytes := 512 << 20
	if *valSize > 64 {
		arenaBytes = 1024 << 20
	}
	stores := make([]*store.Store, shards)
	for i := range stores {
		stores[i] = store.New(arenaBytes, 0)
	}
	snap("launch", stores)

	kbuf := make([]byte, 16)
	val := make([]byte, *valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	sink := byte(0)
	for rep := 0; rep < reps; rep++ {
		// Fill: every key SET once, sharded by low bits like the runtime's
		// key hash fan-out.
		r := xorshift(0x2064 + uint64(rep))
		for i := 0; i < keys; i++ {
			n := r.next() % keys
			k := makeKey(kbuf, n)
			if err := stores[n%shards].Set(k, val); err != nil {
				fmt.Fprintln(os.Stderr, "set:", err)
				os.Exit(1)
			}
		}
		// Conn-garbage churn: the reactor and goroutine drivers' per-conn
		// read and reply buffers, allocated and dropped as connections would
		// cycle them. Touched so the pages are really resident.
		for c := 0; c < churnCyc; c++ {
			bufs := make([][]byte, 0, conns*2)
			for i := 0; i < conns; i++ {
				rb := make([]byte, connBuf)
				ob := make([]byte, connBuf)
				for j := 0; j < connBuf; j += 4096 {
					rb[j], ob[j] = byte(i), byte(c)
				}
				sink ^= rb[0] ^ ob[0]
				bufs = append(bufs, rb, ob)
			}
			_ = bufs
		}
		snap(fmt.Sprintf("rep%d post-fill", rep), stores)
		for _, s := range stores {
			s.Reset()
		}
		snap(fmt.Sprintf("rep%d post-flush", rep), stores)
	}
	_ = sink
}
