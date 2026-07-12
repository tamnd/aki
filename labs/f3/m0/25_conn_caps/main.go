// Lab: the per-connection hop-transport caps as a swept memory lever (spec
// 2064/f3, M0 gate follow-up, lab 25; follows lab 24's located lever).
//
// The question: lab 24 located the M0 memory-bar overage in the per-connection
// hop transport, not the arena (lab 23) and not the socket buffers (lab 24):
// each Conn pools hopBatch nodes carrying a data buffer (batchDataCap 8192) and
// a reply buffer (repCap 10240), plus a replyRing(1024) reorder ring of ~40B
// parked entries. A box rebuild shrinking all three at once dropped c512 VmHWM
// 231,616 -> 173,748 kB (58MB) with SET throughput bit-identical. That combined
// number could not say which cap paid, nor whether a smaller batchDataCap costs
// throughput for larger pipelined values (it is a node split threshold: a fuller
// node splits at the cap, so a 1KiB cap splits a P16 run of 256B values into
// four nodes where the 8KiB cap held one). This lab makes the three caps
// shard.Config fields (Config.BatchDataCap / ReplyRing / FreeListCap, wired
// through drivers.Options) and sweeps them to (1) attribute the memory saving to
// each cap and (2) find the batchDataCap floor that still holds throughput
// across a value band.
//
// Method: `go run .` boots the real in-process server (drivers.Listen, goroutine
// driver) on loopback and drives it with a pipelined SET load of a 1M keyspace,
// then reads VmHWM. It runs two sweeps, each cell a re-exec'd child so VmHWM is
// per-config:
//
//	sweep 1 (attribution), c512 P16 64B: baseline (all defaults), then each cap
//	shrunk alone (batchDataCap 8192->1024, replyRing 1024->128, freeListCap
//	64->8), then all three together (the box A/B combo). The per-cap VmHWM delta
//	names which cap carries the footprint.
//
//	sweep 2 (throughput floor), c512 P16, batchDataCap {8192,4096,2048,1024} x
//	value {64,256,1024}: the ratio of each cell's ops/s to its value-size
//	baseline shows where a smaller data cap starts splitting nodes often enough
//	to cost throughput. A cap that holds throughput at 64B but loses it at 1KiB
//	is safe only as a default if the gate stays small-value.
//
// The harness co-locates the load generator (one client goroutine per conn, each
// with its own 64KiB bufio pair), so its absolute VmHWM is client+server, not
// server-only: the absolute figure is the box's job (lab 24 A/B), and this lab
// reads the SHAPE of the two sweeps. macOS VmHWM is directional (the
// Go-scavenger madvise artifact); the shape holds on either platform.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tamnd/aki/f3srv/drivers"
)

var (
	child    = flag.Bool("child", false, "child mode: run one cell from the knob flags below")
	bcap     = flag.Int("bcap", 0, "Config.BatchDataCap, 0 = tuning default (child mode)")
	ring     = flag.Int("ring", 0, "Config.ReplyRing, 0 = tuning default (child mode)")
	free     = flag.Int("free", 0, "Config.FreeListCap, 0 = tuning default (child mode)")
	label    = flag.String("label", "", "cell label for the row (child mode)")
	shards   = flag.Int("shards", 8, "server shards, the box gate default")
	keys     = flag.Int("keys", 1<<20, "distinct keys, the gate -r keyspace")
	perConn  = flag.Int("per-conn", 20000, "SET commands per connection")
	conns    = flag.Int("conns", 512, "client connections")
	pipe     = flag.Int("pipe", 16, "pipeline depth, the P16 gate")
	valBytes = flag.Int("val", 64, "value bytes")
)

type xorshift uint64

func (x *xorshift) next() uint64 {
	v := *x
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	*x = v
	return uint64(v)
}

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
		hwm /= 1024
	}
	return hwm
}

// loadConn drives one connection: perConn SET commands in pipes of `pipe`,
// random keys over the keyspace, reading the +OK replies back so the server
// keeps draining and the connection reaches steady state.
func loadConn(nc net.Conn, seed uint64, perConn, pipe, keys, valBytes int) error {
	val := make([]byte, valBytes)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	bw := bufio.NewWriterSize(nc, 64<<10)
	br := bufio.NewReaderSize(nc, 64<<10)
	r := xorshift(seed | 1)
	var kb [16]byte
	writeSet := func() {
		n := r.next() % uint64(keys)
		binary.LittleEndian.PutUint64(kb[:8], n)
		binary.LittleEndian.PutUint64(kb[8:], n*0x9e3779b97f4a7c15)
		k := kb[:]
		// bufio.Writer defers errors to Flush, checked in the caller below.
		_, _ = fmt.Fprintf(bw, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n", len(k), k, len(val))
		_, _ = bw.Write(val)
		_, _ = bw.WriteString("\r\n")
	}
	done := 0
	for done < perConn {
		batch := pipe
		if rem := perConn - done; rem < batch {
			batch = rem
		}
		for i := 0; i < batch; i++ {
			writeSet()
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		for i := 0; i < batch; i++ {
			if _, err := br.ReadString('\n'); err != nil {
				return err
			}
		}
		done += batch
	}
	return nil
}

func runCell() {
	o := drivers.Options{
		Addr:         "127.0.0.1:0",
		Shards:       *shards,
		NetDriver:    drivers.NetGoroutine,
		BatchDataCap: *bcap,
		ReplyRing:    *ring,
		FreeListCap:  *free,
	}
	srv, err := drivers.Listen(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	go func() { _ = srv.Serve() }()
	addr := srv.Addr().String()

	ncs := make([]net.Conn, *conns)
	for i := range ncs {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Fprintln(os.Stderr, "dial:", err)
			os.Exit(1)
		}
		ncs[i] = c
	}

	start := time.Now()
	var wg sync.WaitGroup
	var failed sync.Once
	var loadErr error
	for i, c := range ncs {
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			if err := loadConn(c, uint64(0x2064*(i+1)), *perConn, *pipe, *keys, *valBytes); err != nil {
				failed.Do(func() { loadErr = err })
			}
		}(i, c)
	}
	wg.Wait()
	el := time.Since(start)
	if loadErr != nil {
		fmt.Fprintln(os.Stderr, "load:", loadErr)
		os.Exit(1)
	}

	ops := float64(*conns) * float64(*perConn)
	fmt.Printf("%-22s val=%4dB VmHWM=%7d kB ops/s=%11.0f\n",
		*label, *valBytes, vmHWM(), ops/el.Seconds())

	for _, c := range ncs {
		_ = c.Close()
	}
	_ = srv.Close()
}

func main() {
	flag.Parse()
	if *child {
		runCell()
		return
	}
	fmt.Printf("conn caps: %d shards, %d keys, %d/conn P%d, c%d\n",
		*shards, *keys, *perConn, *pipe, *conns)
	fmt.Printf("box rival bar for the 1M/64B dataset: redis 151MB, valkey 126MB VmHWM\n")
	fmt.Printf("tuning defaults: batchDataCap 8192, replyRing 1024, freeListCap 64\n")

	run := func(label string, bc, rg, fr, val, nc int) {
		cmd := exec.Command(os.Args[0], "-child",
			"-label", label,
			"-bcap", strconv.Itoa(bc), "-ring", strconv.Itoa(rg), "-free", strconv.Itoa(fr),
			"-shards", strconv.Itoa(*shards), "-keys", strconv.Itoa(*keys),
			"-per-conn", strconv.Itoa(*perConn), "-pipe", strconv.Itoa(*pipe),
			"-conns", strconv.Itoa(nc), "-val", strconv.Itoa(val))
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "cell:", err)
			os.Exit(1)
		}
	}

	fmt.Println("-- sweep 1: attribution, c512 P16 64B (each cap shrunk alone, then all three) --")
	run("baseline-all-default", 0, 0, 0, 64, 512)
	run("batchDataCap=1024", 1024, 0, 0, 64, 512)
	run("replyRing=128", 0, 128, 0, 64, 512)
	run("freeListCap=8", 0, 0, 8, 64, 512)
	run("all-three-small", 1024, 128, 8, 64, 512)

	fmt.Println("-- sweep 2: throughput floor, c512 P16, batchDataCap x value size --")
	for _, val := range []int{64, 256, 1024} {
		for _, bc := range []int{0, 4096, 2048, 1024} {
			lbl := "batchDataCap=default"
			if bc != 0 {
				lbl = fmt.Sprintf("batchDataCap=%d", bc)
			}
			run(lbl, bc, 0, 0, val, 512)
		}
	}
}
