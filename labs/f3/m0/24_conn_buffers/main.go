// Lab: the c512 connection-footprint lever (spec 2064/f3, M0 gate follow-up,
// lab 24; follows lab 23's named lever).
//
// The question: lab 23 proved the arena reclaim threshold is NOT the M0 memory
// lever (dead=0 at every threshold, the store alone rides ~128MB, under redis's
// 151MB). A c512-vs-c50 box probe then showed the gate's 190-228MB is the
// connection fabric: same 1M/64B dataset, only the client connection count
// differing, VmHWM moved 228MB -> 145MB. This lab isolates WHICH per-connection
// structure carries that cost.
//
// The obvious suspect was the socket buffers: every connection pins a 64KiB read
// buffer and a 64KiB reply buffer (f3srv/drivers/server.go readBufSize /
// replyBufSize). This lab refutes that: a box sweep of -read-buf-kib/-reply-buf-
// kib {64,32,16,8,4} left VmHWM flat at ~222-232MB, because the reactor leases
// those buffers from a per-loop free list and they are a small share of the
// per-connection cost. The real cost is the per-connection hop transport: each
// Conn pools up to freeListCap(64) hopBatch nodes, each carrying a data buffer
// (batchDataCap 8192) and a reply buffer (repCap 10240), plus a replyRing(1024)
// reorder ring of parked entries (~40B each). A box rebuild that shrank those
// three caps (batchDataCap 8192->1024, replyRing 1024->128, freeListCap 64->8)
// dropped c512 VmHWM 231,616 -> 173,748 kB, 58MB, with SET throughput unchanged
// (the two-client summed rate was identical, so the smaller caps cost nothing at
// the 64B gate cell where a hop node holds ~640B). See README for the full box
// A/B and the verdict.
//
// Method: `go run .` boots the real in-process server (drivers.Listen, goroutine
// driver) and drives it over loopback TCP with a pipelined SET load of a 1M
// keyspace at 64B values, then reads VmHWM. It runs two sweeps, each cell a
// re-exec'd child so VmHWM is per-config: (1) connection count {50,128,256,512}
// at the default buffers, reproducing the count-scaling signal; (2) read+reply
// buffer size {64,16,8,4 KiB} at fixed c512, reproducing the flat-VmHWM
// refutation. The const-level sweep (the actual lever) is not runtime-vary-able
// yet, so its evidence is the box rebuild in README; making batchDataCap /
// replyRing / freeListCap shard.Config fields is the fix lab this one motivates.
//
// macOS VmHWM is directional only (Go-scavenger madvise artifact, the gate is
// Linux-authoritative); the shape of both sweeps holds on either platform.
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
	cell     = flag.String("cell", "", "child mode: run one cell conns/readkib; empty drives the sweeps")
	shards   = flag.Int("shards", 8, "server shards, the box gate default")
	keys     = flag.Int("keys", 1<<20, "distinct keys, the gate -r keyspace")
	perConn  = flag.Int("per-conn", 20000, "SET commands per connection")
	pipe     = flag.Int("pipe", 16, "pipeline depth, the P16 gate")
	valBytes = flag.Int("val", 64, "value bytes, the 64B gate cell")
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
		// Read one +OK line per queued SET.
		for i := 0; i < batch; i++ {
			if _, err := br.ReadString('\n'); err != nil {
				return err
			}
		}
		done += batch
	}
	return nil
}

func runCell(nConns, readBytes int) {
	o := drivers.Options{
		Addr:          "127.0.0.1:0",
		Shards:        *shards,
		NetDriver:     drivers.NetGoroutine,
		ReadBufBytes:  readBytes,
		ReplyBufBytes: readBytes,
	}
	srv, err := drivers.Listen(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	go func() { _ = srv.Serve() }()
	addr := srv.Addr().String()

	ncs := make([]net.Conn, nConns)
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

	ops := float64(nConns) * float64(*perConn)
	rk := readBytes >> 10
	if rk == 0 {
		rk = 64
	}
	fmt.Printf("conns=%4d readbuf=%2dKiB VmHWM=%7d kB ops/s=%10.0f\n",
		nConns, rk, vmHWM(), ops/el.Seconds())

	for _, c := range ncs {
		_ = c.Close()
	}
	_ = srv.Close()
}

func main() {
	flag.Parse()
	if *cell != "" {
		parts := strings.SplitN(*cell, "/", 2)
		nc, _ := strconv.Atoi(parts[0])
		rb, _ := strconv.Atoi(parts[1])
		runCell(nc, rb)
		return
	}
	fmt.Printf("conn footprint: %d shards, %d keys, %d/conn P%d, %dB values\n",
		*shards, *keys, *perConn, *pipe, *valBytes)
	fmt.Printf("box rival bar for this dataset: redis 151MB, valkey 126MB VmHWM\n")

	child := func(nc, rb int) {
		cmd := exec.Command(os.Args[0],
			"-cell", fmt.Sprintf("%d/%d", nc, rb),
			"-shards", strconv.Itoa(*shards), "-keys", strconv.Itoa(*keys),
			"-per-conn", strconv.Itoa(*perConn), "-pipe", strconv.Itoa(*pipe),
			"-val", strconv.Itoa(*valBytes))
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "cell:", err)
			os.Exit(1)
		}
	}

	fmt.Println("-- sweep 1: connection count at default buffers (footprint scales with count) --")
	for _, nc := range []int{50, 128, 256, 512} {
		child(nc, 0)
	}
	fmt.Println("-- sweep 2: read+reply buffer size at c512 (footprint flat: buffers are not the lever) --")
	for _, rk := range []int{64, 16, 8, 4} {
		child(512, rk<<10)
	}
}
