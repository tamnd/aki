// Lab: transport decomposition from counters (spec 2064/f3, M10 pull-forward
// slice 1, lab 14, issue #542).
//
// The question: the per-op cost decomposition of the gate cells has so far
// come from CPU profiles (issue #542 charged the pre-lab-11 GET 64B P16 c128
// cell 876 ns/op to write syscalls, 537 to reads, 383 to wakes, 118 to parks
// of 2045 total). Profiles are manual and per-box; the akinet counters (doc
// 08 section 9.5) are readable per run. Can counters/op times a calibrated
// per-event cost reproduce the profile's decomposition? If yes, this lab is
// the per-slice recovery meter for the reactor slices: each slice reruns it
// and reads which band its change moved, no profile needed.
//
// Method: calibrate the four event costs with microbenches in this process
// (write(2) and read(2) from a concurrency-matched loopback echo herd where
// every write lands in a kevent-parked peer, and the waker's wake and park
// through a channel ping-pong: the wake is the producer-visible send to a
// parked consumer, the park is the pair's CPU residue after the wakes are
// subtracted). Then one
// f3srv per cell on loopback, in process, 1M keys at 64B, pipelined GET
// rounds: the P16 c128 gate shape and a P1 c50 latency row. Per cell: ops/s,
// process CPU per op (server plus client, same caveat as lab 11), the akinet
// counters per op from the drivers NetStats snapshot, and each band's
// estimated ns/op as counters/op times the calibrated cost.
//
// -profile fetches a CPU profile from the server's pprof endpoint during the
// P16 cell and writes it next to a path it prints; the README cross-checks
// the counter decomposition against it.
//
// See README.md for the numbers and the frozen verdict.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tamnd/aki/f3srv/drivers"
)

const (
	shards  = 4
	nKeys   = 1 << 20
	valSize = 64
)

func key(i int) string { return fmt.Sprintf("k%08d", i) }

// replyBytes is one GET reply on the wire at valSize: $64\r\n<64>\r\n.
func replyBytes() int { return 1 + len(fmt.Sprint(valSize)) + 2 + valSize + 2 }

// reqBytes is one GET request on the wire for the fixed-width keys.
func reqBytes() int { return len(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key(0)), key(0))) }

func cpuNow() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// costs holds the calibrated per-event costs the decomposition multiplies
// counters/op with.
type costs struct {
	write time.Duration // one write(2), P16 reply round sized
	read  time.Duration // one read(2), P16 request round sized
	wake  time.Duration // one wake token sent to a parked goroutine
	park  time.Duration // one park taken and later resumed
}

// tcpPair dials a loopback pair for the syscall calibrations.
func tcpPair() (a, b net.Conn, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = ln.Close() }()
	done := make(chan error, 1)
	go func() {
		var e error
		b, e = ln.Accept()
		done <- e
	}()
	a, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		return nil, nil, err
	}
	if err = <-done; err != nil {
		_ = a.Close()
		return nil, nil, err
	}
	return a, b, nil
}

// calibrateSyscalls prices a socket write and a socket read in the regime the
// server actually pays them: many connections on the process's one kqueue,
// each write landing in a peer that is parked in a netpoller read, the box
// busy. An isolated hot pair measures the bare syscall (about 300ns for the
// read, 1 to 2us for the write on this box) and misses most of the loaded
// cost, which is kernel-side: waking a kevent-parked peer, socket and kqueue
// locking under concurrency.
//
// The shape is `pairs` echo pairs, a P16 request round up and a P16 reply
// round down, one round in flight per pair. The server-side reply write and
// the client-side request write are timed directly (a write to a fat socket
// buffer never parks, so its wall clock is its cost, preemption noise
// included). The two reads park and get woken, so their wall clock is mostly
// wait; they are priced as the CPU residue, process CPU per round minus the
// two timed writes, split evenly between the two reads. That residue folds
// the netpoller and scheduler share of a woken read into the read cost, which
// is also what a profile's read band sees on the server.
func calibrateSyscalls(pairs, iters int) (write, read time.Duration, err error) {
	type pr struct{ a, b net.Conn }
	ps := make([]pr, pairs)
	for i := range ps {
		a, b, e := tcpPair()
		if e != nil {
			return 0, 0, e
		}
		ps[i] = pr{a, b}
		defer func() { _ = a.Close(); _ = b.Close() }()
	}
	var srvW, cliW, rounds atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range ps {
		wg.Add(1)
		go func(p pr) { // the server shape: read a request round, write a reply round
			defer wg.Done()
			req := make([]byte, 16*reqBytes())
			rep := make([]byte, 16*replyBytes())
			var w int64
			<-start
			for j := 0; j < iters; j++ {
				if _, err := io.ReadFull(p.b, req); err != nil {
					return
				}
				t0 := time.Now()
				if _, err := p.b.Write(rep); err != nil {
					return
				}
				w += time.Since(t0).Nanoseconds()
			}
			srvW.Add(w)
		}(ps[i])
		wg.Add(1)
		go func(p pr) { // the client shape: write a request round, read the reply round
			defer wg.Done()
			req := make([]byte, 16*reqBytes())
			rep := make([]byte, 16*replyBytes())
			var w int64
			<-start
			for j := 0; j < iters; j++ {
				t0 := time.Now()
				if _, err := p.a.Write(req); err != nil {
					return
				}
				w += time.Since(t0).Nanoseconds()
				if _, err := io.ReadFull(p.a, rep); err != nil {
					return
				}
				rounds.Add(1)
			}
			cliW.Add(w)
		}(ps[i])
	}
	cpu0 := cpuNow()
	close(start)
	wg.Wait()
	cpu := cpuNow() - cpu0
	n := rounds.Load()
	if n == 0 {
		return 0, 0, fmt.Errorf("calibration pairs made no progress")
	}
	write = time.Duration(srvW.Load() / n)
	cliWrite := time.Duration(cliW.Load() / n)
	if residue := cpu/time.Duration(n) - write - cliWrite; residue > 0 {
		read = residue / 2
	}
	return write, read, nil
}

// calibrateWakePark prices the waker protocol's two ends with a channel
// ping-pong, the same cap-1 token channels the shard waker blocks on. The
// wake is measured directly: the producer times its send to a consumer that
// is parked on the receive (the send pays the goready and the M wakeup, which
// is the cond-signal cost the profiles name). The park has no direct clock:
// it is priced as the pair's CPU residue, process CPU per iteration minus the
// two measured wakes, halved, which folds the gopark, the semasleep, and the
// resume path into one figure per park.
func calibrateWakePark(iters int) (wake, park time.Duration) {
	a := make(chan struct{}, 1)
	b := make(chan struct{}, 1)
	go func() {
		for range a {
			b <- struct{}{}
		}
	}()
	// Warm the pair so both goroutines exist and the channels are hot.
	for i := 0; i < 1000; i++ {
		a <- struct{}{}
		<-b
	}
	var sendNs int64
	cpu0 := cpuNow()
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		a <- struct{}{}
		sendNs += time.Since(t0).Nanoseconds()
		<-b
	}
	cpu := cpuNow() - cpu0
	close(a)
	wake = time.Duration(sendNs / int64(iters))
	perIter := cpu / time.Duration(iters)
	// One iteration is two wakes (each side wakes the other) and two parks.
	if residue := perIter - 2*wake; residue > 0 {
		park = residue / 2
	}
	return wake, park
}

// load SETs the whole keyspace at valSize, pipelined.
func load(addr string) error {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 1<<20)
	br := bufio.NewReaderSize(nc, 1<<20)
	val := make([]byte, valSize)
	for i := range val {
		val[i] = 'a' + byte(i%26)
	}
	const pipe = 128
	ok := make([]byte, 5) // +OK\r\n
	for i := 0; i < nKeys; i += pipe {
		m := pipe
		if i+m > nKeys {
			m = nKeys - i
		}
		for j := 0; j < m; j++ {
			k := key(i + j)
			_, _ = fmt.Fprintf(bw, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n", len(k), k, valSize)
			_, _ = bw.Write(val)
			_, _ = bw.WriteString("\r\n")
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		for j := 0; j < m; j++ {
			if _, err := io.ReadFull(br, ok); err != nil {
				return err
			}
			if ok[0] != '+' {
				return fmt.Errorf("SET reply %q", ok)
			}
		}
	}
	return nil
}

// hammer runs pipelined GET rounds at the given depth until deadline.
func hammer(addr string, pipeline int, seed uint64, deadline time.Time) (uint64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 64<<10)
	br := bufio.NewReaderSize(nc, 1<<20)

	round := make([]byte, pipeline*replyBytes())
	req := make([]byte, 0, pipeline*40)
	x := seed*0x9e3779b97f4a7c15 + 1
	ops := uint64(0)
	for time.Now().Before(deadline) {
		req = req[:0]
		for j := 0; j < pipeline; j++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			k := key(int(x % nKeys))
			req = append(req, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k)...)
		}
		if _, err := bw.Write(req); err != nil {
			return ops, err
		}
		if err := bw.Flush(); err != nil {
			return ops, err
		}
		if _, err := io.ReadFull(br, round); err != nil {
			return ops, err
		}
		if round[0] != '$' || round[1] == '-' {
			return ops, fmt.Errorf("GET reply starts %q", round[:2])
		}
		ops += uint64(pipeline)
	}
	return ops, nil
}

// perOp is the counter movement of one cell divided by its ops.
type perOp struct {
	writes, reads, batches, commands float64
	workerWakes, connWakes           float64
	workerParks, connParks           float64
	opsPerSec                        float64
	cpuPerOp                         time.Duration
}

func runCell(pipeline, conns int, dur time.Duration, profileTo string) (perOp, error) {
	opts := drivers.Options{Addr: "127.0.0.1:0", Shards: shards}
	if profileTo != "" {
		opts.PprofAddr = "127.0.0.1:0"
	}
	srv, err := drivers.Listen(opts)
	if err != nil {
		return perOp{}, err
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Close() }()
	addr := srv.Addr().String()
	if err := load(addr); err != nil {
		return perOp{}, err
	}

	var profErr error
	var profWg sync.WaitGroup
	if profileTo != "" {
		secs := int(dur.Seconds()) - 1
		if secs < 1 {
			secs = 1
		}
		url := fmt.Sprintf("http://%s/debug/pprof/profile?seconds=%d", srv.PprofAddr(), secs)
		profWg.Add(1)
		go func() {
			defer profWg.Done()
			profErr = fetchProfile(url, profileTo)
		}()
		// Let the profile window open before the traffic starts so it covers
		// steady state, not the ramp.
		time.Sleep(300 * time.Millisecond)
	}

	deadline := time.Now().Add(dur)
	ns0 := srv.NetStats()
	cpu0 := cpuNow()
	start := time.Now()
	var total atomic.Uint64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ops, err := hammer(addr, pipeline, uint64(c)+1, deadline)
			total.Add(ops)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		}(c)
	}
	wg.Wait()
	el := time.Since(start)
	cpu := cpuNow() - cpu0
	ns1 := srv.NetStats()
	profWg.Wait()
	if e := firstErr.Load(); e != nil {
		return perOp{}, e.(error)
	}
	if profErr != nil {
		return perOp{}, profErr
	}
	ops := float64(total.Load())
	d := func(b, a uint64) float64 { return float64(b-a) / ops }
	return perOp{
		writes:      d(ns1.WriteSyscalls, ns0.WriteSyscalls),
		reads:       d(ns1.ReadSyscalls, ns0.ReadSyscalls),
		batches:     d(ns1.Batches, ns0.Batches),
		commands:    d(ns1.Commands, ns0.Commands),
		workerWakes: d(ns1.WorkerWakes, ns0.WorkerWakes),
		connWakes:   d(ns1.ConnWakes, ns0.ConnWakes),
		workerParks: d(ns1.WorkerParks, ns0.WorkerParks),
		connParks:   d(ns1.ConnParks, ns0.ConnParks),
		opsPerSec:   ops / el.Seconds(),
		cpuPerOp:    time.Duration(int64(cpu) / int64(total.Load())),
	}, nil
}

func fetchProfile(url, path string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pprof fetch: %s", resp.Status)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func main() {
	dur := flag.Duration("dur", 5*time.Second, "measured window per cell")
	profile := flag.String("profile", "", "fetch a CPU profile during the P16 cell and write it here")
	flag.Parse()

	fmt.Printf("GET %dB, %d keys, %d shards, %s per cell, in process\n\n", valSize, nKeys, shards, *dur)

	const calPairs, calIters = 128, 5000
	wc, rc, err := calibrateSyscalls(calPairs, calIters)
	if err != nil {
		fmt.Fprintln(os.Stderr, "lab:", err)
		os.Exit(1)
	}
	wk, pk := calibrateWakePark(50000)
	c := costs{write: wc, read: rc, wake: wk, park: pk}
	fmt.Printf("calibration: write(2) %v, read(2) %v, wake %v, park %v\n\n", c.write, c.read, c.wake, c.park)

	type cellCfg struct {
		name      string
		pipeline  int
		conns     int
		profileTo string
	}
	cells := []cellCfg{
		{name: "P16 c128", pipeline: 16, conns: 128, profileTo: *profile},
		{name: "P1 c50", pipeline: 1, conns: 50},
	}
	for _, cc := range cells {
		p, err := runCell(cc.pipeline, cc.conns, *dur, cc.profileTo)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lab:", err)
			os.Exit(1)
		}
		est := func(n float64, cost time.Duration) float64 { return n * float64(cost.Nanoseconds()) }
		wr := est(p.writes, c.write)
		rd := est(p.reads, c.read)
		wa := est(p.workerWakes+p.connWakes, c.wake)
		pa := est(p.workerParks+p.connParks, c.park)
		fmt.Printf("%s: %.0f ops/s, CPU/op %v (server+client)\n", cc.name, p.opsPerSec, p.cpuPerOp)
		fmt.Printf("| counter/op | writes | reads | batches | cmds/batch | worker wakes | conn wakes | worker parks | conn parks |\n")
		fmt.Printf("|---|---|---|---|---|---|---|---|---|\n")
		fmt.Printf("| %s | %.3f | %.3f | %.3f | %.2f | %.3f | %.3f | %.3f | %.3f |\n",
			cc.name, p.writes, p.reads, p.batches, p.commands/p.batches,
			p.workerWakes, p.connWakes, p.workerParks, p.connParks)
		fmt.Printf("| est ns/op | writes | reads | wakes | parks | sum |\n")
		fmt.Printf("|---|---|---|---|---|---|\n")
		fmt.Printf("| %s | %.0f | %.0f | %.0f | %.0f | %.0f |\n\n", cc.name, wr, rd, wa, pa, wr+rd+wa+pa)
		if cc.profileTo != "" {
			fmt.Printf("profile written to %s\n\n", cc.profileTo)
		}
	}
}
