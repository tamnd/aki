// Lab: single-goroutine connection loop vs the reader/writer pair (spec
// 2064/f3, M10 pull-forward slice 2, lab 15, issue #542).
//
// The question: doc 08 section 4.1 prescribes one goroutine per connection;
// the M0 driver shipped a reader/writer pair. The pair costs one goroutine
// per connection plus a worker-to-writer channel wake and a writer park per
// request-reply round. Lab 14's decomposition priced the wake band at ~1.9
// wakes/op at P1 c50 (worker wake plus conn wake) and a few ns/op at P16, so
// the prediction is: P1 gains latency and throughput (one goroutine handoff
// removed per round), P16 within noise, writes/op unchanged at 1 per round
// (the boundary flush discipline is shape-independent).
//
// Method: one in-process f3srv per cell, loopback, pipelined GET rounds. The
// sweep is shape {single, pair} x {P1 c50, P16 c128} x {GET 64B, GET 1KiB}.
// Per cell: ops/s, process CPU per op (server plus client, same caveat as
// labs 11/14), the akinet counters per op from the drivers NetStats snapshot
// (wakes, parks, writes, reads), and at P1 the client-observed RTT p50/p99.
//
// See README.md for the numbers and the frozen verdict.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tamnd/aki/f3srv/drivers"
)

const shards = 4

func key(i int) string { return fmt.Sprintf("k%08d", i) }

// replyBytes is one GET reply on the wire: $<len>\r\n<val>\r\n.
func replyBytes(valSize int) int { return 1 + len(fmt.Sprint(valSize)) + 2 + valSize + 2 }

func cpuNow() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// load SETs the whole keyspace at valSize, pipelined.
func load(addr string, nKeys, valSize int) error {
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

// hammer runs pipelined GET rounds at the given depth until deadline. At
// pipeline 1 it records each round's RTT in nanoseconds for the latency
// percentiles; deeper pipelines skip the clock, throughput is the metric.
func hammer(addr string, pipeline, nKeys, valSize int, seed uint64, deadline time.Time) (uint64, []int64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 64<<10)
	br := bufio.NewReaderSize(nc, 1<<20)

	round := make([]byte, pipeline*replyBytes(valSize))
	req := make([]byte, 0, pipeline*40)
	var lats []int64
	if pipeline == 1 {
		lats = make([]int64, 0, 1<<19)
	}
	x := seed*0x9e3779b97f4a7c15 + 1
	ops := uint64(0)
	for time.Now().Before(deadline) {
		req = req[:0]
		for j := 0; j < pipeline; j++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			k := key(int(x % uint64(nKeys)))
			req = append(req, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k)...)
		}
		var t0 time.Time
		if pipeline == 1 {
			t0 = time.Now()
		}
		if _, err := bw.Write(req); err != nil {
			return ops, lats, err
		}
		if err := bw.Flush(); err != nil {
			return ops, lats, err
		}
		if _, err := io.ReadFull(br, round); err != nil {
			return ops, lats, err
		}
		if pipeline == 1 {
			lats = append(lats, time.Since(t0).Nanoseconds())
		}
		if round[0] != '$' || round[1] == '-' {
			return ops, lats, fmt.Errorf("GET reply starts %q", round[:2])
		}
		ops += uint64(pipeline)
	}
	return ops, lats, nil
}

// perOp is one cell's outcome: counter movement per op, throughput, CPU, and
// the P1 latency percentiles (zero at deeper pipelines).
type perOp struct {
	writes, reads          float64
	workerWakes, connWakes float64
	workerParks, connParks float64
	opsPerSec              float64
	cpuPerOp               time.Duration
	p50, p99               time.Duration
}

func pct(sorted []int64, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return time.Duration(sorted[i])
}

func runCell(shape string, pipeline, conns, nKeys, valSize int, dur time.Duration) (perOp, error) {
	srv, err := drivers.Listen(drivers.Options{Addr: "127.0.0.1:0", Shards: shards, ConnShape: shape})
	if err != nil {
		return perOp{}, err
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Close() }()
	addr := srv.Addr().String()
	if err := load(addr, nKeys, valSize); err != nil {
		return perOp{}, err
	}
	if got := srv.NetStats().Shape; got != shape {
		return perOp{}, fmt.Errorf("server reports shape %q, want %q", got, shape)
	}

	deadline := time.Now().Add(dur)
	ns0 := srv.NetStats()
	cpu0 := cpuNow()
	start := time.Now()
	var total atomic.Uint64
	var firstErr atomic.Value
	var mu sync.Mutex
	var allLats []int64
	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ops, lats, err := hammer(addr, pipeline, nKeys, valSize, uint64(c)+1, deadline)
			total.Add(ops)
			if len(lats) > 0 {
				mu.Lock()
				allLats = append(allLats, lats...)
				mu.Unlock()
			}
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		}(c)
	}
	wg.Wait()
	el := time.Since(start)
	cpu := cpuNow() - cpu0
	ns1 := srv.NetStats()
	if e := firstErr.Load(); e != nil {
		return perOp{}, e.(error)
	}
	ops := float64(total.Load())
	d := func(b, a uint64) float64 { return float64(b-a) / ops }
	p := perOp{
		writes:      d(ns1.WriteSyscalls, ns0.WriteSyscalls),
		reads:       d(ns1.ReadSyscalls, ns0.ReadSyscalls),
		workerWakes: d(ns1.WorkerWakes, ns0.WorkerWakes),
		connWakes:   d(ns1.ConnWakes, ns0.ConnWakes),
		workerParks: d(ns1.WorkerParks, ns0.WorkerParks),
		connParks:   d(ns1.ConnParks, ns0.ConnParks),
		opsPerSec:   ops / el.Seconds(),
		cpuPerOp:    time.Duration(int64(cpu) / int64(total.Load())),
	}
	if len(allLats) > 0 {
		sort.Slice(allLats, func(i, j int) bool { return allLats[i] < allLats[j] })
		p.p50 = pct(allLats, 0.50)
		p.p99 = pct(allLats, 0.99)
	}
	return p, nil
}

func main() {
	dur := flag.Duration("dur", 5*time.Second, "measured window per cell")
	flag.Parse()

	fmt.Printf("GET, %d shards, %s per cell, in process, loopback\n\n", shards, *dur)
	fmt.Printf("| cell | shape | ops/s | CPU/op | writes/op | reads/op | wakes/op (wk+conn) | parks/op (wk+conn) | p50 | p99 |\n")
	fmt.Printf("|---|---|---|---|---|---|---|---|---|---|\n")

	type sizeCfg struct {
		valSize, nKeys int
		label          string
	}
	sizes := []sizeCfg{
		{valSize: 64, nKeys: 1 << 20, label: "64B"},
		{valSize: 1 << 10, nKeys: 1 << 18, label: "1KiB"},
	}
	type cellCfg struct {
		name     string
		pipeline int
		conns    int
	}
	cells := []cellCfg{
		{name: "P1 c50", pipeline: 1, conns: 50},
		{name: "P16 c128", pipeline: 16, conns: 128},
	}
	for _, sz := range sizes {
		for _, cc := range cells {
			for _, shape := range []string{drivers.ShapeSingle, drivers.ShapePair} {
				p, err := runCell(shape, cc.pipeline, cc.conns, sz.nKeys, sz.valSize, *dur)
				if err != nil {
					fmt.Fprintln(os.Stderr, "lab:", err)
					os.Exit(1)
				}
				lat50, lat99 := "-", "-"
				if p.p50 > 0 {
					lat50 = p.p50.String()
					lat99 = p.p99.String()
				}
				fmt.Printf("| GET %s %s | %s | %.0f | %v | %.3f | %.3f | %.3f+%.3f | %.3f+%.3f | %s | %s |\n",
					sz.label, cc.name, shape, p.opsPerSec, p.cpuPerOp,
					p.writes, p.reads, p.workerWakes, p.connWakes,
					p.workerParks, p.connParks, lat50, lat99)
			}
		}
	}
}
