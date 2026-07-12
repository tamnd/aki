// Lab: connection-writer spin vs park at pipe=1 (spec 2064/f3, M0 gate
// follow-up, lab 22).
//
// The question: the M0 point-op gate cells at pipe=1 (GET and SET) lose to
// redis and valkey at high connection counts, and the 512-conn CPU profile put
// about 40 percent of aki's CPU in the connection writer's completion spin
// (Conn.idleOnce polling the outbound queue before it parks, driven by
// spinWindow, tuning.go). Lab 11 already froze the shard worker's spin at 0 for
// the same saturated-cores reason, but the connection writer kept its 4us
// window, sized by lab 3 for a different regime. At high fan-out that spin is a
// core stolen from the eight shard workers draining 512 connections' worth of
// tiny replies. Does parking the writer immediately at high fan-out free those
// cores for real throughput, and where is the crossover below which the spin
// still pays?
//
// Method: one f3srv per cell on loopback, 8 shards, in process. Load nKeys keys
// at 64B, then C client connections run one GET at a time (pipe=1, the gate
// cell shape) for cellDur. The sweep is spin policy {spin: the writer always
// spins the full window; park: the writer parks immediately} x connections {32,
// 128, 256, 512}. The policy is set through shard.SetConnSpinHighWater: 1 parks
// every writer at once, a value past any live count keeps the unconditional
// spin. Per cell: GETs per second and process CPU per op (rusage user plus
// system; the loopback clients live in the same process, so the number is
// server plus client and only same-row comparisons mean anything).
//
// See README.md for the numbers, the gate-box A/B they reproduce, and the
// verdict frozen into engine/f3/shard/tuning.go (connSpinHighWater).
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/f3/shard"
	"github.com/tamnd/aki/f3srv/drivers"
)

const (
	shards   = 8
	nKeys    = 1 << 18
	valSize  = 64
	cellDur  = 2 * time.Second
	loadPipe = 128
	// alwaysSpin is a high-water past any connection count the sweep drives, so
	// the writer never collapses its spin: the fixed-spin control arm.
	alwaysSpin = 1 << 30
	// alwaysPark is the smallest high-water, so any live connection puts the
	// writer straight to park: the park-immediately arm.
	alwaysPark = 1
)

var connCounts = []int{32, 128, 256, 512}

func key(i int) string { return fmt.Sprintf("k%08d", i) }

// load SETs the whole keyspace at valSize, pipelined, and reads the +OK run.
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
	ok := make([]byte, 5) // +OK\r\n
	for i := 0; i < nKeys; i += loadPipe {
		m := loadPipe
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

// hammer runs one GET at a time (pipe=1) on one connection until deadline and
// returns the GET count. Every value is valSize, so a reply is exactly replyLen
// bytes.
func hammer(addr string, seed uint64, deadline time.Time) (uint64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 4<<10)
	br := bufio.NewReaderSize(nc, 4<<10)

	replyLen := 1 + len(fmt.Sprint(valSize)) + 2 + valSize + 2
	round := make([]byte, replyLen)
	x := seed*0x9e3779b97f4a7c15 + 1
	ops := uint64(0)
	for time.Now().Before(deadline) {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		k := key(int(x % nKeys))
		if _, err := fmt.Fprintf(bw, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k); err != nil {
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
		ops++
	}
	return ops, nil
}

func cpuNow() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

type cell struct {
	opsPerSec float64
	cpuPerOp  time.Duration
}

func runCell(highWater, conns int) (cell, error) {
	shard.SetConnSpinHighWater(highWater)
	srv, err := drivers.Listen(drivers.Options{
		Addr:   "127.0.0.1:0",
		Shards: shards,
	})
	if err != nil {
		return cell{}, err
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Close() }()
	addr := srv.Addr().String()
	if err := load(addr); err != nil {
		return cell{}, err
	}

	deadline := time.Now().Add(cellDur)
	cpu0 := cpuNow()
	start := time.Now()
	var total atomic.Uint64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ops, err := hammer(addr, uint64(c)+1, deadline)
			total.Add(ops)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
		}(c)
	}
	wg.Wait()
	el := time.Since(start)
	cpu := cpuNow() - cpu0
	if e := firstErr.Load(); e != nil {
		return cell{}, e.(error)
	}
	ops := total.Load()
	if ops == 0 {
		return cell{}, fmt.Errorf("no ops at highwater %d conns %d", highWater, conns)
	}
	return cell{
		opsPerSec: float64(ops) / el.Seconds(),
		cpuPerOp:  time.Duration(int64(cpu) / int64(ops)),
	}, nil
}

func main() {
	fmt.Printf("GET %dB, %d keys, P1, %d shards, %s per cell, in process\n\n",
		valSize, nKeys, shards, cellDur)
	fmt.Println("| conns | spin ops/s | spin CPU/op | park ops/s | park CPU/op | park/spin |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, conns := range connCounts {
		spin, err := runCell(alwaysSpin, conns)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lab:", err)
			os.Exit(1)
		}
		park, err := runCell(alwaysPark, conns)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lab:", err)
			os.Exit(1)
		}
		fmt.Printf("| %d | %.0f | %v | %.0f | %v | %.2f |\n",
			conns, spin.opsPerSec, spin.cpuPerOp, park.opsPerSec, park.cpuPerOp,
			park.opsPerSec/spin.opsPerSec)
	}
}
