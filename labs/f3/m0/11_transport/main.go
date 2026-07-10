// Lab: transport cheap wins (spec 2064/f3, M0 gate follow-up, lab 11).
//
// The question: the issue #542 ns/op decomposition of the GET 64B P16 gate
// cell put 876 ns/op in write syscalls (1.81 write() per 16-command round
// where 1.0 suffices), 383 ns/op in pthread_cond_signal wake tax (dominated
// by the locked-M handoff when a pinned worker parks the moment its queue
// drains and is cross-thread woken microseconds later), and 118 ns/op in the
// parks themselves. Three transport knobs claim to cut those: the boundary
// flush (the writer defers its socket flush until the connection owes no
// replies for published commands), the worker spin-before-park window (the
// workers inherited the 4us window lab 3 sized for the connection writers,
// but they are the hot consumers and their inter-wave gaps differ), and the
// worker thread lock itself (single-owner correctness is goroutine affinity,
// so the lock only ever bought cache residency). What does each buy on a real
// socket, and how do they interact?
//
// Method: one f3srv per cell on loopback, 4 shards, in process. Load 1M keys
// at 64B, then 128 client connections run pipelined GET rounds of depth 16
// for 2 seconds, the gate cell shape. The sweep is worker spin window {0,
// 5us, 20us, 80us} x workers {unpinned, pinned} x flush {boundary,
// every-drain}. Per cell: GETs per second, process CPU per op (rusage user
// plus system over the measured window; the loopback clients live in the
// same process, so the number is server plus client and only same-column
// comparisons mean anything), and server write flushes per 16-command round
// from the drivers flush counter (the reply buffer is 64KiB and a 64B P16
// round is about 1.2KiB, so every counted flush is exactly one write()).
//
// See README.md for the numbers and the verdicts frozen into
// engine/f3/shard/tuning.go (workerSpinWindow) and the pin-workers default.
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
	shards   = 4
	pipeline = 16
	conns    = 128
	nKeys    = 1 << 20
	valSize  = 64
	cellDur  = 2 * time.Second
	loadPipe = 128
)

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
			// Write errors surface at the checked Flush below.
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

// hammer runs pipelined GET rounds on one connection until deadline and
// returns the GET count. All values are valSize, so every reply is exactly
// replyLen bytes and the reader consumes rounds by exact size.
func hammer(addr string, seed uint64, deadline time.Time) (uint64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 64<<10)
	br := bufio.NewReaderSize(nc, 1<<20)

	replyLen := 1 + len(fmt.Sprint(valSize)) + 2 + valSize + 2
	round := make([]byte, pipeline*replyLen)
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
		ops += pipeline
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
	opsPerSec  float64
	cpuPerOp   time.Duration
	flushRound float64 // server write flushes per 16-command round
}

func runCell(window time.Duration, pin, flushEvery bool) (cell, error) {
	shard.SetWorkerSpinWindow(window)
	srv, err := drivers.Listen(drivers.Options{
		Addr:            "127.0.0.1:0",
		Shards:          shards,
		PinWorkers:      pin,
		FlushEveryDrain: flushEvery,
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
	flushes0 := srv.Flushes()
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
	flushes := srv.Flushes() - flushes0
	if e := firstErr.Load(); e != nil {
		return cell{}, e.(error)
	}
	ops := total.Load()
	return cell{
		opsPerSec:  float64(ops) / el.Seconds(),
		cpuPerOp:   time.Duration(int64(cpu) / int64(ops)),
		flushRound: float64(flushes) * pipeline / float64(ops),
	}, nil
}

func main() {
	windows := []time.Duration{0, 5 * time.Microsecond, 20 * time.Microsecond, 80 * time.Microsecond}

	fmt.Printf("GET %dB, %d keys, P%d, c%d, %d shards, %s per cell, in process\n\n",
		valSize, nKeys, pipeline, conns, shards, cellDur)
	fmt.Println("| spin window | workers | boundary ops/s | boundary CPU/op | boundary flush/round | every-drain ops/s | every-drain CPU/op | every-drain flush/round |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, win := range windows {
		for _, pin := range []bool{false, true} {
			name := "unpinned"
			if pin {
				name = "pinned"
			}
			var cells [2]cell
			for i, fe := range []bool{false, true} {
				c, err := runCell(win, pin, fe)
				if err != nil {
					fmt.Fprintln(os.Stderr, "lab:", err)
					os.Exit(1)
				}
				cells[i] = c
			}
			fmt.Printf("| %v | %s | %.0f | %v | %.2f | %.0f | %v | %.2f |\n",
				win, name,
				cells[0].opsPerSec, cells[0].cpuPerOp, cells[0].flushRound,
				cells[1].opsPerSec, cells[1].cpuPerOp, cells[1].flushRound)
		}
	}
}
