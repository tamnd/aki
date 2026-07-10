// Lab: wake-batched completion path (spec 2064/f3, M10 pull-forward slice 4,
// lab 18).
//
// The question: slice 3's reactor paid one owner-to-loop eventfd write per
// dirty connection per worker drain pass; slice 4 folds them to one write per
// touched loop per pass. Does the counter evidence show it, and what does the
// fold do to throughput? The unbatched cost needs no separate build: a claim
// (net_conn_wakes) is exactly the event the slice 3 path answered with one
// eventfd write, so conn wakes per op IS the unbatched write rate and loop
// wakes per op (net_loop_wakes) is the batched one; their ratio is the
// batching yield per cell.
//
// Method: one in-process f3srv per cell on loopback, reactor driver, 4 shards
// and 4 loops, 1M keys at 64B, pipelined GET rounds. The sweep is connection
// count x pipeline depth: conns drive how many connections a drain pass can
// claim at once (the fold's raw material), depth drives how many batches each
// pass runs. Per cell: ops/s, conn wakes per op, loop wakes per op, the
// batching yield, and write syscalls per op as the sanity column.
//
// Linux only in substance: elsewhere the reactor falls back to the goroutine
// driver and the lab refuses to run rather than print numbers for the wrong
// edge. See README.md for the prediction (filed first) and the numbers.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/f3srv/drivers"
)

const (
	shards  = 4
	loops   = 4
	nKeys   = 1 << 20
	valSize = 64
)

func key(i int) string { return fmt.Sprintf("k%08d", i) }

// replyBytes is one GET reply on the wire at valSize: $64\r\n<64>\r\n.
func replyBytes() int { return 1 + len(fmt.Sprint(valSize)) + 2 + valSize + 2 }

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

// cell is one sweep point's per-op counter movement.
type cell struct {
	opsPerSec           float64
	connWakes, loopWaks float64
	writes              float64
}

func runCell(pipeline, conns int, dur time.Duration) (cell, error) {
	srv, err := drivers.Listen(drivers.Options{
		Addr: "127.0.0.1:0", Shards: shards, NetDriver: drivers.NetReactor, NetLoops: loops,
	})
	if err != nil {
		return cell{}, err
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Close() }()
	if got := srv.NetStats().Driver; got != drivers.NetReactor {
		return cell{}, fmt.Errorf("driver %q, want %q; this lab measures the reactor's owner-to-loop edge and runs on Linux", got, drivers.NetReactor)
	}
	addr := srv.Addr().String()
	if err := load(addr); err != nil {
		return cell{}, err
	}

	deadline := time.Now().Add(dur)
	ns0 := srv.NetStats()
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
	ns1 := srv.NetStats()
	if e := firstErr.Load(); e != nil {
		return cell{}, e.(error)
	}
	ops := float64(total.Load())
	d := func(b, a uint64) float64 { return float64(b-a) / ops }
	return cell{
		opsPerSec: ops / el.Seconds(),
		connWakes: d(ns1.ConnWakes, ns0.ConnWakes),
		loopWaks:  d(ns1.LoopWakes, ns0.LoopWakes),
		writes:    d(ns1.WriteSyscalls, ns0.WriteSyscalls),
	}, nil
}

func main() {
	dur := flag.Duration("dur", 4*time.Second, "measured window per cell")
	flag.Parse()

	fmt.Printf("GET %dB, %d keys, %d shards, %d loops, %s per cell, in process\n", valSize, nKeys, shards, loops, *dur)
	fmt.Printf("conn wakes/op is what slice 3 wrote to eventfd; loop wakes/op is what slice 4 writes\n\n")
	fmt.Printf("| conns | pipeline | ops/s | conn wakes/op | loop wakes/op | yield | writes/op |\n")
	fmt.Printf("|---|---|---|---|---|---|---|\n")
	for _, conns := range []int{8, 64, 256, 512} {
		for _, pipeline := range []int{1, 16} {
			c, err := runCell(pipeline, conns, *dur)
			if err != nil {
				fmt.Fprintln(os.Stderr, "lab:", err)
				os.Exit(1)
			}
			yield := 0.0
			if c.loopWaks > 0 {
				yield = c.connWakes / c.loopWaks
			}
			fmt.Printf("| %d | P%d | %.0f | %.3f | %.3f | %.2fx | %.3f |\n",
				conns, pipeline, c.opsPerSec, c.connWakes, c.loopWaks, yield, c.writes)
		}
	}
}
