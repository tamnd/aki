// Lab: reply write buffer size (spec 2064/f3, M0 gate follow-up, lab 8).
//
// The question: the connection writer drains replies through a bufio.Writer
// whose default is 4096 bytes. The gate profile showed that at 1KiB values
// most of the write() time is mid-drain buffer-full flushes (one syscall per
// ~3.9 replies) and at 4KiB every reply overflows the buffer, so it is one
// syscall per reply. The buffer should hold a full pipelined round so a P16
// burst amortizes into about one write per drained boundary. How much does
// each buffer size actually buy, per reply size, over a real socket?
//
// Method: one f3srv per writer size {4KiB, 16KiB, 64KiB, 256KiB} on loopback,
// default shards. Per value size {64B, 1KiB, 4KiB}: load 16384 keys, then 8
// client connections each run pipelined GET rounds of depth 16 against
// random keys for 2 seconds; replies are fixed-shape ($<len>\r\n<val>\r\n)
// so the client reads the exact byte count per round. The cell reports total
// GETs per second.
//
// See README.md for the numbers and the verdict.
package main

import (
	"bufio"
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
	pipeline = 16
	conns    = 8
	nKeys    = 16384
	cellDur  = 2 * time.Second
	loadPipe = 64
)

func key(i int) string { return fmt.Sprintf("k%08d", i) }

// load SETs the whole keyspace at valSize, pipelined, and reads the +OK run.
func load(addr string, valSize int) error {
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
func hammer(addr string, valSize int, seed uint64, deadline time.Time) (uint64, error) {
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

func runCell(addr string, valSize int) (float64, error) {
	if err := load(addr, valSize); err != nil {
		return 0, err
	}
	deadline := time.Now().Add(cellDur)
	start := time.Now()
	var total, failed atomic.Uint64
	var wg sync.WaitGroup
	var firstErr atomic.Value
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ops, err := hammer(addr, valSize, uint64(c)+1, deadline)
			total.Add(ops)
			if err != nil {
				failed.Add(1)
				firstErr.CompareAndSwap(nil, err)
			}
		}(c)
	}
	wg.Wait()
	if failed.Load() > 0 {
		return 0, firstErr.Load().(error)
	}
	return float64(total.Load()) / time.Since(start).Seconds(), nil
}

func main() {
	valSizes := []int{64, 1024, 4096}
	bufSizes := []int{4 << 10, 16 << 10, 64 << 10, 256 << 10}

	fmt.Println("| writer buf | GET 64B ops/s | GET 1KiB ops/s | GET 4KiB ops/s |")
	fmt.Println("|---|---|---|---|")
	for _, bs := range bufSizes {
		srv, err := drivers.Listen(drivers.Options{Addr: "127.0.0.1:0", ReplyBufBytes: bs})
		if err != nil {
			fmt.Fprintln(os.Stderr, "lab:", err)
			os.Exit(1)
		}
		go func() { _ = srv.Serve() }()
		addr := srv.Addr().String()
		row := fmt.Sprintf("| %dKiB |", bs>>10)
		for _, vs := range valSizes {
			ops, err := runCell(addr, vs)
			if err != nil {
				fmt.Fprintln(os.Stderr, "lab:", err)
				os.Exit(1)
			}
			row += fmt.Sprintf(" %.2fM |", ops/1e6)
		}
		fmt.Println(row)
		_ = srv.Close()
	}
}
