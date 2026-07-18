// Lab: PUBLISH fan-out throughput (spec 2064/f3, M11 pub/sub slice 1, lab 1).
//
// The question: pub/sub lives in a network-layer registry, not the shard
// workers (doc 17 section 13), and PUBLISH is a gate row because its cost is a
// fan-out: one published message reaches N subscribers. This lab measures how a
// single publisher's PUBLISH rate and the total delivered-message rate move as
// the subscriber count on one channel grows, and confirms that a wall of idle
// subscribers does not touch an unrelated GET on the same server, since the
// registry and the out-of-band delivery node sit entirely off the point-op
// drain path (the DrainReplies out-of-band check is one node-level branch a GET
// never trips).
//
// Method: one f3srv on loopback, pair shape (a subscriber's writer goroutine is
// what delivers to it while it sits idle in Read; the single shape cannot, and
// the reactor delivers through its eventfd the same way). Per subscriber count
// {1, 8, 64, 256}: that many connections each SUBSCRIBE the one channel and
// then drain messages to a discard buffer under a read deadline. Each cell then
// runs two phases against that registered wall, kept apart on purpose. Phase one
// is the fan-out: one publisher connection runs PUBLISH as fast as it can for
// two seconds, reading each reply's receiver count, while the subscribers drain.
// Phase two is the point-op control: the publisher is quiet, the subscribers sit
// idle and registered, and one connection runs pipelined GET rounds against a
// small loaded keyspace for two seconds. The GET is a control, not a concurrent
// load, because measuring it while the publisher fans out would only show the
// two contending for the box's cores. With the publisher quiet, a flat GET rate
// across the subscriber sweep is the evidence the claim needs. The cell reports
// PUBLISH ops/s, delivered messages/s, and the idle-wall GET ops/s.
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
	"time"

	"github.com/tamnd/aki/f3srv/drivers"
)

const (
	cellDur   = 2 * time.Second
	getPipe   = 16
	getKeys   = 4096
	channel   = "bench.channel"
	payload   = "hello pub/sub world"
	drainWait = 500 * time.Millisecond
)

func key(i int) string { return fmt.Sprintf("k%06d", i) }

// subscribe opens a subscriber, subscribes to the channel, reads the
// confirmation, then drains delivered messages to a discard buffer until stop.
// The publisher provides the delivery load; correctness of what arrives is the
// driver test's job, so this only has to keep the subscriber's socket drained so
// the writer never stalls on a full output buffer.
func subscribe(addr string, ready *sync.WaitGroup, stop <-chan struct{}) error {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		ready.Done()
		return err
	}
	defer func() { _ = nc.Close() }()
	br := bufio.NewReaderSize(nc, 1<<16)
	if _, err := nc.Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$13\r\nbench.channel\r\n")); err != nil {
		ready.Done()
		return err
	}
	// The subscribe confirmation is one array; drain its five lines.
	for i := 0; i < 5; i++ {
		if _, err := br.ReadString('\n'); err != nil {
			ready.Done()
			return err
		}
	}
	ready.Done()

	sink := make([]byte, 4<<10)
	for {
		select {
		case <-stop:
			return nil
		default:
			_ = nc.SetReadDeadline(time.Now().Add(drainWait))
			if _, err := br.Read(sink); err != nil {
				// A deadline timeout just re-checks stop; any other error ends.
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				return nil
			}
		}
	}
}

// load SETs a small keyspace so the concurrent GET has something to read.
func load(addr string) error {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 1<<20)
	br := bufio.NewReaderSize(nc, 1<<16)
	ok := make([]byte, 5)
	for i := 0; i < getKeys; i++ {
		k := key(i)
		if _, err := fmt.Fprintf(bw, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(k), k); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		if _, err := io.ReadFull(br, ok); err != nil {
			return err
		}
	}
	return nil
}

// publishLoop publishes to the channel until deadline, reading each reply's
// receiver count. It returns the number of PUBLISH calls made.
func publishLoop(addr string, deadline time.Time) (uint64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 1<<16)
	br := bufio.NewReaderSize(nc, 1<<16)
	req := fmt.Sprintf("*3\r\n$7\r\nPUBLISH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(payload), payload)
	var n uint64
	for time.Now().Before(deadline) {
		if _, err := bw.WriteString(req); err != nil {
			return n, err
		}
		if err := bw.Flush(); err != nil {
			return n, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// getLoop runs pipelined GET rounds until deadline, the point-op control.
func getLoop(addr string, deadline time.Time) (uint64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = nc.Close() }()
	bw := bufio.NewWriterSize(nc, 1<<16)
	br := bufio.NewReaderSize(nc, 1<<16)
	reply := make([]byte, getPipe*len("$1\r\nv\r\n"))
	x := uint64(0x9e3779b97f4a7c15)
	var ops uint64
	var req []byte
	for time.Now().Before(deadline) {
		req = req[:0]
		for j := 0; j < getPipe; j++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			k := key(int(x % getKeys))
			req = append(req, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(k), k)...)
		}
		if _, err := bw.Write(req); err != nil {
			return ops, err
		}
		if err := bw.Flush(); err != nil {
			return ops, err
		}
		if _, err := io.ReadFull(br, reply); err != nil {
			return ops, err
		}
		ops += getPipe
	}
	return ops, nil
}

// runCell measures the cell in two phases against the same registered
// subscriber wall, kept apart so the second is a clean control. Phase one is the
// fan-out: a publisher hammers the channel while the subscribers drain, and the
// cell reports the PUBLISH rate and the delivered-message rate. Phase two is the
// point-op control: no publisher runs, the subscribers sit idle and registered,
// and a lone connection runs pipelined GET rounds. Splitting the phases is the
// point. Running GET while the publisher fans out would only measure the two
// contending for the box's cores, not whether the registry touches the GET path;
// with the publisher quiet, a flat GET rate across the subscriber sweep is the
// evidence that idle subscribers and the registry cost the point path nothing.
func runCell(addr string, subs int) (pubRate, msgRate, getRate float64, err error) {
	if err := load(addr); err != nil {
		return 0, 0, 0, err
	}
	stop := make(chan struct{})
	var ready sync.WaitGroup
	var subWg sync.WaitGroup
	ready.Add(subs)
	for i := 0; i < subs; i++ {
		subWg.Add(1)
		go func() {
			defer subWg.Done()
			_ = subscribe(addr, &ready, stop)
		}()
	}
	ready.Wait() // every subscriber is registered before either phase starts
	defer func() {
		close(stop)
		subWg.Wait()
	}()

	// Phase one: fan-out.
	pubDeadline := time.Now().Add(cellDur)
	pubStart := time.Now()
	pub, _ := publishLoop(addr, pubDeadline)
	pubElapsed := time.Since(pubStart).Seconds()
	pubRate = float64(pub) / pubElapsed
	msgRate = float64(pub*uint64(subs)) / pubElapsed

	// Phase two: point-op control against the idle subscriber wall.
	getDeadline := time.Now().Add(cellDur)
	getStart := time.Now()
	get, _ := getLoop(addr, getDeadline)
	getRate = float64(get) / time.Since(getStart).Seconds()
	return pubRate, msgRate, getRate, nil
}

func main() {
	subCounts := []int{1, 8, 64, 256}
	srv, err := drivers.Listen(drivers.Options{Addr: "127.0.0.1:0", ConnShape: drivers.ShapePair})
	if err != nil {
		fmt.Fprintln(os.Stderr, "lab:", err)
		os.Exit(1)
	}
	go func() { _ = srv.Serve() }()
	addr := srv.Addr().String()

	fmt.Println("| subscribers | PUBLISH ops/s | delivered msgs/s | idle-wall GET ops/s |")
	fmt.Println("|---|---|---|---|")
	for _, subs := range subCounts {
		pubRate, msgRate, getRate, err := runCell(addr, subs)
		if err != nil {
			fmt.Fprintln(os.Stderr, "lab:", err)
			os.Exit(1)
		}
		fmt.Printf("| %d | %.3fM | %.3fM | %.3fM |\n", subs, pubRate/1e6, msgRate/1e6, getRate/1e6)
	}
	_ = srv.Close()
}
