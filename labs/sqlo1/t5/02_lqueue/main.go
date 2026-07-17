// Lab: lqueue, the queue marquee (spec 2064/sqlo1 doc 07 section 8,
// milestone T5 lab 02).
//
// The question is the headline cell: sustained LPUSH+RPOP throughput
// and tail latency against Redis and Valkey as the steady depth grows
// from 10 to 10^7. PRED-SQLO1-T5-QUEUE claims the p99 stays flat in
// depth because a queue only ever touches its edge nodes; the rivals
// hold the whole list in RAM, so depth costs them memory instead of
// time, and the fair frame is doc 13's: equal work, then compare the
// throughput, the tail, and (in the suite, not here) the memory bill.
//
// Unlike the resident-model labs (lnode, lmid), this one drives live
// servers over RESP, because the marquee number is end to end: parse,
// dispatch, list op, frame group, reply. The aki arm serves in
// process over either the placeholder memory store or the real
// sqlo1b file store; a rival arm is any address a script points us
// at. Every worker connection alternates one LPUSH with one RPOP on
// a single queue key, the reliable-queue shape, so the depth stays
// within one connection count of the target and a pop never finds
// the queue empty. Elements carry their sequence number, the FIFO
// oracle: with one connection the pops must come back in exact push
// order, and at any width the drain at the end must find exactly the
// elements the ledger says are left.
//
// With fence paging landed the fence spills into page records past
// 167 nodes, so the full depth sweep to 10^7 is reachable; the page
// index caps the two-level layout around 3.4B small elements, far
// past anything this lab drives.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// walSeg is the production WAL segment size, the same constant the
// Track B labs use.
const walSeg = 64 << 20

// seqLen is the element prefix carrying the sequence number; the rest
// of the element is padding to the requested size.
const seqLen = 20

type cfg struct {
	addr       string
	key        string
	depth      int
	elem       int
	conns      int
	warm       time.Duration
	dur        time.Duration
	batch      int
	checkOrder bool
}

type result struct {
	ops       int64
	secs      float64
	pushP50   float64
	pushP99   float64
	popP50    float64
	popP99    float64
	misses    int64
	orderErrs int64
	drained   int64
	expected  int64
}

func main() {
	mode := flag.String("mode", "serve", "serve (in-process aki) or dial (external server)")
	store := flag.String("store", "file", "serve-mode backend: mem or file")
	dir := flag.String("dir", "", "serve-mode data dir for -store file; empty is a temp dir")
	addr := flag.String("addr", "", "dial-mode server address")
	server := flag.String("server", "aki", "server label for the CSV row")
	depth := flag.Int("depth", 1000, "steady queue depth")
	elem := flag.Int("elem", 200, "element size in bytes")
	conns := flag.Int("conns", 8, "worker connections")
	warm := flag.Duration("warm", 5*time.Second, "warmup before measuring")
	dur := flag.Duration("dur", 20*time.Second, "measured window")
	batch := flag.Int("batch", 512, "prefill elements per LPUSH")
	checkOrder := flag.Bool("checkorder", false, "verify FIFO pop order (needs -conns 1)")
	quick := flag.Bool("quick", false, "smoke run: depth 200, 1s warm, 2s window")
	flag.Parse()

	c := cfg{
		addr: *addr, key: "lq", depth: *depth, elem: *elem, conns: *conns,
		warm: *warm, dur: *dur, batch: *batch, checkOrder: *checkOrder,
	}
	if *quick {
		c.depth, c.warm, c.dur = 200, time.Second, 2*time.Second
	}
	if c.elem < seqLen {
		fatal(fmt.Errorf("elem %d is smaller than the %d byte sequence prefix", c.elem, seqLen))
	}
	if c.checkOrder && c.conns != 1 {
		fatal(errors.New("-checkorder needs -conns 1; FIFO across connections is not a total order"))
	}

	if *mode == "serve" {
		a, cleanup, err := serveInProc(*store, *dir)
		if err != nil {
			fatal(err)
		}
		defer cleanup()
		c.addr = a
	} else if c.addr == "" {
		fatal(errors.New("-mode dial needs -addr"))
	}

	res, err := runBench(c)
	if err != nil {
		fatal(err)
	}
	if res.misses > 0 || res.orderErrs > 0 || res.drained != res.expected {
		fatal(fmt.Errorf("oracle: %d misses, %d order errors, drained %d want %d",
			res.misses, res.orderErrs, res.drained, res.expected))
	}
	fmt.Printf("%s,%d,%d,%d,%.1f,%d,%.0f,%.1f,%.1f,%.1f,%.1f,%d\n",
		*server, c.depth, c.elem, c.conns, res.secs, res.ops, float64(res.ops)/res.secs,
		res.pushP50, res.pushP99, res.popP50, res.popP99, res.misses)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "lqueue: %v\n", err)
	os.Exit(1)
}

// serveInProc starts an aki server on an ephemeral port over the
// requested backend and returns its address.
func serveInProc(store, dir string) (string, func(), error) {
	var st sqlo1.Store
	cleanup := func() {}
	switch store {
	case "mem":
		st = sqlo1.NewMemStore()
	case "file":
		d, made := dir, false
		if d == "" {
			t, err := os.MkdirTemp("", "lqueue")
			if err != nil {
				return "", nil, err
			}
			d, made = t, true
		}
		db, err := sqlo1b.CreateStore(filepath.Join(d, "lqueue.aki"), walSeg)
		if err != nil {
			return "", nil, err
		}
		st = db
		cleanup = func() {
			db.Close()
			if made {
				os.RemoveAll(d)
			}
		}
	default:
		return "", nil, fmt.Errorf("unknown store %q", store)
	}
	srv, err := sqlo1.NewServer(st)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cleanup()
		return "", nil, err
	}
	go srv.Serve(l)
	prev := cleanup
	return l.Addr().String(), func() { l.Close(); prev() }, nil
}

type client struct {
	c   net.Conn
	r   *bufio.Reader
	w   *bufio.Writer
	buf []byte
}

func dialClient(addr string) (*client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(24 * time.Hour))
	return &client{c: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}, nil
}

// do sends one command and reads its reply. bulk is a bulk reply's
// payload (aliasing the client's scratch), n an integer reply's value
// or an array reply's element count, and null a nil bulk or null
// array. A wire error reply comes back as a Go error.
func (cl *client) do(args ...[]byte) (bulk []byte, n int64, null bool, err error) {
	cl.buf = cl.buf[:0]
	cl.buf = append(cl.buf, '*')
	cl.buf = strconv.AppendInt(cl.buf, int64(len(args)), 10)
	cl.buf = append(cl.buf, '\r', '\n')
	for _, a := range args {
		cl.buf = append(cl.buf, '$')
		cl.buf = strconv.AppendInt(cl.buf, int64(len(a)), 10)
		cl.buf = append(cl.buf, '\r', '\n')
		cl.buf = append(cl.buf, a...)
		cl.buf = append(cl.buf, '\r', '\n')
	}
	if _, err := cl.w.Write(cl.buf); err != nil {
		return nil, 0, false, err
	}
	if err := cl.w.Flush(); err != nil {
		return nil, 0, false, err
	}
	return cl.read()
}

func (cl *client) read() (bulk []byte, n int64, null bool, err error) {
	line, err := cl.r.ReadString('\n')
	if err != nil {
		return nil, 0, false, err
	}
	if len(line) < 3 {
		return nil, 0, false, fmt.Errorf("short reply line %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return nil, 0, false, nil
	case '-':
		return nil, 0, false, fmt.Errorf("server: %s", body)
	case ':':
		v, err := strconv.ParseInt(body, 10, 64)
		return nil, v, false, err
	case '$':
		l, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return nil, 0, false, err
		}
		if l < 0 {
			return nil, 0, true, nil
		}
		need := int(l) + 2
		if cap(cl.buf) < need {
			cl.buf = make([]byte, need)
		}
		b := cl.buf[:need]
		if _, err := io.ReadFull(cl.r, b); err != nil {
			return nil, 0, false, err
		}
		return b[:l], 0, false, nil
	case '*':
		l, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return nil, 0, false, err
		}
		if l < 0 {
			return nil, 0, true, nil
		}
		for range l {
			if _, _, _, err := cl.read(); err != nil {
				return nil, 0, false, err
			}
		}
		return nil, l, false, nil
	}
	return nil, 0, false, fmt.Errorf("unknown reply type %q", line[0])
}

// payload writes seq into a fresh element of c.elem bytes: a 19 byte
// "q" plus 18 digit prefix, then x padding.
func (c *cfg) payload(seq int64) []byte {
	b := make([]byte, c.elem)
	copy(b, fmt.Sprintf("q%018d", seq))
	for i := seqLen - 1; i < len(b); i++ {
		b[i] = 'x'
	}
	return b
}

func parseSeq(b []byte) (int64, error) {
	if len(b) < seqLen || b[0] != 'q' {
		return 0, fmt.Errorf("element %q has no sequence prefix", b)
	}
	return strconv.ParseInt(string(b[1:seqLen-1]), 10, 64)
}

type worker struct {
	pushNs []int64
	popNs  []int64
	pushes int64
	pops   int64
	misses int64
	order  int64
}

// runBench prefills the queue to depth, runs the paired LPUSH+RPOP
// workers through warmup and the measured window, then drains the
// queue and checks the ledger: every element pushed and not popped
// must come back out, and nothing else.
func runBench(c cfg) (result, error) {
	ctl, err := dialClient(c.addr)
	if err != nil {
		return result{}, err
	}
	defer ctl.c.Close()

	key := []byte(c.key)
	if _, _, _, err := ctl.do([]byte("DEL"), key); err != nil {
		return result{}, err
	}

	// Prefill with LPUSH so the prefilled elements pop back in
	// sequence order: RPOP takes the oldest LPUSHed element.
	var seq int64
	for seq < int64(c.depth) {
		args := [][]byte{[]byte("LPUSH"), key}
		for range c.batch {
			if seq == int64(c.depth) {
				break
			}
			args = append(args, c.payload(seq))
			seq++
		}
		if _, _, _, err := ctl.do(args...); err != nil {
			return result{}, fmt.Errorf("prefill at %d: %w", seq, err)
		}
	}

	workers := make([]*worker, c.conns)
	errs := make([]error, c.conns)
	var popCtr int64 // FIFO oracle: pop i must return sequence i
	start := time.Now()
	measure := start.Add(c.warm)
	end := measure.Add(c.dur)
	var wg sync.WaitGroup
	for i := range c.conns {
		w := &worker{}
		workers[i] = w
		cl, err := dialClient(c.addr)
		if err != nil {
			return result{}, err
		}
		defer cl.c.Close()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lpush, rpop := []byte("LPUSH"), []byte("RPOP")
			for {
				now := time.Now()
				if !now.Before(end) {
					return
				}
				rec := now.After(measure)
				s := atomic.AddInt64(&seq, 1) - 1
				el := c.payload(s)
				t0 := time.Now()
				if _, _, _, err := cl.do(lpush, key, el); err != nil {
					errs[i] = err
					return
				}
				if rec {
					w.pushNs = append(w.pushNs, time.Since(t0).Nanoseconds())
				}
				w.pushes++
				t0 = time.Now()
				bulk, _, null, err := cl.do(rpop, key)
				if err != nil {
					errs[i] = err
					return
				}
				if rec {
					w.popNs = append(w.popNs, time.Since(t0).Nanoseconds())
				}
				if null {
					w.misses++
					continue
				}
				w.pops++
				if c.checkOrder {
					got, err := parseSeq(bulk)
					if err != nil {
						errs[i] = err
						return
					}
					want := atomic.AddInt64(&popCtr, 1) - 1
					if got != want {
						w.order++
					}
				}
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return result{}, err
		}
	}

	res := result{secs: c.dur.Seconds()}
	var pushNs, popNs []int64
	var pushed, popped int64
	for _, w := range workers {
		pushNs = append(pushNs, w.pushNs...)
		popNs = append(popNs, w.popNs...)
		pushed += w.pushes
		popped += w.pops
		res.misses += w.misses
		res.orderErrs += w.order
	}
	res.ops = int64(len(pushNs) + len(popNs))
	res.pushP50, res.pushP99 = pcts(pushNs)
	res.popP50, res.popP99 = pcts(popNs)
	res.expected = int64(c.depth) + pushed - popped

	// The drain: RPOP in count batches until the null array, counting
	// elements. The queue must hold exactly the ledger's remainder.
	for {
		_, n, null, err := ctl.do([]byte("RPOP"), key, []byte("4096"))
		if err != nil {
			return result{}, fmt.Errorf("drain: %w", err)
		}
		if null {
			break
		}
		res.drained += n
	}
	return res, nil
}

// pcts answers the p50 and p99 in microseconds.
func pcts(ns []int64) (p50, p99 float64) {
	if len(ns) == 0 {
		return 0, 0
	}
	slices.Sort(ns)
	at := func(p float64) float64 {
		i := int(float64(len(ns)) * p)
		if i >= len(ns) {
			i = len(ns) - 1
		}
		return float64(ns[i]) / 1e3
	}
	return at(0.50), at(0.99)
}
