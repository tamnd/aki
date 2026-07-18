// Lab: cold stream catch-up (spec 2064/sqlo1 doc 10 section 7,
// milestone T6 lab 02).
//
// The question is PRED-SQLO1-T6-CATCHUP's frame: a consumer replaying
// 10^7 entries cold, end to end over RESP against the real sqlo1b
// file, with three numbers on the line. Throughput: how fast a cold
// XRANGE walk pages through the stream when every run is a store read
// behind the 16-run prefetch round. Pollution: what the replay does to
// the hot tier's working set, measured behaviorally as foreground
// point-GET latency before, during, and after the replay, plus the
// process resident set, since the prediction says the p99 moves under
// 20 percent. Batch depth: the consumer-side knob, the XRANGE COUNT a
// catch-up loop should use, swept against the fixed prefetch round so
// the verdict names the depth XREAD should default to when its slice
// lands.
//
// Unlike the xadd lab (a resident model pricing WAL arithmetic), this
// one drives a live in-process server over RESP the way lqueue does,
// because catch-up is an end-to-end read path: parse, dispatch, the
// two-pass paged range, run prefetch rounds through the cold index,
// and the reply build. The stream is built by one arm and replayed by
// another in a fresh process, so the replay process's peak resident
// set is the catch-up footprint alone and the hot tier starts as cold
// as a restarted server.
//
// Entries carry their ordinal in a fixed field, the oracle: the replay
// must see every ordinal exactly once, in order, whatever the COUNT
// geometry does at page and run boundaries. The build arm also plants
// a keyspace of point strings for the pollute arm's foreground load.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/tamnd/aki/engine/sqlo1"
	"github.com/tamnd/aki/engine/sqlo1b"
)

// walSeg is the production WAL segment size, the same constant the
// Track B labs use.
const walSeg = 64 << 20

// ordLen is the zero-padded ordinal prefix in the "i" field, the
// replay oracle's sequence number.
const ordLen = 20

type cfg struct {
	dir    string
	n      int
	elen   int
	count  int
	pipe   int
	wkeys  int
	wlen   int
	warmup int
	probes int
	gap    time.Duration
}

func main() {
	var c cfg
	mix := flag.String("mix", "build", "arm: build, catchup, pollute")
	flag.StringVar(&c.dir, "dir", "", "data dir; build creates the store file here")
	flag.IntVar(&c.n, "n", 10_000_000, "stream entries")
	flag.IntVar(&c.elen, "elen", 100, "value bytes per entry across both fields")
	flag.IntVar(&c.count, "count", 1000, "XRANGE COUNT per replay call")
	flag.IntVar(&c.pipe, "pipe", 256, "pipelined commands per round in build and warmup")
	flag.IntVar(&c.wkeys, "wkeys", 20000, "point string keys for the pollute arm")
	flag.IntVar(&c.wlen, "wlen", 128, "point string value bytes")
	flag.IntVar(&c.warmup, "warmup", 12, "GET rounds over the point keys before probing")
	flag.IntVar(&c.probes, "probes", 4000, "point GETs per latency window")
	flag.DurationVar(&c.gap, "gap", 500*time.Microsecond, "pause between foreground probes")
	quick := flag.Bool("quick", false, "shrink counts for smoke runs")
	flag.Parse()
	if *quick {
		c.n, c.wkeys, c.warmup, c.probes = 200_000, 2000, 6, 1000
	}
	if c.dir == "" {
		fatal(errors.New("-dir is required; build and replay must see the same file"))
	}
	if c.elen < ordLen+1 {
		fatal(fmt.Errorf("elen %d is smaller than the %d byte ordinal plus padding", c.elen, ordLen+1))
	}
	var err error
	switch *mix {
	case "build":
		err = runBuild(c)
	case "catchup":
		err = runCatchup(c)
	case "pollute":
		err = runPollute(c)
	default:
		err = fmt.Errorf("unknown mix %q", *mix)
	}
	if err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "xcatchup: %v\n", err)
	os.Exit(1)
}

func storePath(dir string) string {
	return filepath.Join(dir, "xcatchup.aki")
}

// serve starts the in-process server over the store file, creating or
// opening it, and returns a cleanup that closes both ends. The cleanup
// flushes the hot tier between listener close and store close, so the
// next process reads every acked write and not just the last drained
// prefix; the replay oracle caught exactly that undercount before the
// Server.Flush door existed.
func serve(dir string, create bool) (string, func(), error) {
	open := sqlo1b.OpenStore
	if create {
		open = sqlo1b.CreateStore
	}
	db, err := open(storePath(dir), walSeg)
	if err != nil {
		return "", nil, err
	}
	srv, err := sqlo1.NewServer(db)
	if err != nil {
		db.Close()
		return "", nil, err
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		return "", nil, err
	}
	go srv.Serve(l)
	cleanup := func() {
		l.Close()
		if err := srv.Flush(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "xcatchup: flush: %v\n", err)
		}
		db.Close()
	}
	return l.Addr().String(), cleanup, nil
}

// client is a minimal RESP2 pipeline client: queue commands, flush,
// then read replies one at a time.
type client struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func dial(addr string) (*client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.SetDeadline(time.Now().Add(24 * time.Hour))
	return &client{c: c, r: bufio.NewReaderSize(c, 1<<20), w: bufio.NewWriterSize(c, 1<<20)}, nil
}

func (cl *client) cmd(args ...[]byte) {
	fmt.Fprintf(cl.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(cl.w, "$%d\r\n", len(a))
		cl.w.Write(a)
		cl.w.WriteString("\r\n")
	}
}

func (cl *client) flush() error {
	return cl.w.Flush()
}

// reply is one parsed RESP value; arrays nest.
type reply struct {
	kind byte // '+', '-', ':', '$', '*'
	str  []byte
	n    int64
	arr  []reply
}

func (cl *client) read() (reply, error) {
	line, err := cl.r.ReadBytes('\n')
	if err != nil {
		return reply{}, err
	}
	if len(line) < 3 {
		return reply{}, fmt.Errorf("short reply line %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+', '-':
		return reply{kind: line[0], str: append([]byte(nil), body...)}, nil
	case ':':
		n, err := strconv.ParseInt(string(body), 10, 64)
		return reply{kind: ':', n: n}, err
	case '$':
		n, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return reply{}, err
		}
		if n < 0 {
			return reply{kind: '$', n: -1}, nil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(cl.r, buf); err != nil {
			return reply{}, err
		}
		return reply{kind: '$', n: n, str: buf[:n]}, nil
	case '*':
		n, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return reply{}, err
		}
		r := reply{kind: '*', n: n}
		for range n {
			el, err := cl.read()
			if err != nil {
				return reply{}, err
			}
			r.arr = append(r.arr, el)
		}
		return r, nil
	}
	return reply{}, fmt.Errorf("unknown reply type %q", line[0])
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// drain reads k replies and fails on the first error reply.
func (cl *client) drain(k int) error {
	for range k {
		r, err := cl.read()
		if err != nil {
			return err
		}
		if r.kind == '-' {
			return fmt.Errorf("server: %s", r.str)
		}
	}
	return nil
}

// runBuild populates the stream and the point keyspace, then reports
// the write rate and the file size the replay arms will open.
func runBuild(c cfg) error {
	addr, cleanup, err := serve(c.dir, true)
	if err != nil {
		return err
	}
	defer cleanup()
	cl, err := dial(addr)
	if err != nil {
		return err
	}
	defer cl.c.Close()

	pad := make([]byte, c.elen-ordLen)
	for i := range pad {
		pad[i] = 'x'
	}
	ord := make([]byte, ordLen)
	start := time.Now()
	for i := 0; i < c.n; {
		k := min(c.pipe, c.n-i)
		for j := range k {
			ord = fmt.Appendf(ord[:0], "%0*d", ordLen, i+j)
			cl.cmd([]byte("XADD"), []byte("s"), []byte("*"), []byte("i"), ord, []byte("p"), pad)
		}
		if err := cl.flush(); err != nil {
			return err
		}
		if err := cl.drain(k); err != nil {
			return err
		}
		i += k
	}
	buildSecs := time.Since(start).Seconds()

	cl.cmd([]byte("XLEN"), []byte("s"))
	if err := cl.flush(); err != nil {
		return err
	}
	r, err := cl.read()
	if err != nil {
		return err
	}
	if r.kind != ':' || r.n != int64(c.n) {
		return fmt.Errorf("XLEN after build: got %d want %d", r.n, c.n)
	}

	// The pollute arm's foreground keyspace.
	val := make([]byte, c.wlen)
	for i := range val {
		val[i] = 'w'
	}
	for i := 0; i < c.wkeys; {
		k := min(c.pipe, c.wkeys-i)
		for j := range k {
			cl.cmd([]byte("SET"), wkey(i+j), val)
		}
		if err := cl.flush(); err != nil {
			return err
		}
		if err := cl.drain(k); err != nil {
			return err
		}
		i += k
	}
	cleanup()

	fi, err := os.Stat(storePath(c.dir))
	if err != nil {
		return err
	}
	fmt.Printf("build,%d,%d,%d,%.1f,%.0f,%.1f,%d,0,0\n",
		c.n, c.elen, c.wkeys, buildSecs, float64(c.n)/buildSecs,
		float64(fi.Size())/(1<<20), maxRSSMiB())
	return nil
}

func wkey(i int) []byte {
	return fmt.Appendf(nil, "w:%08d", i)
}

// replay pages through the whole stream with XRANGE COUNT, checking
// the ordinal sequence, and reports entries and payload bytes seen.
// onBatch, when set, runs between pages, the pollute arm's hook.
func replay(cl *client, c cfg, onBatch func()) (int, int64, error) {
	seen, payload := 0, int64(0)
	start := []byte("-")
	for {
		cl.cmd([]byte("XRANGE"), []byte("s"), start, []byte("+"), []byte("COUNT"), []byte(strconv.Itoa(c.count)))
		if err := cl.flush(); err != nil {
			return 0, 0, err
		}
		r, err := cl.read()
		if err != nil {
			return 0, 0, err
		}
		if r.kind != '*' {
			return 0, 0, fmt.Errorf("XRANGE reply type %q", r.kind)
		}
		if len(r.arr) == 0 {
			break
		}
		for _, e := range r.arr {
			if len(e.arr) != 2 || len(e.arr[1].arr) < 2 {
				return 0, 0, errors.New("malformed XRANGE entry")
			}
			fv := e.arr[1].arr
			got := -1
			for f := 0; f+1 < len(fv); f += 2 {
				payload += fv[f+1].n
				if string(fv[f].str) == "i" {
					got, err = strconv.Atoi(string(fv[f+1].str))
					if err != nil {
						return 0, 0, err
					}
				}
			}
			if got != seen {
				return 0, 0, fmt.Errorf("ordinal %d at position %d", got, seen)
			}
			seen++
		}
		last := r.arr[len(r.arr)-1].arr[0].str
		start = append([]byte("("), last...)
		if onBatch != nil {
			onBatch()
		}
	}
	return seen, payload, nil
}

// runCatchup opens the built file cold and replays it, the marquee.
func runCatchup(c cfg) error {
	addr, cleanup, err := serve(c.dir, false)
	if err != nil {
		return err
	}
	defer cleanup()
	cl, err := dial(addr)
	if err != nil {
		return err
	}
	defer cl.c.Close()

	start := time.Now()
	seen, payload, err := replay(cl, c, nil)
	if err != nil {
		return err
	}
	secs := time.Since(start).Seconds()
	if seen != c.n {
		return fmt.Errorf("replay saw %d entries, want %d", seen, c.n)
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("catchup,%d,%d,%d,%.1f,%.0f,%.1f,%d,%d,0\n",
		c.n, c.elen, c.count, secs, float64(seen)/secs,
		float64(payload)/secs/(1<<20), maxRSSMiB(), ms.HeapSys>>20)
	return nil
}

// probeWindow runs point GETs at the probe cadence and returns p50 and
// p99 in microseconds. stop, when non-nil, ends the window early once
// it goes true and at least probes/4 samples are in.
func probeWindow(cl *client, c cfg, rng *rand.Rand, stop *atomic.Bool) (float64, float64, error) {
	lats := make([]float64, 0, c.probes)
	for len(lats) < c.probes {
		key := wkey(rng.Intn(c.wkeys))
		t0 := time.Now()
		cl.cmd([]byte("GET"), key)
		if err := cl.flush(); err != nil {
			return 0, 0, err
		}
		r, err := cl.read()
		if err != nil {
			return 0, 0, err
		}
		if r.kind != '$' || r.n != int64(c.wlen) {
			return 0, 0, fmt.Errorf("GET %s: kind %q len %d", key, r.kind, r.n)
		}
		lats = append(lats, float64(time.Since(t0).Microseconds()))
		if stop != nil && stop.Load() && len(lats) >= c.probes/4 {
			break
		}
		time.Sleep(c.gap)
	}
	sort.Float64s(lats)
	return lats[len(lats)/2], lats[len(lats)*99/100], nil
}

// runPollute is the isolation arm: a warmed point working set probed
// before, during, and after a full replay on a second connection.
func runPollute(c cfg) error {
	addr, cleanup, err := serve(c.dir, false)
	if err != nil {
		return err
	}
	defer cleanup()
	fg, err := dial(addr)
	if err != nil {
		return err
	}
	defer fg.c.Close()
	bg, err := dial(addr)
	if err != nil {
		return err
	}
	defer bg.c.Close()

	// Warm the working set through the promotion coin.
	for range c.warmup {
		for i := 0; i < c.wkeys; {
			k := min(c.pipe, c.wkeys-i)
			for j := range k {
				fg.cmd([]byte("GET"), wkey(i+j))
			}
			if err := fg.flush(); err != nil {
				return err
			}
			if err := fg.drain(k); err != nil {
				return err
			}
			i += k
		}
	}

	rng := rand.New(rand.NewSource(47))
	baseP50, baseP99, err := probeWindow(fg, c, rng, nil)
	if err != nil {
		return err
	}

	var done atomic.Bool
	repl := make(chan error, 1)
	var seen int
	var secs float64
	go func() {
		t0 := time.Now()
		var err error
		seen, _, err = replay(bg, c, nil)
		secs = time.Since(t0).Seconds()
		done.Store(true)
		repl <- err
	}()
	durP50, durP99 := 0.0, 0.0
	for !done.Load() {
		p50, p99, err := probeWindow(fg, c, rng, &done)
		if err != nil {
			return err
		}
		// Keep the worst window: a replay that only hurts briefly still
		// hurt.
		if p99 > durP99 {
			durP50, durP99 = p50, p99
		}
	}
	if err := <-repl; err != nil {
		return err
	}
	if seen != c.n {
		return fmt.Errorf("replay saw %d entries, want %d", seen, c.n)
	}
	afterP50, afterP99, err := probeWindow(fg, c, rng, nil)
	if err != nil {
		return err
	}

	fmt.Printf("pollute,%d,%d,%d,%.1f,%.0f,%.1f,%d,%.0f/%.0f,%.0f/%.0f;%.0f/%.0f\n",
		c.n, c.elen, c.count, secs, float64(seen)/secs, durP99/baseP99,
		maxRSSMiB(), baseP50, baseP99, durP50, durP99, afterP50, afterP99)
	return nil
}

// maxRSSMiB is the process peak resident set, the pollution number a
// fresh replay process makes meaningful.
func maxRSSMiB() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	if runtime.GOOS == "darwin" {
		return ru.Maxrss >> 20
	}
	return ru.Maxrss >> 10
}
