// queue re-gates the doc 08 section 6 claim on the landed fold plane and
// scores PRED-OBS1-O2B-QUEUE: queue-shape workloads with hot ends take
// zero bucket GETs at steady state. The queueend lab pinned the local
// cold-region contract before list fold emission existed and disclosed
// this re-gate; since the list slice, every demoted interior chunk also
// folds a position-run projection to the bucket, so the claim now runs
// with the fold plane live end to end: the queue's interior provably
// reaches the published ledger while the working ends never pay a GET.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/aki/engine/obs1/shard"
	"github.com/tamnd/aki/engine/obs1/sim"
	"github.com/tamnd/aki/obs1srv/conformance"
	"github.com/tamnd/aki/obs1srv/drivers"
)

const queueKey = "queue:jobs"

type cfg struct {
	backlog int // elements pushed before the steady phase
	rounds  int // steady rounds, each k pushes then k pops
	k       int
}

func qval(seq int) string { return fmt.Sprintf("qv-%09d-%s", seq, strings.Repeat("x", 48)) }

type lab struct {
	bucket  *sim.Sim
	b       *drivers.Booted
	srv     *drivers.Server
	rc      *conformance.Conn
	nc      net.Conn
	pushSeq int
	popSeq  int
}

func boot(dir string) (*lab, error) {
	bucket := sim.New(sim.Config{})
	l := &lab{bucket: bucket}
	srv, err := drivers.Listen(drivers.Options{
		Addr: "127.0.0.1:0", Shards: 4, ArenaBytes: 16 << 20, SegBytes: 4 << 20,
		VlogDir: dir, ColdDir: dir, ResidentCapBytes: 2 << 20,
		Boot: func(rt *shard.Runtime) error {
			b, err := drivers.BootDurability(context.Background(), drivers.BootConfig{
				Store: bucket, Prefix: "p", Node: 0xED, Incarnation: 1,
				FlushAge: 5 * time.Millisecond, FoldAge: 20 * time.Millisecond,
			}, rt)
			if err != nil {
				return err
			}
			l.b = b
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	l.srv = srv
	go func() { _ = srv.Serve() }()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		_ = srv.Close()
		return nil, err
	}
	l.nc = nc
	l.rc = conformance.NewConn(nc)
	return l, nil
}

func (l *lab) close() {
	_ = l.nc.Close()
	_ = l.srv.Close()
	_ = l.b.Close()
}

func (l *lab) do(args ...string) string {
	_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
	v, err := l.rc.Do(args)
	if err != nil {
		die("command %v: %v", args, err)
	}
	return conformance.Render(v)
}

// push appends n values pipelined; the queue grows at the left end.
func (l *lab) push(n int) {
	const batch = 500
	for done := 0; done < n; {
		step := min(batch, n-done)
		var sb strings.Builder
		for i := 0; i < step; i++ {
			v := qval(l.pushSeq + i)
			fmt.Fprintf(&sb, "*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(queueKey), queueKey, len(v), v)
		}
		_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := l.nc.Write([]byte(sb.String())); err != nil {
			die("push write: %v", err)
		}
		for i := 0; i < step; i++ {
			line, err := l.rc.R.ReadString('\n')
			if err != nil {
				die("push read: %v", err)
			}
			if !strings.HasPrefix(line, ":") {
				die("push reply %q", line)
			}
		}
		l.pushSeq += step
		done += step
	}
}

// pop drains n values from the right end, verifying strict FIFO order.
func (l *lab) pop(n int) {
	for i := 0; i < n; i++ {
		got := l.do("RPOP", queueKey)
		want := qval(l.popSeq)
		if got != want {
			die("pop %d: got %q want %q", l.popSeq, got, want)
		}
		l.popSeq++
	}
}

// ballastRaw drives string ballast through the resident cap so demotion
// and fold pressure are real, the ledger lab's loop.
func (l *lab) ballastRaw(round int) {
	const keys, batch = 4000, 500
	val := strings.Repeat("b", 48)
	_ = l.nc.SetDeadline(time.Now().Add(30 * time.Second))
	for base := 0; base < keys; base += batch {
		var sb strings.Builder
		for i := base; i < base+batch; i++ {
			key := "ballast:" + strconv.Itoa(round) + ":" + strconv.Itoa(i)
			fmt.Fprintf(&sb, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		}
		if _, err := l.nc.Write([]byte(sb.String())); err != nil {
			die("ballast write: %v", err)
		}
		for i := 0; i < batch; i++ {
			line, err := l.rc.R.ReadString('\n')
			if err != nil {
				die("ballast read: %v", err)
			}
			if !strings.HasPrefix(line, "+OK") {
				die("ballast reply %q", line)
			}
		}
	}
}

func (l *lab) settle() {
	deadline := time.Now().Add(30 * time.Second)
	for {
		l.b.Folder.Flush()
		fs := l.b.Folder.Stats()
		if fs.SegmentsPut == fs.SegmentsCut && fs.Published == fs.SegmentsPut {
			return
		}
		if time.Now().After(deadline) {
			die("fold never settled: %+v", fs)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// queuePlaces counts the queue key's chunk placements across the
// published ledger, the proof the interior reached the bucket.
func (l *lab) queuePlaces() int {
	n := 0
	for _, led := range l.b.Folder.Ledger() {
		for _, p := range led.Places {
			if string(p.Key) == queueKey && !p.Tombstone {
				n++
			}
		}
	}
	return n
}

// coldStat reads one summed counter out of the INFO reply.
func (l *lab) coldStat(name string) uint64 {
	text := l.do("INFO")
	for _, line := range strings.Split(text, "\r\n") {
		if v, ok := strings.CutPrefix(line, name+":"); ok {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				die("INFO %s parse %q: %v", name, v, err)
			}
			return n
		}
	}
	die("INFO has no %s row", name)
	return 0
}

func die(format string, args ...any) {
	panic(fmt.Errorf("queue: "+format, args...))
}

type phase struct {
	name string
	ops  int
	gets int64
}

type results struct {
	coldBytes    uint64
	folds        uint64
	queuePlaces  int
	primedRounds int
	phases       []phase
	fetches      uint64
	errs         uint64
}

func run(c cfg) (res results, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
				return
			}
			panic(r)
		}
	}()
	dir, derr := os.MkdirTemp("", "obs1-queue-*")
	if derr != nil {
		return res, derr
	}
	defer func() { _ = os.RemoveAll(dir) }()
	l, berr := boot(dir)
	if berr != nil {
		return res, berr
	}
	defer l.close()

	measure := func(name string, ops int, f func()) {
		u0 := l.bucket.Usage()
		f()
		u1 := l.bucket.Usage()
		res.phases = append(res.phases, phase{name: name, ops: ops, gets: u1.GetRequests - u0.GetRequests})
	}

	// The backlog: the interior demotes under the 2 MiB cap during the
	// build, and since the list slice every demoted chunk also emits its
	// position-run projection through the fold tap.
	measure("backlog_build", c.backlog, func() { l.push(c.backlog) })
	res.coldBytes = l.coldStat("cold_region_bytes")

	// Priming until the queue key is provably in the published ledger:
	// ballast keeps the fold pipeline cutting until the queue's own chunk
	// placements publish, the re-gate's new proof obligation.
	deadline := time.Now().Add(600 * time.Second)
	for l.queuePlaces() == 0 {
		if time.Now().After(deadline) {
			die("queue key never reached the fold ledger")
		}
		l.ballastRaw(10_000 + res.primedRounds)
		res.primedRounds++
		l.settle()
	}
	res.queuePlaces = l.queuePlaces()

	// The steady phase, the claim under test: bounded-lag push and pop at
	// the working ends over the cold backlog, with ballast rounds keeping
	// demotion and fold pressure live throughout.
	measure("steady", 2*c.rounds*c.k, func() {
		for r := 0; r < c.rounds; r++ {
			l.push(c.k)
			l.pop(c.k)
			if r%4 == 3 {
				l.ballastRaw(r)
				l.settle()
			}
		}
	})
	res.folds = l.b.Folder.Stats().SegmentsPut

	// The drain: pop the whole backlog in FIFO order, walking through
	// every demoted interior chunk from the cold end.
	measure("drain", l.pushSeq-l.popSeq, func() {
		l.pop(l.pushSeq - l.popSeq)
		if got := l.do("EXISTS", queueKey); got != "0" {
			die("queue not empty after drain: EXISTS %s", got)
		}
	})

	cs := l.b.Cold.Stats()
	res.fetches = cs.Fetches
	res.errs = cs.Errs + cs.Unresolved
	return res, nil
}

func main() {
	quick := flag.Bool("quick", false, "small backlog for a fast pass")
	flag.Parse()
	c := cfg{backlog: 200_000, rounds: 20, k: 100}
	if *quick {
		c = cfg{backlog: 60_000, rounds: 4, k: 50}
	}
	res, err := run(c)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("phase,ops,bucket_gets")
	for _, p := range res.phases {
		fmt.Printf("%s,%d,%d\n", p.name, p.ops, p.gets)
	}
	fmt.Printf("backlog_cold_bytes,%d,\n", res.coldBytes)
	fmt.Printf("queue_ledger_places,%d,\n", res.queuePlaces)
	fmt.Printf("primed_rounds,%d,\n", res.primedRounds)
	fmt.Printf("segments_folded,%d,\n", res.folds)
	fmt.Printf("cold_fetches,%d,\n", res.fetches)
	fmt.Printf("cold_errs_unresolved,%d,\n", res.errs)
}
