// paritysmoke drives the built f3srv and obs1srv binaries with the same
// pipelined hot workload and compares throughput per command family
// (spec 2064/obs1 milestone O1a exit gate, PRED-OBS1-O1A-PARITY). The
// smoke is non-evidential and box-local: it scores the port, not the
// product, and any family outside noise is a port bug to chase, not a
// design cost.
//
// Each rep spawns a fresh server per binary so no state carries over,
// and the binary order alternates per rep so box drift cancels instead
// of favoring whichever ran second. One CSV row per (rep, server,
// family) to stdout, then a summary table with the obs1/f3 ratio.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	conns    = 8
	depth    = 32
	keyspace = 10000
)

// A family is one verb shape; prep seeds whatever the op reads.
type family struct {
	name string
	prep func(i int) []string
	op   func(i int) []string
}

var val = strings.Repeat("v", 64)

var families = []family{
	{name: "ping", op: func(i int) []string { return []string{"PING"} }},
	{name: "set", op: func(i int) []string { return []string{"SET", key("k", i), val} }},
	{name: "get",
		prep: func(i int) []string { return []string{"SET", key("g", i), val} },
		op:   func(i int) []string { return []string{"GET", key("g", i)} }},
	{name: "incr", op: func(i int) []string { return []string{"INCR", key("c", i)} }},
	{name: "rpush", op: func(i int) []string { return []string{"RPUSH", key("l", i), val} }},
	{name: "hset", op: func(i int) []string { return []string{"HSET", key("h", i), field(i), val} }},
	{name: "zadd", op: func(i int) []string { return []string{"ZADD", key("z", i), score(i), member(i)} }},
}

func key(p string, i int) string { return fmt.Sprintf("%s:%d", p, i%keyspace) }
func field(i int) string         { return fmt.Sprintf("f%d", i/keyspace%16) }
func score(i int) string         { return fmt.Sprintf("%d", i%1000) }
func member(i int) string        { return fmt.Sprintf("m%d", i/keyspace%16) }

func main() {
	f3bin := flag.String("f3bin", "bin/f3srv", "built f3srv binary")
	obs1bin := flag.String("obs1bin", "bin/obs1srv", "built obs1srv binary")
	ops := flag.Int("ops", 200000, "operations per family per rep")
	reps := flag.Int("reps", 3, "reps per server, order alternating")
	quick := flag.Bool("quick", false, "one tiny rep per server")
	flag.Parse()

	if *quick {
		*ops, *reps = 2000, 1
	}
	servers := []struct{ label, bin string }{
		{"f3", *f3bin}, {"obs1", *obs1bin},
	}
	perFam := map[string]map[string][]float64{"f3": {}, "obs1": {}}

	fmt.Println("rep,server,family,ops,elapsed_s,ops_per_s")
	for rep := 0; rep < *reps; rep++ {
		order := []int{0, 1}
		if rep%2 == 1 {
			order = []int{1, 0}
		}
		for _, si := range order {
			s := servers[si]
			addr, stop := spawn(s.bin)
			for _, fam := range families {
				if fam.prep != nil {
					drive(addr, *ops, fam.prep)
				}
				el := drive(addr, *ops, fam.op)
				rate := float64(*ops) / el.Seconds()
				fmt.Printf("%d,%s,%s,%d,%.3f,%.0f\n", rep, s.label, fam.name, *ops, el.Seconds(), rate)
				perFam[s.label][fam.name] = append(perFam[s.label][fam.name], rate)
			}
			stop()
		}
	}

	fmt.Println()
	fmt.Println("family        f3_mean    obs1_mean  ratio  f3_med     obs1_med   med_ratio  f3_spread  obs1_spread")
	for _, fam := range families {
		f3m, f3s := meanSpread(perFam["f3"][fam.name])
		o1m, o1s := meanSpread(perFam["obs1"][fam.name])
		f3md, o1md := median(perFam["f3"][fam.name]), median(perFam["obs1"][fam.name])
		fmt.Printf("%-12s %10.0f %10.0f  %5.3f %10.0f %10.0f      %5.3f     %5.1f%%       %5.1f%%\n",
			fam.name, f3m, o1m, o1m/f3m, f3md, o1md, o1md/f3md, f3s*100, o1s*100)
	}
}

// median is the rep statistic the prediction is scored on: a shared dev
// box mixes in slow outlier reps that a mean drags around, and the
// median of alternating-order reps is what the underlying rate looks
// like between them.
func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// meanSpread returns the mean and the (max-min)/mean spread, the noise
// bar the prediction is scored against.
func meanSpread(xs []float64) (float64, float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	min, max, sum := xs[0], xs[0], 0.0
	for _, x := range xs {
		sum += x
		if x < min {
			min = x
		}
		if x > max {
			max = x
		}
	}
	mean := sum / float64(len(xs))
	return mean, (max - min) / mean
}

var servingRE = regexp.MustCompile(`serving on (\S+) with`)

func spawn(bin string) (string, func()) {
	cmd := exec.Command(bin, "-addr", "127.0.0.1:0", "-net", "goroutine", "-shards", "4")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fatal(err)
	}
	if err := cmd.Start(); err != nil {
		fatal(fmt.Errorf("start %s: %w", bin, err))
	}
	sc := bufio.NewScanner(stderr)
	for sc.Scan() {
		if m := servingRE.FindStringSubmatch(sc.Text()); m != nil {
			return m[1], func() {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
			}
		}
	}
	fatal(fmt.Errorf("no serving banner from %s", bin))
	return "", nil
}

// drive splits n ops over the connection pool, each connection writing
// depth-sized pipelined batches and draining depth replies, and returns
// the wall time across all of them.
func drive(addr string, n int, op func(i int) []string) time.Duration {
	var wg sync.WaitGroup
	per := n / conns
	start := time.Now()
	for w := 0; w < conns; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			runConn(addr, w*per, per, op)
		}(w)
	}
	wg.Wait()
	return time.Since(start)
}

func runConn(addr string, base, n int, op func(i int) []string) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		fatal(err)
	}
	defer func() { _ = nc.Close() }()
	r := bufio.NewReaderSize(nc, 1<<16)
	w := bufio.NewWriterSize(nc, 1<<16)
	for done := 0; done < n; {
		batch := depth
		if n-done < batch {
			batch = n - done
		}
		for i := 0; i < batch; i++ {
			writeCmd(w, op(base+done+i))
		}
		if err := w.Flush(); err != nil {
			fatal(err)
		}
		for i := 0; i < batch; i++ {
			if err := skipReply(r); err != nil {
				fatal(err)
			}
		}
		done += batch
	}
}

func writeCmd(w *bufio.Writer, args []string) {
	// Write errors on the buffered writer surface at Flush.
	_, _ = fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		_, _ = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a)
	}
}

// skipReply consumes one RESP2 reply without decoding it; the smoke
// measures throughput, the conformance suite owns reply content.
func skipReply(r *bufio.Reader) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return fmt.Errorf("empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+', ':':
		return nil
	case '-':
		return fmt.Errorf("server error: %s", body)
	case '$':
		var n int
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return err
		}
		if n < 0 {
			return nil
		}
		_, err := r.Discard(n + 2)
		return err
	case '*':
		var n int
		if _, err := fmt.Sscanf(body, "%d", &n); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := skipReply(r); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unknown reply type %q", line)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
