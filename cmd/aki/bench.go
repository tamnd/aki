package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/aki/bench"
)

// benchConfig holds the parsed flags for a bench run.
type benchConfig struct {
	addr       string
	socket     string
	tls        bool
	auth       string
	clients    int
	requests   int
	pipeline   int
	keyspace   int
	dataSize   int
	workload   string
	ratio      string
	access     string
	zipfS      float64
	warmup     int
	duration   time.Duration
	coCorr     bool
	hdrFile    string
	jsonOut    string
	format     string
	shardCount int
	quiet      bool
}

// benchRun is the raw outcome of a load test: the merged histogram plus the
// run-level facts the reporters need. Percentiles are derived from the
// histogram on demand so every output format reports the same numbers.
type benchRun struct {
	cfg      benchConfig
	hist     *bench.Histogram
	requests int64
	duration time.Duration
}

// cmdBench dispatches the bench subcommands. Only run is built so far.
func cmdBench(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: aki bench run [flags]")
	}
	switch args[0] {
	case "run":
		return cmdBenchRun(args[1:])
	default:
		return fmt.Errorf("unknown bench subcommand %q (try: aki bench run)", args[0])
	}
}

// cmdBenchRun parses flags, runs the load test, and prints the result.
func cmdBenchRun(args []string) error {
	fs := flag.NewFlagSet("bench run", flag.ContinueOnError)
	cfg := benchConfig{}
	fs.StringVar(&cfg.addr, "addr", "127.0.0.1:6379", "server address")
	fs.StringVar(&cfg.socket, "socket", "", "unix socket path (overrides --addr)")
	fs.BoolVar(&cfg.tls, "tls", false, "dial the server over TLS")
	fs.StringVar(&cfg.auth, "auth", "", "password to authenticate with")
	fs.IntVar(&cfg.clients, "clients", 50, "number of parallel clients")
	fs.IntVar(&cfg.requests, "requests", 1000000, "total requests per test")
	fs.IntVar(&cfg.pipeline, "pipeline", 1, "pipeline depth")
	fs.IntVar(&cfg.keyspace, "keyspace", 1000000, "key cardinality")
	fs.IntVar(&cfg.dataSize, "data-size", 64, "value size in bytes")
	fs.StringVar(&cfg.workload, "workload", "set", "workload: set,get,mixed,cache,queue,leaderboard,session,ratelimit,stream")
	fs.StringVar(&cfg.ratio, "ratio", "9:1", "read:write ratio for mixed-style workloads")
	fs.StringVar(&cfg.access, "access", "uniform", "key access pattern: uniform,zipfian,latest")
	fs.Float64Var(&cfg.zipfS, "zipf-s", 1.01, "zipfian s parameter")
	fs.IntVar(&cfg.warmup, "warmup", 10000, "requests to discard at start")
	fs.DurationVar(&cfg.duration, "duration", 0, "run for this long instead of a fixed --requests")
	fs.BoolVar(&cfg.coCorr, "co-correct", true, "enable coordinated-omission correction")
	fs.StringVar(&cfg.hdrFile, "hdr-file", "", "write an HdrHistogram percentile table to this path")
	fs.StringVar(&cfg.jsonOut, "json-out", "", "write the JSON result to this path")
	fs.StringVar(&cfg.format, "format", "text", "output format: text,json,csv")
	fs.IntVar(&cfg.shardCount, "shard-count", 0, "server shard count, recorded in the report")
	fs.BoolVar(&cfg.quiet, "quiet", false, "suppress progress output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	run, err := runBench(cfg)
	if err != nil {
		return err
	}

	if cfg.hdrFile != "" {
		if err := writeHDRFile(cfg.hdrFile, run.hist); err != nil {
			return err
		}
	}
	if cfg.jsonOut != "" {
		if err := writeJSONReport(cfg.jsonOut, run); err != nil {
			return err
		}
	}
	printRun(os.Stdout, cfg.format, run)
	return nil
}

// validate checks the flag combination is runnable.
func (c *benchConfig) validate() error {
	if c.clients < 1 {
		return fmt.Errorf("--clients must be at least 1")
	}
	if c.keyspace < 1 {
		return fmt.Errorf("--keyspace must be at least 1")
	}
	if c.pipeline < 1 {
		return fmt.Errorf("--pipeline must be at least 1")
	}
	if _, ok := workloads[c.workload]; !ok {
		return fmt.Errorf("unknown workload %q", c.workload)
	}
	switch c.access {
	case "uniform", "zipfian", "latest":
	default:
		return fmt.Errorf("unknown access pattern %q", c.access)
	}
	switch c.format {
	case "text", "json", "csv":
	default:
		return fmt.Errorf("unknown format %q", c.format)
	}
	if c.duration == 0 && c.requests < 1 {
		return fmt.Errorf("--requests must be at least 1")
	}
	return nil
}

// dialBenchConn opens one client connection and authenticates if needed.
func dialBenchConn(cfg benchConfig) (net.Conn, *bufio.Reader, error) {
	network, address := "tcp", cfg.addr
	if cfg.socket != "" {
		network, address = "unix", cfg.socket
	}
	var (
		conn net.Conn
		err  error
	)
	if cfg.tls {
		d := &net.Dialer{Timeout: 5 * time.Second}
		conn, err = tls.DialWithDialer(d, network, address, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // benchmark tool dials operator-controlled test servers
	} else {
		conn, err = net.DialTimeout(network, address, 5*time.Second)
	}
	if err != nil {
		return nil, nil, err
	}
	r := bufio.NewReaderSize(conn, 64*1024)
	if cfg.auth != "" {
		if _, werr := conn.Write(appendArray(nil, [][]byte{[]byte("AUTH"), []byte(cfg.auth)})); werr != nil {
			_ = conn.Close()
			return nil, nil, werr
		}
		if rerr := readOneReply(r); rerr != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("auth: %w", rerr)
		}
	}
	return conn, r, nil
}

// runBench runs the configured load test. It spawns the clients, splits the
// request budget across them, records per-request latency into per-client
// histograms, and merges them into one result.
func runBench(cfg benchConfig) (benchRun, error) {
	// Probe one connection up front so a bad address fails fast with a clear error.
	probe, _, err := dialBenchConn(cfg)
	if err != nil {
		return benchRun{}, fmt.Errorf("connect to server: %w", err)
	}
	_ = probe.Close()

	gen := workloads[cfg.workload]
	ratioReads, ratioWrites := parseRatio(cfg.ratio)

	var (
		remaining   atomic.Int64
		recordGate  atomic.Int64 // counts issued requests for the warmup gate
		wg          sync.WaitGroup
		hists       = make([]*bench.Histogram, cfg.clients)
		errOnce     sync.Once
		firstErr    error
		deadlineSet = cfg.duration > 0
		deadline    time.Time
	)
	if !deadlineSet {
		remaining.Store(int64(cfg.requests))
	}
	start := time.Now()
	if deadlineSet {
		deadline = start.Add(cfg.duration)
	}

	for id := 0; id < cfg.clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			h := bench.NewHistogram(1, int64(60)*1e9, 3)
			hists[id] = h
			conn, r, derr := dialBenchConn(cfg)
			if derr != nil {
				errOnce.Do(func() { firstErr = derr })
				return
			}
			defer func() { _ = conn.Close() }()

			rng := rand.New(rand.NewSource(int64(id)*2862933555777941757 + 1))
			var zipf *rand.Zipf
			if cfg.access == "zipfian" {
				zipf = rand.NewZipf(rng, cfg.zipfS, 1, uint64(cfg.keyspace-1))
			}
			val := make([]byte, cfg.dataSize)
			for i := range val {
				val[i] = byte('a' + rng.Intn(26))
			}

			var sendBuf []byte
			expectedInterval := int64(0) // EMA of per-op latency, ns, for CO correction
			for {
				if deadlineSet {
					if time.Now().After(deadline) {
						return
					}
				} else if remaining.Add(int64(-cfg.pipeline)) < 0 {
					return
				}

				sendBuf = sendBuf[:0]
				for p := 0; p < cfg.pipeline; p++ {
					isRead := pickRead(rng, gen, ratioReads, ratioWrites)
					idx := keyIndex(rng, zipf, cfg)
					sendBuf = gen.build(sendBuf, isRead, idx, val, rng)
				}

				t0 := time.Now()
				if _, werr := conn.Write(sendBuf); werr != nil {
					errOnce.Do(func() { firstErr = werr })
					return
				}
				for p := 0; p < cfg.pipeline; p++ {
					if rerr := readOneReply(r); rerr != nil {
						errOnce.Do(func() { firstErr = rerr })
						return
					}
				}
				perOp := time.Since(t0).Nanoseconds() / int64(cfg.pipeline)

				issued := recordGate.Add(int64(cfg.pipeline))
				if int(issued) <= cfg.warmup {
					expectedInterval = emaInterval(expectedInterval, perOp)
					continue
				}
				for p := 0; p < cfg.pipeline; p++ {
					h.RecordValue(perOp)
				}
				if cfg.coCorr && expectedInterval > 0 && perOp > 2*expectedInterval {
					// The connection stalled relative to its recent norm. Backfill the
					// intervals it missed so the tail reflects the stall (Gil Tene).
					if missed := perOp/expectedInterval - 1; missed > 0 {
						h.RecordValues(perOp, missed)
					}
				}
				expectedInterval = emaInterval(expectedInterval, perOp)
			}
		}(id)
	}

	wg.Wait()
	if firstErr != nil {
		return benchRun{}, firstErr
	}
	elapsed := time.Since(start)

	merged := bench.NewHistogram(1, int64(60)*1e9, 3)
	for _, h := range hists {
		merged.Merge(h)
	}
	return benchRun{cfg: cfg, hist: merged, requests: merged.TotalCount(), duration: elapsed}, nil
}

// throughput returns ops per second for the run.
func (r benchRun) throughput() float64 {
	secs := r.duration.Seconds()
	if secs <= 0 {
		return 0
	}
	return float64(r.requests) / secs
}

// emaInterval folds a new sample into the exponential moving average interval.
func emaInterval(cur, sample int64) int64 {
	if cur == 0 {
		return sample
	}
	return (cur*7 + sample) / 8 // 7/8 old, 1/8 new
}

// pickRead decides whether the next op is a read for the workload.
func pickRead(rng *rand.Rand, gen workload, reads, writes int) bool {
	switch gen.mix {
	case mixRead:
		return true
	case mixWrite:
		return false
	default:
		if reads+writes == 0 {
			return true
		}
		return rng.Intn(reads+writes) < reads
	}
}

// keyIndex picks a key index by the access pattern.
func keyIndex(rng *rand.Rand, zipf *rand.Zipf, cfg benchConfig) int {
	var idx int
	switch cfg.access {
	case "zipfian":
		if zipf != nil {
			idx = int(zipf.Uint64())
		}
	case "latest":
		// Bias toward the newest keys: most picks land near the high end.
		span := int(rng.ExpFloat64() * float64(cfg.keyspace) / 8)
		idx = cfg.keyspace - 1 - span
		if idx < 0 {
			idx = rng.Intn(cfg.keyspace)
		}
	default: // uniform
		idx = rng.Intn(cfg.keyspace)
	}
	if idx < 0 {
		idx = 0
	}
	if idx >= cfg.keyspace {
		idx = cfg.keyspace - 1
	}
	return idx
}

// appendArray appends a RESP array of bulk strings to dst.
func appendArray(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// readOneReply consumes one full RESP reply from r without decoding its value,
// which is all the benchmark needs since it only counts replies.
func readOneReply(r *bufio.Reader) error {
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	if len(line) < 3 {
		return fmt.Errorf("short reply line %q", line)
	}
	body := strings.TrimRight(line[1:], "\r\n")
	switch line[0] {
	case '+', '-', ':', ',', '#', '(', '_':
		return nil
	case '$', '=', '!':
		n, perr := strconv.Atoi(body)
		if perr != nil {
			return fmt.Errorf("bad bulk length %q", body)
		}
		if n < 0 {
			return nil
		}
		_, derr := r.Discard(n + 2)
		return derr
	case '*', '~', '>', '%':
		n, perr := strconv.Atoi(body)
		if perr != nil {
			return fmt.Errorf("bad aggregate length %q", body)
		}
		if n < 0 {
			return nil
		}
		mult := 1
		if line[0] == '%' {
			mult = 2
		}
		for i := 0; i < n*mult; i++ {
			if err := readOneReply(r); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown reply type %q", line[0])
	}
}

// parseRatio reads a "reads:writes" ratio string, defaulting to 9:1 on a bad
// value.
func parseRatio(s string) (int, int) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 9, 1
	}
	reads, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	writes, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || reads < 0 || writes < 0 || reads+writes == 0 {
		return 9, 1
	}
	return reads, writes
}

// --- output ---------------------------------------------------------------

// jsonReport mirrors the JSON schema in spec 22 section 6.7.
type jsonReport struct {
	Test    string            `json:"test"`
	Config  jsonReportConfig  `json:"config"`
	Results jsonReportResults `json:"results"`
}

type jsonReportConfig struct {
	Clients     int    `json:"clients"`
	Requests    int    `json:"requests"`
	Pipeline    int    `json:"pipeline"`
	Keyspace    int    `json:"keyspace"`
	DataSize    int    `json:"data_size"`
	Workload    string `json:"workload"`
	Access      string `json:"access"`
	COCorrected bool   `json:"co_corrected"`
	Addr        string `json:"addr"`
	AkiCommit   string `json:"aki_commit"`
	GoVersion   string `json:"go_version"`
	HW          string `json:"hw"`
	ShardCount  int    `json:"shard_count"`
}

type jsonReportResults struct {
	TotalRequests int64       `json:"total_requests"`
	DurationMS    int64       `json:"duration_ms"`
	OpsPerSec     float64     `json:"ops_per_sec"`
	LatencyUS     latencyView `json:"latency_us"`
}

// latencyView is the percentile set shared by the JSON report and the text
// table, all in microseconds.
type latencyView struct {
	P50    float64 `json:"p50"`
	P75    float64 `json:"p75"`
	P90    float64 `json:"p90"`
	P95    float64 `json:"p95"`
	P99    float64 `json:"p99"`
	P999   float64 `json:"p999"`
	P9999  float64 `json:"p9999"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"stddev"`
}

// latencyOf reads the percentile set out of the run's histogram in microseconds.
func (r benchRun) latencyOf() latencyView {
	us := func(ns int64) float64 { return float64(ns) / 1000 }
	h := r.hist
	return latencyView{
		P50:    us(h.ValueAtPercentile(50)),
		P75:    us(h.ValueAtPercentile(75)),
		P90:    us(h.ValueAtPercentile(90)),
		P95:    us(h.ValueAtPercentile(95)),
		P99:    us(h.ValueAtPercentile(99)),
		P999:   us(h.ValueAtPercentile(99.9)),
		P9999:  us(h.ValueAtPercentile(99.99)),
		Max:    us(h.Max()),
		Mean:   h.Mean() / 1000,
		StdDev: h.StdDev() / 1000,
	}
}

// report builds the serializable report for the run.
func (r benchRun) report() jsonReport {
	return jsonReport{
		Test: strings.ToUpper(r.cfg.workload),
		Config: jsonReportConfig{
			Clients:     r.cfg.clients,
			Requests:    r.cfg.requests,
			Pipeline:    r.cfg.pipeline,
			Keyspace:    r.cfg.keyspace,
			DataSize:    r.cfg.dataSize,
			Workload:    r.cfg.workload,
			Access:      r.cfg.access,
			COCorrected: r.cfg.coCorr,
			Addr:        r.cfg.addr,
			AkiCommit:   Commit,
			GoVersion:   runtime.Version(),
			HW:          fmt.Sprintf("%s/%s %d-core", runtime.GOOS, runtime.GOARCH, runtime.NumCPU()),
			ShardCount:  r.cfg.shardCount,
		},
		Results: jsonReportResults{
			TotalRequests: r.requests,
			DurationMS:    r.duration.Milliseconds(),
			OpsPerSec:     r.throughput(),
			LatencyUS:     r.latencyOf(),
		},
	}
}

// writeHDRFile writes a plain percentile distribution table for external tools.
func writeHDRFile(path string, h *bench.Histogram) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	h.OutputPercentileDistribution(f, 1000) // ns to us
	return f.Close()
}

// writeJSONReport writes the report as indented JSON to path.
func writeJSONReport(path string, run benchRun) error {
	blob, err := json.MarshalIndent(run.report(), "", "  ")
	if err != nil {
		return err
	}
	blob = append(blob, '\n')
	if err := os.WriteFile(path, blob, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// printRun writes the result in the requested format.
func printRun(w io.Writer, format string, run benchRun) {
	switch format {
	case "json":
		blob, _ := json.MarshalIndent(run.report(), "", "  ")
		_, _ = fmt.Fprintf(w, "%s\n", blob)
	case "csv":
		l := run.latencyOf()
		_, _ = fmt.Fprintln(w, "workload,requests,duration_ms,ops_per_sec,p50_us,p90_us,p99_us,p999_us,max_us,mean_us")
		_, _ = fmt.Fprintf(w, "%s,%d,%d,%.0f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f\n",
			run.cfg.workload, run.requests, run.duration.Milliseconds(), run.throughput(),
			l.P50, l.P90, l.P99, l.P999, l.Max, l.Mean)
	default:
		printText(w, run)
	}
}

// printText renders the human-readable report from spec 22 section 6.6.
func printText(w io.Writer, run benchRun) {
	l := run.latencyOf()
	co := "no"
	if run.cfg.coCorr {
		co = "yes"
	}
	_, _ = fmt.Fprintf(w, "Test: %s (%d bytes, %d clients, P=%d, %s)\n",
		strings.ToUpper(run.cfg.workload), run.cfg.dataSize, run.cfg.clients, run.cfg.pipeline, run.cfg.access)
	_, _ = fmt.Fprintf(w, "  Requests:     %d\n", run.requests)
	_, _ = fmt.Fprintf(w, "  Duration:     %.2fs\n", run.duration.Seconds())
	_, _ = fmt.Fprintf(w, "  Throughput:   %.0f ops/s\n", run.throughput())
	_, _ = fmt.Fprintf(w, "  CO-corrected: %s\n", co)
	_, _ = fmt.Fprintf(w, "\n  Latency (us):\n")
	_, _ = fmt.Fprintf(w, "    p50:    %8.1f\n", l.P50)
	_, _ = fmt.Fprintf(w, "    p75:    %8.1f\n", l.P75)
	_, _ = fmt.Fprintf(w, "    p90:    %8.1f\n", l.P90)
	_, _ = fmt.Fprintf(w, "    p95:    %8.1f\n", l.P95)
	_, _ = fmt.Fprintf(w, "    p99:    %8.1f\n", l.P99)
	_, _ = fmt.Fprintf(w, "    p999:   %8.1f\n", l.P999)
	_, _ = fmt.Fprintf(w, "    p9999:  %8.1f\n", l.P9999)
	_, _ = fmt.Fprintf(w, "    max:    %8.1f\n", l.Max)
	_, _ = fmt.Fprintf(w, "    mean:   %8.1f\n", l.Mean)
	_, _ = fmt.Fprintf(w, "    stddev: %8.1f\n", l.StdDev)
}
