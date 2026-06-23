package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tamnd/aki/bench"
	"github.com/tamnd/aki/respclient"
)

// TestBenchSetWorkload runs a small SET load against an in-process server and
// checks the result has the expected shape.
func TestBenchSetWorkload(t *testing.T) {
	addr := startServer(t)
	cfg := benchConfig{
		addr:     addr,
		clients:  4,
		requests: 2000,
		pipeline: 1,
		keyspace: 500,
		dataSize: 16,
		workload: "set",
		ratio:    "9:1",
		access:   "uniform",
		warmup:   100,
		coCorr:   true,
		format:   "text",
	}
	run, err := runBench(cfg)
	if err != nil {
		t.Fatalf("runBench: %v", err)
	}
	if run.requests < 1000 {
		t.Fatalf("requests recorded = %d want >= 1000", run.requests)
	}
	if run.throughput() <= 0 {
		t.Fatalf("throughput = %.2f want > 0", run.throughput())
	}
	if run.latencyOf().P50 <= 0 {
		t.Fatalf("p50 = %.2f want > 0", run.latencyOf().P50)
	}
}

// TestBenchGetWorkload checks the GET path against pre-seeded keys.
func TestBenchGetWorkload(t *testing.T) {
	addr := startServer(t)
	cl, err := respclient.Dial(addr, 0)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	for i := 0; i < 100; i++ {
		mustCall(t, cl, "SET", "key:"+itoa(i), "v")
	}
	cl.Close()

	cfg := benchConfig{
		addr:     addr,
		clients:  4,
		requests: 1000,
		pipeline: 2,
		keyspace: 100,
		dataSize: 8,
		workload: "get",
		access:   "uniform",
		warmup:   50,
		format:   "json",
	}
	run, err := runBench(cfg)
	if err != nil {
		t.Fatalf("runBench: %v", err)
	}
	if run.requests <= 0 {
		t.Fatalf("requests = %d want > 0", run.requests)
	}
}

// TestBenchWorkloadCatalog runs each extended workload briefly to confirm the
// command it issues is accepted by the server.
func TestBenchWorkloadCatalog(t *testing.T) {
	addr := startServer(t)
	for _, name := range []string{"mixed", "cache", "queue", "leaderboard", "session", "ratelimit", "stream"} {
		cfg := benchConfig{
			addr:     addr,
			clients:  2,
			requests: 400,
			pipeline: 1,
			keyspace: 50,
			dataSize: 8,
			workload: name,
			ratio:    "1:1",
			access:   "uniform",
			warmup:   20,
			coCorr:   true,
			format:   "text",
		}
		run, err := runBench(cfg)
		if err != nil {
			t.Fatalf("workload %s: %v", name, err)
		}
		if run.requests <= 0 {
			t.Fatalf("workload %s: requests = %d want > 0", name, run.requests)
		}
	}
}

// TestBenchValidate checks bad flag combinations are rejected.
func TestBenchValidate(t *testing.T) {
	cases := []benchConfig{
		{clients: 0, keyspace: 1, pipeline: 1, workload: "set", access: "uniform", format: "text", requests: 1},
		{clients: 1, keyspace: 1, pipeline: 1, workload: "bogus", access: "uniform", format: "text", requests: 1},
		{clients: 1, keyspace: 1, pipeline: 1, workload: "set", access: "bogus", format: "text", requests: 1},
		{clients: 1, keyspace: 1, pipeline: 1, workload: "set", access: "uniform", format: "bogus", requests: 1},
	}
	for i, c := range cases {
		if err := c.validate(); err == nil {
			t.Fatalf("case %d: expected validation error", i)
		}
	}
}

// TestBenchJSONOut checks the JSON report file is written and parses against the
// schema in spec 22 section 6.7.
func TestBenchJSONOut(t *testing.T) {
	addr := startServer(t)
	out := filepath.Join(t.TempDir(), "result.json")
	cfg := benchConfig{
		addr:     addr,
		clients:  2,
		requests: 500,
		pipeline: 1,
		keyspace: 100,
		dataSize: 8,
		workload: "set",
		access:   "uniform",
		warmup:   10,
		jsonOut:  out,
		format:   "text",
	}
	run, err := runBench(cfg)
	if err != nil {
		t.Fatalf("runBench: %v", err)
	}
	if err := writeJSONReport(out, run); err != nil {
		t.Fatalf("writeJSONReport: %v", err)
	}
	blob, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	var parsed jsonReport
	if err := json.Unmarshal(blob, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.Config.Workload != "set" {
		t.Fatalf("workload = %q want set", parsed.Config.Workload)
	}
	if parsed.Results.TotalRequests <= 0 {
		t.Fatalf("total_requests = %d want > 0", parsed.Results.TotalRequests)
	}
}

// TestBenchHDRFile checks the HdrHistogram percentile file is written.
func TestBenchHDRFile(t *testing.T) {
	addr := startServer(t)
	out := filepath.Join(t.TempDir(), "out.hdr")
	cfg := benchConfig{
		addr:     addr,
		clients:  2,
		requests: 400,
		pipeline: 1,
		keyspace: 50,
		dataSize: 8,
		workload: "set",
		access:   "uniform",
		warmup:   10,
		format:   "text",
	}
	run, err := runBench(cfg)
	if err != nil {
		t.Fatalf("runBench: %v", err)
	}
	if err := writeHDRFile(out, run.hist); err != nil {
		t.Fatalf("writeHDRFile: %v", err)
	}
	blob, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read hdr: %v", err)
	}
	if !strings.Contains(string(blob), "Percentile") {
		t.Fatalf("hdr file missing header: %q", blob)
	}
}

// TestBenchConnectError checks a dead address reports a clear error.
func TestBenchConnectError(t *testing.T) {
	cfg := benchConfig{
		addr:     "127.0.0.1:1",
		clients:  1,
		requests: 10,
		pipeline: 1,
		keyspace: 10,
		workload: "set",
		access:   "uniform",
		format:   "text",
	}
	if _, err := runBench(cfg); err == nil {
		t.Fatal("expected connect error")
	}
}

// TestReadOneReply checks the RESP reply skimmer consumes each reply type.
func TestReadOneReply(t *testing.T) {
	stream := "+OK\r\n" +
		":42\r\n" +
		"$5\r\nhello\r\n" +
		"$-1\r\n" +
		"*2\r\n$1\r\na\r\n$1\r\nb\r\n" +
		"%1\r\n$1\r\nk\r\n$1\r\nv\r\n"
	r := bufio.NewReader(strings.NewReader(stream))
	for i := 0; i < 6; i++ {
		if err := readOneReply(r); err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
	}
}

// TestParseRatio checks ratio parsing and its fallback.
func TestParseRatio(t *testing.T) {
	if r, w := parseRatio("8:2"); r != 8 || w != 2 {
		t.Fatalf("8:2 -> %d:%d", r, w)
	}
	if r, w := parseRatio("garbage"); r != 9 || w != 1 {
		t.Fatalf("garbage -> %d:%d want 9:1", r, w)
	}
	if r, w := parseRatio("0:0"); r != 9 || w != 1 {
		t.Fatalf("0:0 -> %d:%d want 9:1", r, w)
	}
}

// TestPrintRunFormats checks each output format renders without panicking and
// names the workload.
func TestPrintRunFormats(t *testing.T) {
	h := histWithSamples()
	run := benchRun{cfg: benchConfig{workload: "set", coCorr: true}, hist: h, requests: h.TotalCount()}
	for _, f := range []string{"text", "json", "csv"} {
		var buf bytes.Buffer
		printRun(&buf, f, run)
		if !strings.Contains(strings.ToLower(buf.String()), "set") {
			t.Fatalf("format %q output missing workload: %q", f, buf.String())
		}
	}
}

// histWithSamples returns a histogram with a few recorded values for printing.
func histWithSamples() *bench.Histogram {
	h := bench.NewHistogram(1, int64(60)*1e9, 3)
	for v := int64(1); v <= 100; v++ {
		h.RecordValue(v * 1000)
	}
	return h
}

// itoa is a tiny helper so the test does not import strconv just for seeding.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// BenchmarkE2EGet measures end-to-end GET throughput at concurrency=50 against
// an in-process server with all keys pre-warmed in the hot cache. The hot-GET
// bypass means these reads bypass e.mu.RLock entirely once the cache is warm.
func BenchmarkE2EGet(b *testing.B) {
	addr := startServer(b)
	warmCl, err := respclient.Dial(addr, 0)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	for i := 0; i < 1000; i++ {
		if _, wErr := warmCl.CallStr("SET", "k:"+itoa(i), "val"); wErr != nil {
			b.Fatalf("warmup set: %v", wErr)
		}
		if _, wErr := warmCl.CallStr("GET", "k:"+itoa(i)); wErr != nil {
			b.Fatalf("warmup get: %v", wErr)
		}
	}
	warmCl.Close()

	cfg := benchConfig{
		addr:     addr,
		clients:  50,
		requests: b.N,
		pipeline: 1,
		keyspace: 1000,
		dataSize: 8,
		workload: "get",
		access:   "uniform",
		warmup:   0,
		format:   "text",
	}
	run, err := runBench(cfg)
	if err != nil {
		b.Fatalf("runBench: %v", err)
	}
	b.ReportMetric(run.throughput(), "ops/s")
}

// BenchmarkE2ESet measures end-to-end SET throughput at concurrency=50 to give
// a comparison baseline alongside BenchmarkE2EGet.
func BenchmarkE2ESet(b *testing.B) {
	addr := startServer(b)
	cfg := benchConfig{
		addr:     addr,
		clients:  50,
		requests: b.N,
		pipeline: 1,
		keyspace: 1000,
		dataSize: 8,
		workload: "set",
		access:   "uniform",
		warmup:   0,
		format:   "text",
	}
	run, err := runBench(cfg)
	if err != nil {
		b.Fatalf("runBench: %v", err)
	}
	b.ReportMetric(run.throughput(), "ops/s")
}

// BenchmarkE2ESetPipelined measures pipelined SET throughput at concurrency=50
// and pipeline depth=16. Pipelining amortizes the TCP round trip cost over 16
// commands, which should show how much network overhead limits non-pipelined SET.
func BenchmarkE2ESetPipelined(b *testing.B) {
	addr := startServer(b)
	cfg := benchConfig{
		addr:     addr,
		clients:  50,
		requests: b.N,
		pipeline: 16,
		keyspace: 1000,
		dataSize: 8,
		workload: "set",
		access:   "uniform",
		warmup:   0,
		format:   "text",
	}
	run, err := runBench(cfg)
	if err != nil {
		b.Fatalf("runBench: %v", err)
	}
	b.ReportMetric(run.throughput(), "ops/s")
}
